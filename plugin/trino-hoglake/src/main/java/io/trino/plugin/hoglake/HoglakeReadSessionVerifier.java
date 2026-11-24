/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hoglake;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hoglake.HoglakeReadSessionContext.Placement;
import io.trino.plugin.hoglake.HoglakeReadSessionContext.StorageGrant;
import io.trino.plugin.hoglake.HoglakeReadSessionContext.StorageSession;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.ECFieldFp;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.time.Clock;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

import static io.trino.plugin.hoglake.HoglakeReadSessionContext.isValidBucket;
import static io.trino.plugin.hoglake.HoglakeReadSessionContext.isValidKey;
import static io.trino.plugin.hoglake.HoglakeReadSessionContext.isValidRegion;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_HEADER_BYTES;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_SIGNING_KEYS;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_TOKEN_BYTES;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_VERIFICATION_FILE_BYTES;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Offline verifier for the dedicated AWS read session profile.
 * This verifier uses mounted public keys and never alters legacy session verification.
 */
final class HoglakeReadSessionVerifier
{
    static final String DEFAULT_PROFILE = "hogtower.aws-session";
    static final String SESSION_TOKEN = HoglakeSharedSessionVerifier.SESSION_TOKEN;
    static final Set<String> AUDIENCES = ImmutableSet.of("trino-session", "hoglake-session", "hoglite-session");

    private static final String EXPERIMENTAL_PROFILE = "storage-session";
    private static final long DEFAULT_CLOCK_SKEW_SECONDS = 60;
    private static final long DEFAULT_MAX_LIFETIME_SECONDS = 900;
    private static final long RELOAD_NANOS = SECONDS.toNanos(30);
    private static final UUID NIL_UUID = new UUID(0, 0);
    private static final Pattern BASE64URL = Pattern.compile("[A-Za-z0-9_-]+");
    private static final Pattern CATALOG = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}");
    private static final Pattern POSITIVE_INTEGER = Pattern.compile("[1-9][0-9]*");
    private static final Pattern ROUTED_TENANT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,125}_[1-9][0-9]*");
    private static final Set<String> PRIVATE_KEY_FIELDS = ImmutableSet.of("d", "p", "q", "dp", "dq", "qi", "oth", "k");
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final String issuer;
    private final Path jwksFile;
    private final String profileId;
    private final Set<String> remoteTargets;
    private final Set<UUID> hogliteBackends;
    private final Optional<String> storageRegion;
    private final long skewSeconds;
    private final long maxLifetimeSeconds;
    private final Clock clock;
    private final LongSupplier ticker;
    private volatile Map<String, PublicKey> keys;
    private long lastReload;

    HoglakeReadSessionVerifier(
            String issuer,
            Path jwksFile,
            String profileId,
            Set<String> remoteTargets,
            Set<UUID> hogliteBackends,
            Optional<String> storageRegion)
    {
        this(issuer,
                jwksFile,
                profileId,
                remoteTargets,
                hogliteBackends,
                storageRegion,
                DEFAULT_CLOCK_SKEW_SECONDS,
                DEFAULT_MAX_LIFETIME_SECONDS,
                Clock.systemUTC(),
                System::nanoTime);
    }

    HoglakeReadSessionVerifier(
            String issuer,
            Path jwksFile,
            String profileId,
            Set<String> remoteTargets,
            Set<UUID> hogliteBackends,
            Optional<String> storageRegion,
            Clock clock,
            LongSupplier ticker)
    {
        this(issuer,
                jwksFile,
                profileId,
                remoteTargets,
                hogliteBackends,
                storageRegion,
                DEFAULT_CLOCK_SKEW_SECONDS,
                DEFAULT_MAX_LIFETIME_SECONDS,
                clock,
                ticker);
    }

    HoglakeReadSessionVerifier(
            String issuer,
            Path jwksFile,
            String profileId,
            Set<String> remoteTargets,
            Set<UUID> hogliteBackends,
            Optional<String> storageRegion,
            long clockSkewSeconds,
            long maxLifetimeSeconds,
            Clock clock,
            LongSupplier ticker)
    {
        try {
            require(issuer != null && !issuer.isBlank());
            require(jwksFile != null && Files.isRegularFile(jwksFile));
            require(profileId != null && !profileId.isBlank() && !profileId.equals(EXPERIMENTAL_PROFILE));
            require(remoteTargets != null);
            ImmutableSet<String> configuredTargets = ImmutableSet.copyOf(remoteTargets);
            require(configuredTargets.size() == 2);
            for (String target : configuredTargets) {
                require(!target.isBlank());
            }
            require(hogliteBackends != null && !hogliteBackends.isEmpty());
            ImmutableSet<UUID> configuredBackends = ImmutableSet.copyOf(hogliteBackends);
            for (UUID backendId : configuredBackends) {
                require(!backendId.equals(NIL_UUID));
            }
            require(storageRegion != null);
            require(storageRegion.isEmpty() || isValidRegion(storageRegion.orElseThrow()));
            require(clockSkewSeconds >= 0 && clockSkewSeconds <= 600);
            require(maxLifetimeSeconds > 0 && maxLifetimeSeconds <= 900);
            require(clock != null && ticker != null);

            this.issuer = issuer;
            this.jwksFile = jwksFile;
            this.profileId = profileId;
            this.remoteTargets = configuredTargets;
            this.hogliteBackends = configuredBackends;
            this.storageRegion = storageRegion;
            this.skewSeconds = clockSkewSeconds;
            this.maxLifetimeSeconds = maxLifetimeSeconds;
            this.clock = clock;
            this.ticker = ticker;
            this.keys = readKeys(true);
            this.lastReload = ticker.getAsLong();
        }
        catch (Exception _) {
            // Do not expose file paths or parser causes in configuration failures.
            throw new IllegalArgumentException("Invalid dedicated Hoglake read session configuration");
        }
    }

    /**
     * True when the session token names any dedicated read session audience. The token is
     * not verified here: the answer only selects which verifier judges it, and a token
     * that mixes these audiences with others is refused by both.
     */
    static boolean namesDedicatedAudience(ConnectorIdentity identity)
    {
        try {
            String token = identity == null ? null : identity.getExtraCredentials().get(SESSION_TOKEN);
            if (token == null || token.isEmpty() || token.length() > MAX_TOKEN_BYTES) {
                return false;
            }
            String[] segments = token.split("\\.", -1);
            if (segments.length != 3) {
                return false;
            }
            JsonNode audience = object(decode(segments[1], MAX_TOKEN_BYTES)).get("aud");
            if (audience == null) {
                return false;
            }
            if (audience.isTextual()) {
                return AUDIENCES.contains(audience.textValue());
            }
            if (!audience.isArray()) {
                return false;
            }
            for (JsonNode value : audience) {
                if (value.isTextual() && AUDIENCES.contains(value.textValue())) {
                    return true;
                }
            }
            return false;
        }
        catch (IOException | RuntimeException _) {
            return false;
        }
    }

    public HoglakeReadSessionContext verify(ConnectorSession session)
    {
        if (session == null) {
            throw authorityDenied();
        }
        return verify(session.getIdentity());
    }

    public HoglakeReadSessionContext verify(ConnectorIdentity identity)
    {
        try {
            require(identity != null);
            String token = identity.getExtraCredentials().get(SESSION_TOKEN);
            require(token != null && !token.isEmpty() && token.length() <= MAX_TOKEN_BYTES);
            String[] segments = token.split("\\.", -1);
            require(segments.length == 3);
            for (String segment : segments) {
                require(BASE64URL.matcher(segment).matches());
            }

            JsonNode header = object(decode(segments[0], MAX_HEADER_BYTES));
            require(text(header, "alg").equals("ES256"));
            String keyId = text(header, "kid");
            if (header.has("crit")) {
                require(header.get("crit").isArray() && header.get("crit").isEmpty());
            }
            // Reject headers that inject keys or alter compact payload encoding.
            require(!header.has("jku") && !header.has("jwk") && !header.has("x5u") && !header.has("b64"));

            reloadIfDue();
            PublicKey key = keys.get(keyId);
            require(key != null);
            byte[] signature = decode(segments[2], 64);
            require(signature.length == 64);
            Signature verifier = Signature.getInstance("SHA256withECDSAinP1363Format");
            verifier.initVerify(key);
            verifier.update((segments[0] + "." + segments[1]).getBytes(US_ASCII));
            require(verifier.verify(signature));

            // Parse claims only after signature verification succeeds.
            JsonNode claims = object(decode(segments[1], MAX_TOKEN_BYTES));
            require(text(claims, "iss").equals(issuer));
            validateAudiences(claims.get("aud"));
            require(text(claims, "profile").equals(profileId));
            require(positiveSeconds(claims, "version") == 1L);
            require(text(claims, "purpose").equals("user-session"));
            require(text(claims, "ten").equals("scoped"));

            String subject = text(claims, "sub");
            String principal = text(claims, "usr");
            require(subject.equals(principal));
            require(principal.length() <= 200);
            require(!claims.has("dlg") && !claims.has("scope") && !claims.has("svc"));
            if (claims.has("cli")) {
                text(claims, "cli");
            }

            require(identity.getPrincipal().isPresent());
            require(principal.equals(identity.getPrincipal().orElseThrow().getName()));
            String user = identity.getUser();
            require(sameDatabase(principal, user));

            String organization = text(claims, "org");
            String sessionId = text(claims, "sid");
            text(claims, "jti");
            long generation = positiveSeconds(claims, "generation");
            require(generation == 1L);

            String primaryCatalog = text(claims, "primary_catalog");
            require(CATALOG.matcher(primaryCatalog).matches());
            requireSingleRead(claims.get("cat"), primaryCatalog);
            requireSingleRead(claims.get("remote_schemas"), "team");
            long teamId = teamId(claims.get("prd"));

            long issued = positiveSeconds(claims, "iat");
            long notBefore = positiveSeconds(claims, "nbf");
            long expiry = positiveSeconds(claims, "exp");
            long sessionDeadline = positiveSeconds(claims, "session_deadline");
            StorageSession storage = storageSession(claims.get("aws"));
            validateTimes(issued, notBefore, expiry, storage.expiresAt(), sessionDeadline);

            Placement placement = placement(claims.get("rtg"));
            List<StorageGrant> grants = validateGrants(claims.get("grants"), primaryCatalog, placement, storage.region());
            return new HoglakeReadSessionContext(
                    primaryCatalog,
                    token,
                    principal,
                    user,
                    organization,
                    sessionId,
                    generation,
                    sessionDeadline,
                    teamId,
                    placement,
                    storage,
                    grants);
        }
        catch (Exception _) {
            // Refuse without exposing the JWT, claim values, or parser causes.
            throw authorityDenied();
        }
    }

    /**
     * Verify the session and check one storage object against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageTarget(ConnectorSession session, String bucket, String region, String objectKey)
    {
        HoglakeReadSessionContext context = verify(session);
        context.requireStorageTarget(bucket, region, objectKey);
        return context;
    }

    /**
     * Verify the identity and check one storage object against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageTarget(ConnectorIdentity identity, String bucket, String region, String objectKey)
    {
        HoglakeReadSessionContext context = verify(identity);
        context.requireStorageTarget(bucket, region, objectKey);
        return context;
    }

    /**
     * Verify the session and check one named target and object against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageTarget(ConnectorSession session, String target, String bucket, String region, String objectKey)
    {
        HoglakeReadSessionContext context = verify(session);
        context.requireStorageTarget(target, bucket, region, objectKey);
        return context;
    }

    /**
     * Verify the identity and check one named target and object against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageTarget(ConnectorIdentity identity, String target, String bucket, String region, String objectKey)
    {
        HoglakeReadSessionContext context = verify(identity);
        context.requireStorageTarget(target, bucket, region, objectKey);
        return context;
    }

    /**
     * Verify the session and check one S3 URI against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageLocation(ConnectorSession session, String location, String region)
    {
        HoglakeReadSessionContext context = verify(session);
        context.requireStorageLocation(location, region);
        return context;
    }

    /**
     * Verify the identity and check one S3 URI against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageLocation(ConnectorIdentity identity, String location, String region)
    {
        HoglakeReadSessionContext context = verify(identity);
        context.requireStorageLocation(location, region);
        return context;
    }

    /**
     * Verify the session and check one named target and S3 URI against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageLocation(ConnectorSession session, String target, String location, String region)
    {
        HoglakeReadSessionContext context = verify(session);
        context.requireStorageLocation(target, location, region);
        return context;
    }

    /**
     * Verify the identity and check one named target and S3 URI against signed grants.
     */
    public HoglakeReadSessionContext authorizeStorageLocation(ConnectorIdentity identity, String target, String location, String region)
    {
        HoglakeReadSessionContext context = verify(identity);
        context.requireStorageLocation(target, location, region);
        return context;
    }

    private static void validateAudiences(JsonNode audiences)
    {
        require(audiences != null && audiences.isArray() && audiences.size() == AUDIENCES.size());
        ImmutableSet.Builder<String> values = ImmutableSet.builder();
        for (JsonNode value : audiences) {
            require(value.isTextual());
            values.add(value.textValue());
        }
        require(values.build().equals(AUDIENCES));
    }

    private static long teamId(JsonNode predicates)
    {
        require(predicates != null && predicates.isObject() && predicates.size() == 1);
        JsonNode teams = predicates.get("team_id");
        require(teams != null && teams.isObject() && teams.size() == 1);
        Map.Entry<String, JsonNode> entry = teams.properties().iterator().next();
        String key = entry.getKey();
        require(POSITIVE_INTEGER.matcher(key).matches());
        long teamId = Long.parseLong(key);
        require(teamId > 0);
        JsonNode permission = entry.getValue();
        require(permission != null && permission.isTextual() && permission.textValue().equals("read"));
        return teamId;
    }

    private StorageSession storageSession(JsonNode aws)
    {
        require(aws != null && aws.isObject() && aws.size() == 5);
        String accessKeyId = text(aws, "access_key_id");
        String secretAccessKey = text(aws, "secret_access_key");
        String sessionToken = text(aws, "session_token");
        String region = text(aws, "region");
        require(isValidRegion(region));
        require(storageRegion.isEmpty() || region.equals(storageRegion.orElseThrow()));
        long expiresAt = positiveSeconds(aws, "expires_at");
        return new StorageSession(accessKeyId, secretAccessKey, sessionToken, region, expiresAt);
    }

    private void validateTimes(long issued, long notBefore, long expiry, long awsExpiry, long sessionDeadline)
    {
        require(notBefore <= issued && issued < expiry && expiry <= awsExpiry && awsExpiry <= sessionDeadline);
        long lifetime = Math.subtractExact(expiry, issued);
        require(lifetime > 0 && lifetime <= maxLifetimeSeconds);
        long now = clock.instant().getEpochSecond();
        // Enforce zero clock skew on AWS credential expiry.
        require(now < awsExpiry);
        if (now > expiry) {
            require(Math.subtractExact(now, expiry) <= skewSeconds);
        }
        if (issued > now) {
            require(Math.subtractExact(issued, now) <= skewSeconds);
        }
    }

    private Placement placement(JsonNode route)
    {
        require(route != null && route.isObject() && route.size() == 3);
        require(text(route, "b").equals("hoglite"));
        String backend = text(route, "be");
        UUID backendId = canonicalUuid(backend);
        require(!backendId.equals(NIL_UUID) && hogliteBackends.contains(backendId));
        String tenant = text(route, "t");
        require(tenant.length() <= 128 && ROUTED_TENANT.matcher(tenant).matches() && !tenant.contains(".."));
        int separator = tenant.lastIndexOf('_');
        require(separator > 0 && separator < tenant.length() - 1);
        String incarnation = tenant.substring(separator + 1);
        require(POSITIVE_INTEGER.matcher(incarnation).matches());
        require(Long.parseLong(incarnation) > 0);
        return new Placement(backendId, tenant);
    }

    private List<StorageGrant> validateGrants(JsonNode grantsNode, String catalog, Placement placement, String region)
    {
        String backend = placement.backendId().toString();
        Set<String> targets = ImmutableSet.<String>builder()
                .addAll(remoteTargets)
                .add(catalog)
                .add(backend)
                .build();
        require(targets.size() == 4);
        require(grantsNode != null && grantsNode.isArray() && grantsNode.size() >= targets.size());

        String manifestSuffix = "/" + shard(placement.tenant()) + "/t/" + placement.tenant() + "/manifest";
        Set<String> seen = new HashSet<>();
        ImmutableList.Builder<StorageGrant> result = ImmutableList.builder();
        String previousBucket = "";
        String previousKey = "";
        for (JsonNode grant : grantsNode) {
            require(grant.isObject() && grant.size() == 6);
            boolean prefix = grant.has("prefix");
            boolean objectKey = grant.has("object_key");
            require(prefix != objectKey);

            String bucket = text(grant, "bucket");
            String key = text(grant, prefix ? "prefix" : "object_key");
            String target = text(grant, "target");
            String grantCatalog = text(grant, "catalog");
            String grantRegion = text(grant, "region");
            JsonNode actions = grant.get("actions");

            require(isValidBucket(bucket));
            require(isValidKey(key, prefix));
            require(targets.contains(target));
            require(grantCatalog.equals(catalog));
            require(grantRegion.equals(region));
            require(actions != null && actions.isArray() && actions.size() == 1 &&
                    actions.get(0).isTextual() && actions.get(0).textValue().equals("s3:GetObject"));

            if (target.equals(backend)) {
                require(!prefix && key.endsWith(manifestSuffix) && !seen.contains(backend));
            }
            boolean overlaps = previousKey.endsWith("/") && key.startsWith(previousKey);
            int bucketOrder = bucket.compareTo(previousBucket);
            require(bucketOrder >= 0);
            if (bucketOrder == 0) {
                require(key.compareTo(previousKey) > 0 && !overlaps);
            }

            previousBucket = bucket;
            previousKey = key;
            seen.add(target);
            result.add(new StorageGrant(grantCatalog, target, bucket, grantRegion, key, prefix));
        }
        require(seen.equals(targets));
        return result.build();
    }

    private static String shard(String tenant)
    {
        int hash = 0x811C9DC5;
        for (int index = 0; index < tenant.length(); index++) {
            hash ^= tenant.charAt(index);
            hash *= 0x01000193;
        }
        return "%02x".formatted(hash & 0xFF);
    }

    private static void requireSingleRead(JsonNode map, String key)
    {
        require(map != null && map.isObject() && map.size() == 1);
        JsonNode scopes = map.get(key);
        require(scopes != null && scopes.isArray() && scopes.size() == 1 &&
                scopes.get(0).isTextual() && scopes.get(0).textValue().equals("read"));
    }

    private static boolean sameDatabase(String principal, String user)
    {
        if (user == null) {
            return false;
        }
        int principalSeparator = principal.indexOf('.');
        int userSeparator = user.indexOf('.');
        return principalSeparator > 0 && principalSeparator < principal.length() - 1 &&
                userSeparator == principalSeparator && userSeparator < user.length() - 1 &&
                principal.substring(0, principalSeparator).equals(user.substring(0, userSeparator));
    }

    private synchronized void reloadIfDue()
    {
        long now = ticker.getAsLong();
        if (now - lastReload < RELOAD_NANOS) {
            return;
        }
        lastReload = now;
        try {
            // Apply a well-formed empty key set so withdrawal of the last signing key takes effect.
            keys = readKeys(false);
        }
        catch (Exception _) {
            // Keep the previous public keys when a mounted file reload fails.
        }
    }

    private Map<String, PublicKey> readKeys(boolean requireSigningKey)
            throws IOException, GeneralSecurityException
    {
        require(Files.isRegularFile(jwksFile));
        require(Files.size(jwksFile) <= MAX_VERIFICATION_FILE_BYTES);
        byte[] bytes;
        try (var input = Files.newInputStream(jwksFile)) {
            bytes = input.readNBytes(MAX_VERIFICATION_FILE_BYTES + 1);
        }
        require(bytes.length <= MAX_VERIFICATION_FILE_BYTES);
        JsonNode root = object(bytes);
        JsonNode entries = root.get("keys");
        require(entries != null && entries.isArray());
        ImmutableMap.Builder<String, PublicKey> result = ImmutableMap.builder();
        int count = 0;
        for (JsonNode entry : entries) {
            require(entry.isObject());
            for (String field : PRIVATE_KEY_FIELDS) {
                require(!entry.has(field));
            }
            if (entry.has("use")) {
                if (!text(entry, "use").equals("sig")) {
                    continue;
                }
            }
            if (entry.has("alg")) {
                if (!text(entry, "alg").equals("ES256")) {
                    continue;
                }
            }
            if (entry.has("key_ops")) {
                JsonNode operations = entry.get("key_ops");
                require(operations.isArray());
                require(operations.size() == 1 && operations.get(0).isTextual() && operations.get(0).textValue().equals("verify"));
            }
            require(text(entry, "kty").equals("EC") && text(entry, "crv").equals("P-256"));
            require(++count <= MAX_SIGNING_KEYS);
            result.put(text(entry, "kid"), publicKey(entry));
        }
        if (requireSigningKey) {
            require(count > 0);
        }
        return result.buildOrThrow();
    }

    private static PublicKey publicKey(JsonNode entry)
            throws GeneralSecurityException
    {
        byte[] x = decode(text(entry, "x"), 32);
        byte[] y = decode(text(entry, "y"), 32);
        require(x.length == 32 && y.length == 32);
        AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
        parameters.init(new ECGenParameterSpec("secp256r1"));
        ECParameterSpec curve = parameters.getParameterSpec(ECParameterSpec.class);
        BigInteger pointX = new BigInteger(1, x);
        BigInteger pointY = new BigInteger(1, y);
        BigInteger prime = ((ECFieldFp) curve.getCurve().getField()).getP();
        require(pointX.compareTo(prime) < 0 && pointY.compareTo(prime) < 0);
        // Validate the P-256 point before passing it to the JCA verifier.
        require(pointY.modPow(BigInteger.TWO, prime).equals(pointX.modPow(BigInteger.valueOf(3), prime)
                .add(curve.getCurve().getA().multiply(pointX)).add(curve.getCurve().getB()).mod(prime)));
        return KeyFactory.getInstance("EC").generatePublic(new ECPublicKeySpec(new ECPoint(pointX, pointY), curve));
    }

    private static UUID canonicalUuid(String value)
    {
        UUID id = UUID.fromString(value);
        require(id.toString().equals(value));
        return id;
    }

    private static byte[] decode(String segment, int maxBytes)
    {
        require(segment.length() <= (maxBytes * 4L + 2) / 3);
        require(BASE64URL.matcher(segment).matches());
        byte[] bytes = Base64.getUrlDecoder().decode(segment);
        require(bytes.length <= maxBytes);
        require(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(segment));
        return bytes;
    }

    private static JsonNode object(byte[] bytes)
            throws IOException
    {
        JsonNode root = JSON.readTree(bytes);
        require(root != null && root.isObject());
        return root;
    }

    private static String text(JsonNode object, String field)
    {
        JsonNode value = object.get(field);
        require(value != null && value.isTextual() && !value.textValue().isBlank());
        return value.textValue();
    }

    private static long positiveSeconds(JsonNode object, String field)
    {
        JsonNode value = object.get(field);
        require(value != null && value.isIntegralNumber() && value.canConvertToLong());
        long result = value.longValue();
        require(result > 0);
        return result;
    }

    private static void require(boolean condition)
    {
        if (!condition) {
            throw new IllegalArgumentException("Invalid dedicated Hoglake read session authority");
        }
    }

    private static TrinoException authorityDenied()
    {
        return new TrinoException(PERMISSION_DENIED, "Dedicated Hoglake read session authority is invalid or unavailable");
    }
}
