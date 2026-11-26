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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hoglake.HoglakeReadSessionContext.StorageGrant;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.trino.plugin.hoglake.HoglakeReadSessionVerifier.DEFAULT_PROFILE;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.BACKEND_ID;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.ISSUER;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.config;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.jwk;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.sign;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeReadSessionVerifier
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant NOW = Instant.ofEpochSecond(2_000_000_001L);
    private static final UUID BACKEND_UUID = UUID.fromString(BACKEND_ID);
    private static final String KID = "synthetic-read-session-key";
    private static final String PRINCIPAL = "example.alice";
    private static final String USER = "example.mapped_alice";
    private static final String CATALOG = "example-catalog";
    private static final String REGION = "us-east-1";
    private static final String EVENTS_TARGET = "gigahog:posthog";
    private static final String IMPORTS_TARGET = "gigahog:imports";
    private static final Set<String> REMOTE_TARGETS = ImmutableSet.of(EVENTS_TARGET, IMPORTS_TARGET);
    private static final String ACCESS_KEY = "SYNTHETICREADACCESSKEY";
    private static final String SECRET_KEY = "synthetic-read-secret-key";
    private static final String SESSION_TOKEN_VALUE = "synthetic-read-session-token";
    private static final String AUTHORITY_DENIED = "Dedicated Hoglake read session authority is invalid or unavailable";
    private static final String TARGET_DENIED = "Dedicated Hoglake read session does not allow this storage target";

    @TempDir
    Path directory;

    private Path jwksFile;
    private KeyPair key;
    private AtomicLong ticker;
    private HoglakeReadSessionVerifier verifier;

    @BeforeEach
    void setUp()
    {
        jwksFile = directory.resolve("read-session-jwks.json");
        key = keyPair();
        writeJwks(jwksFile, key, KID);
        ticker = new AtomicLong();
        verifier = verifier(Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @TestFactory
    List<DynamicTest> testPublishedReadSessionVectors()
            throws Exception
    {
        JsonNode root;
        try (InputStream stream = TestHoglakeReadSessionVerifier.class.getResourceAsStream("/read-session-vectors.json")) {
            assertThat(stream).isNotNull();
            root = JSON.readTree(stream);
        }

        Path vectorJwks = directory.resolve("vector-jwks.json");
        Files.write(vectorJwks, JSON.writeValueAsBytes(root.get("jwks")));
        JsonNode vectorConfig = root.get("config");
        String issuer = root.get("issuer").textValue();
        String profile = vectorConfig.get("profile").textValue();
        UUID backendId = UUID.fromString(vectorConfig.get("backend_id").textValue());
        ImmutableSet.Builder<String> targetsBuilder = ImmutableSet.builder();
        for (JsonNode target : vectorConfig.get("remote_targets")) {
            targetsBuilder.add(target.textValue());
        }
        Set<String> targets = targetsBuilder.build();

        JsonNode vectors = root.get("vectors");
        assertThat(vectors.size()).isEqualTo(14);
        ImmutableList.Builder<DynamicTest> tests = ImmutableList.builder();
        for (JsonNode vector : vectors) {
            String name = vector.get("name").textValue();
            tests.add(DynamicTest.dynamicTest(name, () -> {
                long verifyAt = vector.get("verify_at").longValue();
                Clock clock = Clock.fixed(Instant.ofEpochSecond(verifyAt), ZoneOffset.UTC);
                String jwt = vector.get("token").textValue();
                String principal = vector.get("claims").get("usr").textValue();
                String expected = vector.get("expect").get("read").textValue();

                HoglakeReadSessionVerifier dedicated = new HoglakeReadSessionVerifier(
                        issuer,
                        vectorJwks,
                        profile,
                        targets,
                        ImmutableSet.of(backendId),
                        Optional.of(REGION),
                        clock,
                        () -> 0L);

                if (expected.equals("accept")) {
                    HoglakeReadSessionContext context = dedicated.verify(session(principal, principal, jwt));
                    assertThat(context.catalog()).isEqualTo("example-catalog");
                    assertThat(context.principal()).isEqualTo(principal);
                    assertThat(context.organization()).isEqualTo("example-org");
                    assertThat(context.sessionId()).isEqualTo("session-a");
                    assertThat(context.generation()).isEqualTo(1L);
                    assertThat(context.sessionDeadline()).isEqualTo(2_000_000_120L);
                    assertThat(context.teamId()).isEqualTo(42L);
                    assertThat(context.placement().backendId()).isEqualTo(backendId);
                    assertThat(context.placement().tenant()).isEqualTo("fixture_3");
                    assertThat(context.storage().accessKeyId()).isEqualTo("fictional-key");
                    assertThat(context.storage().region()).isEqualTo(REGION);
                    assertThat(context.grants()).hasSize(4);
                    assertThat(context.allowsStorageTarget("example-owner", REGION, "root/2c/t/fixture_3/manifest")).isTrue();
                    assertThat(context.allowsStorageTarget("example-owner", REGION, "root/2c/t/fixture_3/manifest.json")).isFalse();
                    assertThat(context.allowsStorageTarget("remote:events", "example-remote", REGION, "events/team_id=42/part.parquet")).isTrue();
                    assertThat(context.allowsStorageTarget("remote:imports", "example-remote", REGION, "imports/team_id=42/part.parquet")).isTrue();
                }
                else {
                    assertAuthorityDenied(
                            () -> dedicated.verify(session(principal, principal, jwt)),
                            jwt,
                            "fictional-key",
                            "fictional-secret",
                            "fictional-session");
                }

                // Ordinary and storage-session verifiers reject every dedicated vector.
                HoglakeSharedConfig sharedDiscovery = new HoglakeSharedConfig()
                        .setMultiTenant(true)
                        .setUri(URI.create("https://metadata.example.test"))
                        .setJwksFile(vectorJwks.toFile())
                        .setIssuer(issuer)
                        .setBackendBindings("{\"%s\":\"hoglite\"}".formatted(backendId));
                HoglakeSharedSessionVerifier ordinaryVerifier = new HoglakeSharedSessionVerifier(sharedDiscovery, clock, () -> 0L);
                assertThatThrownBy(() -> ordinaryVerifier.verify(session(principal, principal, jwt)))
                        .isInstanceOfSatisfying(TrinoException.class, failure ->
                                assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()));

                HoglakeSharedConfig sharedRemoteRead = new HoglakeSharedConfig()
                        .setMultiTenant(true)
                        .setRemoteRead(true)
                        .setUri(URI.create("https://metadata.example.test"))
                        .setJwksFile(vectorJwks.toFile())
                        .setIssuer(issuer)
                        .setBackendBindings("{\"%s\":\"hoglite\"}".formatted(backendId));
                HoglakeSharedSessionVerifier experimentalVerifier = new HoglakeSharedSessionVerifier(
                        sharedRemoteRead,
                        Optional.of(REGION),
                        clock,
                        () -> 0L);
                assertThatThrownBy(() -> experimentalVerifier.verify(session(principal, principal, jwt)))
                        .isInstanceOfSatisfying(TrinoException.class, failure ->
                                assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()));
            }));
        }
        return tests.build();
    }

    @Test
    void testHogtowerProfileShapeAndRedaction()
    {
        String jwt = token(key, KID, validClaims());
        HoglakeReadSessionContext context = verifier.verify(session(USER, PRINCIPAL, jwt));

        assertThat(context.catalog()).isEqualTo(CATALOG);
        assertThat(context.token()).isEqualTo(jwt);
        assertThat(context.principal()).isEqualTo(PRINCIPAL);
        assertThat(context.user()).isEqualTo(USER);
        assertThat(context.organization()).isEqualTo("example-org");
        assertThat(context.sessionId()).isEqualTo("session-a");
        assertThat(context.generation()).isEqualTo(1L);
        assertThat(context.sessionDeadline()).isEqualTo(NOW.getEpochSecond() + 119);
        assertThat(context.teamId()).isEqualTo(4L);
        assertThat(context.placement().backendId()).isEqualTo(BACKEND_UUID);
        assertThat(context.placement().tenant()).isEqualTo("fixture_3");
        assertThat(context.storage().accessKeyId()).isEqualTo(ACCESS_KEY);
        assertThat(context.storage().secretAccessKey()).isEqualTo(SECRET_KEY);
        assertThat(context.storage().sessionToken()).isEqualTo(SESSION_TOKEN_VALUE);
        assertThat(context.storage().region()).isEqualTo(REGION);
        assertThat(context.grants()).hasSize(4);

        assertThat(context.toString()).isEqualTo("HoglakeReadSessionContext{redacted}");
        assertThat(context.placement().toString()).isEqualTo("Placement{redacted}");
        assertThat(context.storage().toString()).isEqualTo("StorageSession{redacted}");
        for (StorageGrant grant : context.grants()) {
            assertThat(grant.toString()).isEqualTo("StorageGrant{redacted}");
        }
        assertThatThrownBy(() -> context.grants().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testLegacyProfilesRejectedByDedicatedVerifier()
    {
        Map<String, Object> plainClaims = claims(CATALOG, PRINCIPAL, "fixture_3", NOW);
        String plainJwt = token(key, KID, plainClaims);
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, plainJwt)), plainJwt);

        Map<String, Object> experimentalClaims = claims(CATALOG, PRINCIPAL, "fixture_3", NOW);
        experimentalClaims.put("profile", "storage-session");
        experimentalClaims.put("version", 1);
        experimentalClaims.put("purpose", "user-session");
        experimentalClaims.put("primary_catalog", CATALOG);
        experimentalClaims.put("remote_schemas", Map.of("team", List.of("read")));
        experimentalClaims.put("aws", awsBundle(NOW.getEpochSecond() + 360));
        String experimentalJwt = token(key, KID, experimentalClaims);
        assertAuthorityDenied(
                () -> verifier.verify(session(USER, PRINCIPAL, experimentalJwt)),
                experimentalJwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE);

        // Both existing shared verifiers reject the dedicated token.
        String dedicatedJwt = token(key, KID, validClaims());
        HoglakeSharedConfig discoveryConfig = config(jwksFile, URI.create("https://metadata.example.test"));
        HoglakeSharedSessionVerifier discoveryVerifier = new HoglakeSharedSessionVerifier(discoveryConfig, Clock.fixed(NOW, ZoneOffset.UTC), () -> 0L);
        assertThatThrownBy(() -> discoveryVerifier.verify(session(USER, PRINCIPAL, dedicatedJwt)))
                .isInstanceOfSatisfying(TrinoException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()));

        HoglakeSharedConfig remoteReadConfig = config(jwksFile, URI.create("https://metadata.example.test")).setRemoteRead(true);
        HoglakeSharedSessionVerifier remoteReadVerifier = new HoglakeSharedSessionVerifier(
                remoteReadConfig,
                Optional.of(REGION),
                Clock.fixed(NOW, ZoneOffset.UTC),
                () -> 0L);
        assertThatThrownBy(() -> remoteReadVerifier.verify(session(USER, PRINCIPAL, dedicatedJwt)))
                .isInstanceOfSatisfying(TrinoException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()));
    }

    @Test
    void testCallerIdentityBinding()
    {
        String jwt = token(key, KID, validClaims());
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, jwt)).principal()).isEqualTo(PRINCIPAL);
        assertThat(verifier.verify(session(USER, PRINCIPAL, jwt)).user()).isEqualTo(USER);

        for (String badUser : List.of("other.alice", "alice", ".alice", "example.")) {
            assertAuthorityDenied(() -> verifier.verify(session(badUser, PRINCIPAL, jwt)), jwt, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);
        }
        assertAuthorityDenied(() -> verifier.verify(session(USER, null, jwt)), jwt, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);
        assertAuthorityDenied(() -> verifier.verify(session(USER, "example.bob", jwt)), jwt, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, null)));
        assertAuthorityDenied(() -> verifier.verify((ConnectorSession) null));
        assertAuthorityDenied(() -> verifier.verify((ConnectorIdentity) null));

        denyMutation(claims -> claims.put("sub", "example.other"));
        denyMutation(claims -> claims.remove("usr"));
        denyMutation(claims -> claims.put("usr", " "));
        denyMutation(claims -> claims.put("usr", "x".repeat(201)));

        String maxPrincipal = "example." + "x".repeat(192);
        assertThat(maxPrincipal).hasSize(200);
        Map<String, Object> atLimit = validClaims();
        atLimit.put("sub", maxPrincipal);
        atLimit.put("usr", maxPrincipal);
        assertThat(verifier.verify(session(maxPrincipal, maxPrincipal, token(key, KID, atLimit))).principal()).isEqualTo(maxPrincipal);

        String overLimit = "example." + "\uD83D\uDE00".repeat(97);
        assertThat(overLimit).hasSize(202);
        Map<String, Object> overClaims = validClaims();
        overClaims.put("sub", overLimit);
        overClaims.put("usr", overLimit);
        assertAuthorityDenied(() -> verifier.verify(session(overLimit, overLimit, token(key, KID, overClaims))));
    }

    @Test
    void testExpiredJwtVsExpiredAwsCredentialsWithZeroSkew()
    {
        long base = NOW.getEpochSecond();
        // JWT expiry uses configured skew, and AWS expiry uses zero skew.
        Map<String, Object> staggered = validClaims();
        staggered.put("nbf", base);
        staggered.put("iat", base);
        staggered.put("exp", base + 60);
        aws(staggered).put("expires_at", base + 120);
        staggered.put("session_deadline", base + 180);
        String staggeredJwt = token(key, KID, staggered);

        AtomicLong nowSeconds = new AtomicLong(base + 119);
        HoglakeReadSessionVerifier skewedVerifier = verifier(new AdjustableClock(nowSeconds, ZoneOffset.UTC));
        // At exp + 59s, the verifier accepts the token within JWT skew.
        assertThat(skewedVerifier.verify(session(USER, PRINCIPAL, staggeredJwt)).catalog()).isEqualTo(CATALOG);

        // At aws.expires_at, the verifier rejects the token with zero skew.
        nowSeconds.set(base + 120);
        assertAuthorityDenied(
                () -> skewedVerifier.verify(session(USER, PRINCIPAL, staggeredJwt)),
                staggeredJwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE);

        // Beyond exp plus skew, the verifier rejects the token even when AWS credentials remain valid.
        Map<String, Object> longAws = validClaims();
        longAws.put("nbf", base);
        longAws.put("iat", base);
        longAws.put("exp", base + 60);
        aws(longAws).put("expires_at", base + 300);
        longAws.put("session_deadline", base + 300);
        String longAwsJwt = token(key, KID, longAws);

        nowSeconds.set(base + 120);
        assertThat(skewedVerifier.verify(session(USER, PRINCIPAL, longAwsJwt)).catalog()).isEqualTo(CATALOG);
        nowSeconds.set(base + 121);
        assertAuthorityDenied(() -> skewedVerifier.verify(session(USER, PRINCIPAL, longAwsJwt)), longAwsJwt);

        // At equal exp and aws.expires_at, the verifier enforces zero skew.
        Map<String, Object> equalExpiry = validClaims();
        equalExpiry.put("nbf", base);
        equalExpiry.put("iat", base);
        equalExpiry.put("exp", base + 60);
        aws(equalExpiry).put("expires_at", base + 60);
        equalExpiry.put("session_deadline", base + 120);
        String equalJwt = token(key, KID, equalExpiry);

        nowSeconds.set(base + 59);
        assertThat(skewedVerifier.verify(session(USER, PRINCIPAL, equalJwt)).catalog()).isEqualTo(CATALOG);
        nowSeconds.set(base + 60);
        assertAuthorityDenied(() -> skewedVerifier.verify(session(USER, PRINCIPAL, equalJwt)), equalJwt);

        // The verifier accepts future iat within skew and rejects it beyond skew.
        nowSeconds.set(base - 60);
        assertThat(skewedVerifier.verify(session(USER, PRINCIPAL, equalJwt)).catalog()).isEqualTo(CATALOG);
        nowSeconds.set(base - 61);
        assertAuthorityDenied(() -> skewedVerifier.verify(session(USER, PRINCIPAL, equalJwt)), equalJwt);
    }

    @Test
    void testTimeOrderingAndBounds()
    {
        long iat = NOW.getEpochSecond() - 1;
        denyMutation(claims -> claims.remove("nbf"));
        denyMutation(claims -> claims.remove("session_deadline"));
        denyMutation(claims -> claims.put("nbf", iat + 1));
        denyMutation(claims -> claims.put("exp", iat));
        denyMutation(claims -> claims.put("exp", iat + 901));
        denyMutation(claims -> aws(claims).put("expires_at", iat + 58));
        denyMutation(claims -> claims.put("session_deadline", iat + 58));
        denyMutation(claims -> claims.put("nbf", 0));
        denyMutation(claims -> claims.put("iat", 0));
        denyMutation(claims -> claims.put("exp", Long.MAX_VALUE));
        denyMutation(claims -> claims.put("exp", new BigInteger("9223372036854775808")));
        for (String field : List.of("nbf", "iat", "exp", "session_deadline")) {
            denyMutation(claims -> claims.put(field, 1.5));
            denyMutation(claims -> claims.put(field, "2000000000"));
        }
        denyMutation(claims -> aws(claims).put("expires_at", "2000000060"));
        denyMutation(claims -> aws(claims).put("expires_at", 1.5));
    }

    @Test
    void testAudiencesClaimsRoutingAndGrants()
    {
        // Reordered dedicated audiences are valid.
        Map<String, Object> reordered = validClaims();
        reordered.put("aud", List.of("hoglite-session", "trino-session", "hoglake-session"));
        assertThat(verify(reordered).catalog()).isEqualTo(CATALOG);

        // Invalid audiences.
        for (Object badAudience : List.of(
                "trino-session",
                List.of(),
                List.of("trino-session", "hoglake-session"),
                List.of("hoglake", "hoglite"),
                List.of("trino-session", "hoglake-session", "hoglite-session", "hoglake"),
                List.of("trino-session", "trino-session", "hoglite-session"),
                List.of("trino-session", "hoglake-session", 1))) {
            denyMutation(claims -> claims.put("aud", badAudience));
        }

        // Invalid profile, version, purpose, generation, ten, and forbidden claims.
        denyMutation(claims -> claims.remove("profile"));
        denyMutation(claims -> claims.put("profile", "storage-session"));
        denyMutation(claims -> claims.put("profile", "other.aws-session"));
        denyMutation(claims -> claims.put("version", 2));
        denyMutation(claims -> claims.put("version", "1"));
        denyMutation(claims -> claims.put("purpose", "operator"));
        denyMutation(claims -> claims.put("ten", "tenant"));
        denyMutation(claims -> claims.put("generation", 2));
        denyMutation(claims -> claims.remove("sid"));
        denyMutation(claims -> claims.remove("org"));
        denyMutation(claims -> claims.remove("jti"));
        denyMutation(claims -> claims.put("dlg", false));
        denyMutation(claims -> claims.put("scope", "hoglake:" + CATALOG + ":read"));
        denyMutation(claims -> claims.put("svc", "hogtower"));
        denyMutation(claims -> claims.put("cli", ""));
        denyMutation(claims -> claims.put("iss", "https://other.example.test"));

        // Invalid catalog, remote schemas, and predicates.
        denyMutation(claims -> claims.put("primary_catalog", "other-catalog"));
        denyMutation(claims -> claims.put("cat", Map.of(CATALOG, List.of("read", "write"))));
        denyMutation(claims -> claims.put("cat", Map.of(CATALOG, List.of("write"))));
        denyMutation(claims -> claims.put("cat", Map.of(CATALOG, List.of("read"), "other", List.of("read"))));
        denyMutation(claims -> claims.put("remote_schemas", Map.of("team", List.of("write"))));
        denyMutation(claims -> claims.put("remote_schemas", Map.of("team_4", List.of("read"))));
        denyMutation(claims -> claims.put("prd", Map.of("team_id", Map.of("*", "read"))));
        denyMutation(claims -> claims.put("prd", Map.of("team_id", Map.of("4", "write"))));
        denyMutation(claims -> claims.put("prd", Map.of("team_id", Map.of("4", "read", "5", "read"))));
        denyMutation(claims -> claims.put("prd", Map.of("team_id", Map.of("04", "read"))));
        denyMutation(claims -> claims.put("prd", Map.of("team_id", Map.of("0", "read"))));
        denyMutation(claims -> claims.put("prd", Map.of("team_id", Map.of("9223372036854775808", "read"))));

        // Invalid AWS bundle.
        denyMutation(claims -> aws(claims).remove("access_key_id"));
        denyMutation(claims -> aws(claims).remove("secret_access_key"));
        denyMutation(claims -> aws(claims).remove("session_token"));
        denyMutation(claims -> aws(claims).put("region", "eu-central-1"));
        denyMutation(claims -> aws(claims).put("extra", "disallowed"));

        // Invalid routing.
        denyMutation(claims -> claims.put("rtg", Map.of("b", "postgres", "be", BACKEND_ID)));
        denyMutation(claims -> claims.put("rtg", Map.of("b", "hoglite", "be", "00000000-0000-0000-0000-000000000000", "t", "fixture_3")));
        denyMutation(claims -> claims.put("rtg", Map.of("b", "hoglite", "be", "22222222-2222-4222-8222-222222222222", "t", "fixture_3")));
        denyMutation(claims -> claims.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", "fixture_03")));
        denyMutation(claims -> claims.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", "fix..ture_3")));
        denyMutation(claims -> claims.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", "fixture")));

        // The verifier rejects uppercase backend UUIDs even for an allowed backend.
        String hexBackend = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        HoglakeReadSessionVerifier hexVerifier = new HoglakeReadSessionVerifier(
                ISSUER,
                jwksFile,
                DEFAULT_PROFILE,
                REMOTE_TARGETS,
                ImmutableSet.of(UUID.fromString(hexBackend)),
                Optional.of(REGION),
                Clock.fixed(NOW, ZoneOffset.UTC),
                ticker::get);
        Map<String, Object> upperBackendClaims = validClaims();
        upperBackendClaims.put("rtg", Map.of("b", "hoglite", "be", hexBackend.toUpperCase(Locale.ROOT), "t", "fixture_3"));
        grants(upperBackendClaims).get(1).put("target", hexBackend.toUpperCase(Locale.ROOT));
        assertAuthorityDenied(() -> hexVerifier.verify(session(USER, PRINCIPAL, token(key, KID, upperBackendClaims))));

        // Invalid storage grants.
        denyMutation(claims -> grants(claims).get(1).put("object_key", "root/2c/t/fixture_3/manifest.json"));
        denyMutation(claims -> grants(claims).get(1).put("object_key", "root/ff/t/fixture_3/manifest"));
        denyMutation(claims -> grants(claims).get(1).put("object_key", "2c/t/fixture_3/manifest"));
        denyMutation(claims -> {
            grants(claims).get(1).remove("object_key");
            grants(claims).get(1).put("prefix", "root/2c/t/fixture_3/");
        });
        denyMutation(claims -> grants(claims).get(0).put("prefix", "tables/"));
        denyMutation(claims -> grants(claims).get(0).remove("object_key"));
        denyMutation(claims -> grants(claims).get(0).put("object_key", "tables/data.parquet/"));
        denyMutation(claims -> grants(claims).get(2).put("prefix", "events/team_id=4"));
        denyMutation(claims -> grants(claims).get(2).put("prefix", "events/*/"));
        denyMutation(claims -> grants(claims).get(2).put("prefix", "events/../team_id=4/"));
        denyMutation(claims -> grants(claims).get(2).put("prefix", "events//team_id=4/"));
        denyMutation(claims -> grants(claims).get(2).put("bucket", "BAD_BUCKET"));
        denyMutation(claims -> grants(claims).get(2).put("bucket", "bad..bucket"));
        denyMutation(claims -> grants(claims).get(2).put("region", "eu-central-1"));
        denyMutation(claims -> grants(claims).get(2).put("catalog", "other-catalog"));
        denyMutation(claims -> grants(claims).get(2).put("actions", List.of("s3:PutObject")));
        denyMutation(claims -> grants(claims).get(2).put("actions", List.of("s3:GetObject", "s3:ListBucket")));
        denyMutation(claims -> grants(claims).get(3).put("prefix", "events/team_id=4/child/"));
        denyMutation(claims -> grants(claims).get(3).put("prefix", "events/team_id=4/"));
        denyMutation(claims -> grants(claims).remove(3));
        denyMutation(claims -> {
            // Unsorted grants must fail.
            List<Map<String, Object>> list = grants(claims);
            Map<String, Object> second = list.remove(3);
            list.add(2, second);
        });
        denyMutation(claims -> {
            // The verifier rejects duplicate backend manifest targets even with distinct paths.
            Map<String, Object> duplicateBackend = new LinkedHashMap<>(grants(claims).get(1));
            duplicateBackend.put("object_key", "root2/2c/t/fixture_3/manifest");
            grants(claims).add(2, duplicateBackend);
        });

        // The verifier accepts multiple sorted customer grants that do not overlap.
        Map<String, Object> multipleCustomerGrants = validClaims();
        Map<String, Object> extraGrant = new LinkedHashMap<>();
        extraGrant.put("catalog", CATALOG);
        extraGrant.put("target", CATALOG);
        extraGrant.put("bucket", "example-customer");
        extraGrant.put("object_key", "tables/data2.parquet");
        extraGrant.put("region", REGION);
        extraGrant.put("actions", List.of("s3:GetObject"));
        grants(multipleCustomerGrants).add(1, extraGrant);
        assertThat(verify(multipleCustomerGrants).grants()).hasSize(5);
    }

    @Test
    void testStorageTargetAuthorizationAndBoundaries()
    {
        String jwt = token(key, KID, validClaims());
        var caller = session(USER, PRINCIPAL, jwt);
        HoglakeReadSessionContext context = verifier.verify(caller);

        // Literal object grants allow exact keys only.
        assertThat(context.allowsStorageTarget("example-customer", REGION, "tables/data.parquet")).isTrue();
        assertThat(context.allowsStorageTarget(CATALOG, "example-customer", REGION, "tables/data.parquet")).isTrue();
        assertThat(context.allowsStorageLocation("s3://example-customer/tables/data.parquet", REGION)).isTrue();
        assertThat(context.allowsStorageLocation(CATALOG, "s3://example-customer/tables/data.parquet", REGION)).isTrue();
        assertThat(context.allowsStorageTarget(BACKEND_ID, "example-owner", REGION, "root/2c/t/fixture_3/manifest")).isTrue();
        assertThat(context.allowsStorageTarget("example-owner", REGION, "root/2c/t/fixture_3/manifest.json")).isFalse();
        assertThat(context.allowsStorageTarget("example-customer", REGION, "tables/data.parquet.bak")).isFalse();
        assertThat(context.allowsStorageTarget("example-customer", REGION, "tables/data.parquet/extra")).isFalse();

        // Prefix grants allow descendants and enforce strict prefix boundaries.
        assertThat(context.allowsStorageTarget("example-remote", REGION, "events/team_id=4/part-0.parquet")).isTrue();
        assertThat(context.allowsStorageTarget("example-remote", REGION, "events/team_id=4/2026/11/part-1.parquet")).isTrue();
        assertThat(context.allowsStorageTarget("example-remote", REGION, "events/team_id=4/part(1).parquet")).isTrue();
        assertThat(context.allowsStorageTarget(EVENTS_TARGET, "example-remote", REGION, "events/team_id=4/part-0.parquet")).isTrue();
        assertThat(context.allowsStorageTarget(EVENTS_TARGET, "example-remote", REGION, "events/team_id=4/part(1).parquet")).isTrue();
        assertThat(context.allowsStorageTarget(IMPORTS_TARGET, "example-remote", REGION, "imports/team_id=4/part-0.parquet")).isTrue();
        assertThat(context.allowsStorageLocation(EVENTS_TARGET, "s3://example-remote/events/team_id=4/part-0.parquet", REGION)).isTrue();
        assertThat(context.allowsStorageLocation(EVENTS_TARGET, "s3://example-remote/events/team_id=4/part(1).parquet", REGION)).isTrue();

        // Prefix boundary (team_id=4/ vs team_id=42/), sibling prefix, wrong bucket, wrong region, and path traversal.
        List<String> deniedKeys = List.of(
                "events/team_id=42/part-0.parquet",
                "events/team_id=5/part-0.parquet",
                "events/team_id=4",
                "events/team_id=4/",
                "events/team_id=4/../team_id=42/part-0.parquet",
                "events/team_id=4/./part-0.parquet",
                "events/team_id=4//part-0.parquet",
                "events/team_id=4/*",
                "other/team_id=4/part-0.parquet");
        for (String deniedKey : deniedKeys) {
            assertThat(context.allowsStorageTarget("example-remote", REGION, deniedKey)).as(deniedKey).isFalse();
            assertTargetDenied(
                    () -> verifier.authorizeStorageTarget(caller, "example-remote", REGION, deniedKey),
                    jwt,
                    ACCESS_KEY,
                    SECRET_KEY,
                    SESSION_TOKEN_VALUE,
                    "example-remote",
                    deniedKey);
        }

        // Wrong bucket, wrong region, and cross-target access.
        assertTargetDenied(
                () -> verifier.authorizeStorageTarget(caller, "other-bucket", REGION, "events/team_id=4/part-0.parquet"),
                jwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE,
                "other-bucket",
                "events/team_id=4/part-0.parquet");
        assertTargetDenied(
                () -> verifier.authorizeStorageTarget(caller, "example-remote", "eu-central-1", "events/team_id=4/part-0.parquet"),
                jwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE,
                "example-remote",
                "events/team_id=4/part-0.parquet");
        assertTargetDenied(
                () -> verifier.authorizeStorageTarget(caller, EVENTS_TARGET, "example-remote", REGION, "imports/team_id=4/part-0.parquet"),
                jwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE,
                "example-remote",
                "imports/team_id=4/part-0.parquet");
        assertTargetDenied(
                () -> verifier.authorizeStorageTarget(caller, IMPORTS_TARGET, "example-owner", REGION, "root/2c/t/fixture_3/manifest"),
                jwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE,
                "example-owner",
                "manifest");

        // Malformed S3 locations.
        for (String badLocation : List.of(
                "http://example-remote/events/team_id=4/part-0.parquet",
                "s3a://example-remote/events/team_id=4/part-0.parquet",
                "s3://user:pass@example-remote/events/team_id=4/part-0.parquet",
                "s3://example-remote/events/team_id=4/part-0.parquet?versionId=1",
                "s3://example-remote/events/team_id=4/part-0.parquet#frag",
                "s3://example-remote/events/team_id=4/../team_id=42/part-0.parquet",
                "s3://example-remote/")) {
            assertTargetDenied(
                    () -> verifier.authorizeStorageLocation(caller, badLocation, REGION),
                    jwt,
                    ACCESS_KEY,
                    SECRET_KEY,
                    SESSION_TOKEN_VALUE,
                    badLocation);
        }

        // Target authorization re-verifies token expiry on every file access.
        AtomicLong nowSeconds = new AtomicLong(NOW.getEpochSecond());
        HoglakeReadSessionVerifier dynamicVerifier = verifier(new AdjustableClock(nowSeconds, ZoneOffset.UTC));
        assertThat(dynamicVerifier.authorizeStorageLocation(caller, EVENTS_TARGET, "s3://example-remote/events/team_id=4/part-0.parquet", REGION).teamId())
                .isEqualTo(4L);
        nowSeconds.set(NOW.getEpochSecond() + 59);
        assertAuthorityDenied(
                () -> dynamicVerifier.authorizeStorageLocation(caller, EVENTS_TARGET, "s3://example-remote/events/team_id=4/part-0.parquet", REGION),
                jwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE);
    }

    @Test
    void testCompactEncodingHeadersDuplicateKeysAndRotation()
            throws Exception
    {
        String jwt = token(key, KID, validClaims());
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, "")));
        for (String invalid : List.of(" " + jwt, jwt + " ", jwt + ".extra", jwt + "=", "tok1.tok2", "tok1..tok2", "x".repeat(65_537))) {
            assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, invalid)), invalid);
        }

        String rawClaims = JSON.writeValueAsString(validClaims());
        for (String badHeader : List.of(
                "[]",
                "{}",
                "{\"alg\":\"none\",\"kid\":\"" + KID + "\"}",
                "{\"alg\":\"HS256\",\"kid\":\"" + KID + "\"}",
                "{\"alg\":\"RS256\",\"kid\":\"" + KID + "\"}",
                "{\"alg\":\"ES256\",\"kid\":\"unknown\"}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"crit\":[\"exp\"]}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"b64\":false}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"jku\":\"https://unused.example.test\"}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"jwk\":{\"kty\":\"EC\"}}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"x5u\":\"https://unused.example.test\"}")) {
            assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, sign(key, badHeader, rawClaims))));
        }

        // The verifier rejects duplicate JSON keys and trailing tokens.
        String validHeader = "{\"alg\":\"ES256\",\"kid\":\"%s\"}".formatted(KID);
        String duplicateClaim = rawClaims.substring(0, rawClaims.length() - 1) + ",\"usr\":\"example.other\"}";
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, sign(key, validHeader, duplicateClaim))));
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, sign(key, validHeader, rawClaims + " {}"))));

        // The verifier rejects a wrong signature.
        String wrongSignature = token(keyPair(), KID, validClaims());
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, wrongSignature)), wrongSignature);

        // Mounted JWKS reload waits 30 seconds and keeps prior keys on reload failure.
        KeyPair rotated = keyPair();
        String rotatedJwt = token(rotated, "rotated-key", validClaims());
        writeJwks(jwksFile, rotated, "rotated-key");
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, rotatedJwt)));
        assertThat(verifier.verify(session(USER, PRINCIPAL, jwt)).token()).isEqualTo(jwt);

        ticker.set(SECONDS.toNanos(30));
        assertThat(verifier.verify(session(USER, PRINCIPAL, rotatedJwt)).token()).isEqualTo(rotatedJwt);
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, jwt)));

        Files.writeString(jwksFile, "not-valid-json", UTF_8);
        ticker.set(SECONDS.toNanos(60));
        assertThat(verifier.verify(session(USER, PRINCIPAL, rotatedJwt)).token()).isEqualTo(rotatedJwt);

        // A well-formed empty JWKS withdraws the last signing key on reload.
        Files.writeString(jwksFile, "{\"keys\":[]}", UTF_8);
        ticker.set(SECONDS.toNanos(90));
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, rotatedJwt)), rotatedJwt);

        // A subsequent reload restores verification once a new key is published.
        KeyPair restored = keyPair();
        String restoredJwt = token(restored, "restored-key", validClaims());
        writeJwks(jwksFile, restored, "restored-key");
        ticker.set(SECONDS.toNanos(120));
        assertThat(verifier.verify(session(USER, PRINCIPAL, restoredJwt)).token()).isEqualTo(restoredJwt);
        assertAuthorityDenied(() -> verifier.verify(session(USER, PRINCIPAL, rotatedJwt)), rotatedJwt);
    }

    @Test
    void testConfigurationValidationIsStrictAndRedacted()
            throws Exception
    {
        assertBadConfig(() -> new HoglakeReadSessionVerifier(" ", jwksFile, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, directory.resolve("missing.json"), DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, "storage-session", REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, " ", REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, ImmutableSet.of(EVENTS_TARGET), ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, ImmutableSet.of(EVENTS_TARGET, " "), ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(new UUID(0, 0)), Optional.of(REGION)));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of("INVALID_REGION")));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION), 601, 900, Clock.systemUTC(), System::nanoTime));
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, jwksFile, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION), 60, 901, Clock.systemUTC(), System::nanoTime));

        // The verifier rejects an empty JWKS at startup.
        Path emptyJwks = directory.resolve("empty-jwks.json");
        Files.writeString(emptyJwks, "{\"keys\":[]}", UTF_8);
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, emptyJwks, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));

        // The verifier rejects private key fields in mounted JWKS.
        Map<String, Object> withPrivateField = new LinkedHashMap<>(jwk(key, KID));
        withPrivateField.put("d", "synthetic-private-scalar");
        Path badJwks = directory.resolve("private-jwks.json");
        Files.writeString(badJwks, JSON.writeValueAsString(Map.of("keys", List.of(withPrivateField))), UTF_8);
        assertBadConfig(() -> new HoglakeReadSessionVerifier(ISSUER, badJwks, DEFAULT_PROFILE, REMOTE_TARGETS, ImmutableSet.of(BACKEND_UUID), Optional.of(REGION)));
    }

    private HoglakeReadSessionVerifier verifier(Clock clock)
    {
        return new HoglakeReadSessionVerifier(
                ISSUER,
                jwksFile,
                DEFAULT_PROFILE,
                REMOTE_TARGETS,
                ImmutableSet.of(BACKEND_UUID),
                Optional.of(REGION),
                clock,
                ticker::get);
    }

    private HoglakeReadSessionContext verify(Map<String, Object> claims)
    {
        return verifier.verify(session(USER, PRINCIPAL, token(key, KID, claims)));
    }

    private void denyMutation(Consumer<Map<String, Object>> mutation)
    {
        Map<String, Object> mutated = validClaims();
        mutation.accept(mutated);
        String jwt = token(key, KID, mutated);
        assertAuthorityDenied(
                () -> verifier.verify(session(USER, PRINCIPAL, jwt)),
                jwt,
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_TOKEN_VALUE);
    }

    @Test
    void testWritableSessionAcceptedAndStrictlyCoupled()
    {
        HoglakeReadSessionContext context = verify(validWritableClaims());
        assertThat(context.writable()).isTrue();
        assertThat(context.primaryWritePrefixGrant()).isPresent();
        context.requirePrimaryWriteLocation("s3://example-customer/data/team_4/scratch/t/part-0.parquet", REGION);

        // cat write with prd read is rejected
        Map<String, Object> catOnly = validWritableClaims();
        catOnly.put("prd", Map.of("team_id", Map.of("4", "read")));
        assertAuthorityDenied(() -> verify(catOnly), ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);

        // prd write with cat read is rejected
        Map<String, Object> prdOnly = validWritableClaims();
        prdOnly.put("cat", Map.of(CATALOG, List.of("read")));
        assertAuthorityDenied(() -> verify(prdOnly), ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);

        // s3:DeleteObject in primary grant is rejected
        Map<String, Object> withDelete = validWritableClaims();
        for (Map<String, Object> grant : grants(withDelete)) {
            if (CATALOG.equals(grant.get("target"))) {
                grant.put("actions", List.of("s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"));
            }
        }
        assertAuthorityDenied(() -> verify(withDelete), ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);

        // Writable manifest grant is rejected
        Map<String, Object> writableManifest = validWritableClaims();
        for (Map<String, Object> grant : grants(writableManifest)) {
            if (BACKEND_UUID.toString().equals(grant.get("target"))) {
                grant.put("actions", HoglakeReadSessionVerifier.WRITE_ACTIONS);
            }
        }
        assertAuthorityDenied(() -> verify(writableManifest), ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);

        // Writable primary prefix outside data/team_<id>/ is rejected
        Map<String, Object> wrongPrefix = validWritableClaims();
        for (Map<String, Object> grant : grants(wrongPrefix)) {
            if (CATALOG.equals(grant.get("target"))) {
                grant.put("prefix", "data/local_team_4/");
            }
        }
        assertAuthorityDenied(() -> verify(wrongPrefix), ACCESS_KEY, SECRET_KEY, SESSION_TOKEN_VALUE);
    }

    private static Map<String, Object> validWritableClaims()
    {
        Map<String, Object> claims = validClaims();
        claims.put("cat", Map.of(CATALOG, List.of("read", "write")));
        claims.put("prd", Map.of("team_id", Map.of("4", "write")));
        for (Map<String, Object> grant : grants(claims)) {
            if (CATALOG.equals(grant.get("target"))) {
                grant.remove("object_key");
                grant.put("prefix", "data/team_4/");
                grant.put("actions", new ArrayList<>(HoglakeReadSessionVerifier.WRITE_ACTIONS));
            }
        }
        return claims;
    }

    private static Map<String, Object> validClaims()
    {
        long iat = NOW.getEpochSecond() - 1;
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("iss", ISSUER);
        claims.put("aud", new ArrayList<>(List.of("trino-session", "hoglake-session", "hoglite-session")));
        claims.put("sub", PRINCIPAL);
        claims.put("usr", PRINCIPAL);
        claims.put("jti", "token-a");
        claims.put("profile", DEFAULT_PROFILE);
        claims.put("version", 1);
        claims.put("purpose", "user-session");
        claims.put("ten", "scoped");
        claims.put("org", "example-org");
        claims.put("primary_catalog", CATALOG);
        claims.put("sid", "session-a");
        claims.put("generation", 1);
        claims.put("session_deadline", iat + 120);
        claims.put("iat", iat);
        claims.put("nbf", iat);
        claims.put("exp", iat + 60);
        claims.put("cat", new LinkedHashMap<>(Map.of(CATALOG, List.of("read"))));
        claims.put("prd", new LinkedHashMap<>(Map.of("team_id", new LinkedHashMap<>(Map.of("4", "read")))));
        claims.put("rtg", new LinkedHashMap<>(Map.of("b", "hoglite", "be", BACKEND_ID, "t", "fixture_3")));
        claims.put("remote_schemas", new LinkedHashMap<>(Map.of("team", List.of("read"))));
        claims.put("aws", awsBundle(iat + 60));

        List<Map<String, Object>> grants = new ArrayList<>();
        grants.add(objectGrant(CATALOG, "example-customer", "tables/data.parquet"));
        grants.add(objectGrant(BACKEND_ID, "example-owner", "root/2c/t/fixture_3/manifest"));
        grants.add(prefixGrant(EVENTS_TARGET, "example-remote", "events/team_id=4/"));
        grants.add(prefixGrant(IMPORTS_TARGET, "example-remote", "imports/team_id=4/"));
        claims.put("grants", grants);
        return claims;
    }

    private static Map<String, Object> awsBundle(long expiresAt)
    {
        Map<String, Object> aws = new LinkedHashMap<>();
        aws.put("access_key_id", ACCESS_KEY);
        aws.put("secret_access_key", SECRET_KEY);
        aws.put("session_token", SESSION_TOKEN_VALUE);
        aws.put("region", REGION);
        aws.put("expires_at", expiresAt);
        return aws;
    }

    private static Map<String, Object> objectGrant(String target, String bucket, String objectKey)
    {
        Map<String, Object> grant = new LinkedHashMap<>();
        grant.put("catalog", CATALOG);
        grant.put("target", target);
        grant.put("bucket", bucket);
        grant.put("object_key", objectKey);
        grant.put("region", REGION);
        grant.put("actions", new ArrayList<>(List.of("s3:GetObject")));
        return grant;
    }

    private static Map<String, Object> prefixGrant(String target, String bucket, String prefix)
    {
        Map<String, Object> grant = new LinkedHashMap<>();
        grant.put("catalog", CATALOG);
        grant.put("target", target);
        grant.put("bucket", bucket);
        grant.put("prefix", prefix);
        grant.put("region", REGION);
        grant.put("actions", new ArrayList<>(List.of("s3:GetObject")));
        return grant;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> aws(Map<String, Object> claims)
    {
        return (Map<String, Object>) claims.get("aws");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> grants(Map<String, Object> claims)
    {
        return (List<Map<String, Object>>) claims.get("grants");
    }

    private static void assertBadConfig(Runnable action)
    {
        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid dedicated Hoglake read session configuration")
                .hasNoCause()
                .satisfies(failure -> assertThat(failure.getSuppressed()).isEmpty());
    }

    private static void assertAuthorityDenied(Runnable action, String... sensitive)
    {
        assertRedactedDenial(action, AUTHORITY_DENIED, sensitive);
    }

    private static void assertTargetDenied(Runnable action, String... sensitive)
    {
        assertRedactedDenial(action, TARGET_DENIED, sensitive);
    }

    private static void assertRedactedDenial(Runnable action, String message, String... sensitive)
    {
        assertThatThrownBy(action::run)
                .isInstanceOf(TrinoException.class)
                .hasMessage(message)
                .hasNoCause()
                .satisfies(failure -> {
                    assertThat(((TrinoException) failure).getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode());
                    assertThat(failure.getSuppressed()).isEmpty();
                    StringWriter trace = new StringWriter();
                    failure.printStackTrace(new PrintWriter(trace));
                    assertThat(trace.toString()).doesNotContain("Bearer ");
                    if (sensitive.length > 0) {
                        assertThat(trace.toString()).doesNotContain(sensitive);
                    }
                });
    }

    private static final class AdjustableClock
            extends Clock
    {
        private final AtomicLong seconds;
        private final ZoneId zone;

        private AdjustableClock(AtomicLong seconds, ZoneId zone)
        {
            this.seconds = seconds;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone()
        {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return new AdjustableClock(seconds, zone);
        }

        @Override
        public Instant instant()
        {
            return Instant.ofEpochSecond(seconds.get());
        }
    }
}
