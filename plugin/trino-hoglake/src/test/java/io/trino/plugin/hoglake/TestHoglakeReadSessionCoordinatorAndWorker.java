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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.plugin.hoglake.HoglakeReadSessionVerifier.DEFAULT_PROFILE;
import static io.trino.plugin.hoglake.HoglakeReadSessionVerifier.SESSION_TOKEN;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.BACKEND_ID;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.ISSUER;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies dedicated read sessions on coordinator and worker paths.
 * Local customer table reads stay disabled on the shared connector.
 */
final class TestHoglakeReadSessionCoordinatorAndWorker
{
    private static final Instant NOW = Instant.ofEpochSecond(2_000_000_001L);
    private static final UUID BACKEND_UUID = UUID.fromString(BACKEND_ID);
    private static final String KID = "synthetic-node-key";
    private static final String REGION = "us-east-1";
    private static final String EVENTS_TARGET = "gigahog:posthog";
    private static final String IMPORTS_TARGET = "gigahog:imports";
    private static final Set<String> REMOTE_TARGETS = ImmutableSet.of(EVENTS_TARGET, IMPORTS_TARGET);
    private static final CatalogHandle CATALOG_HANDLE = createRootCatalogHandle(new CatalogName("hoglake"), new CatalogVersion("test"));
    private static final String AUTHORITY_DENIED = "Dedicated Hoglake read session authority is invalid or unavailable";
    private static final String TARGET_DENIED = "Dedicated Hoglake read session does not allow this storage target";

    @TempDir
    Path directory;

    @Test
    void testCoordinatorAndWorkerVerifyReconstructedSession()
    {
        KeyPair key = keyPair();
        Path coordinatorJwks = directory.resolve("coordinator-jwks.json");
        Path workerJwks = directory.resolve("worker-jwks.json");
        writeJwks(coordinatorJwks, key, KID);
        writeJwks(workerJwks, key, KID);

        HoglakeReadSessionVerifier coordinatorVerifier = verifier(coordinatorJwks);
        HoglakeReadSessionVerifier workerVerifier = verifier(workerJwks);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        Session coordinatorSession = querySession(alice.mappedUser(), alice.principal(), alice.jwt());
        ConnectorSession coordinatorConnectorSession = coordinatorSession.toConnectorSession(CATALOG_HANDLE);

        HoglakeReadSessionContext coordinatorContext = coordinatorVerifier.authorizeStorageLocation(
                coordinatorConnectorSession,
                EVENTS_TARGET,
                "s3://example-remote/events/team_id=4/part-0.parquet",
                REGION);
        assertThat(coordinatorContext.catalog()).isEqualTo("catalog-a");
        assertThat(coordinatorContext.principal()).isEqualTo("example.alice");
        assertThat(coordinatorContext.teamId()).isEqualTo(4L);

        // Reconstruct the worker session from the session representation and protected credentials.
        // Protected credential task dispatch is covered by TestHttpRemoteTask.testProtectedCredentialDispatch.
        SessionPropertyManager properties = new SessionPropertyManager();
        Session workerSession = coordinatorSession.toSessionRepresentation()
                .toSession(properties, coordinatorSession.getIdentity().getExtraCredentials(), Optional.empty());
        ConnectorSession workerConnectorSession = workerSession.toConnectorSession(CATALOG_HANDLE);

        HoglakeReadSessionContext workerContext = workerVerifier.authorizeStorageLocation(
                workerConnectorSession,
                "catalog-a",
                "s3://example-customer/catalog-a/data.parquet",
                REGION);
        assertThat(workerContext.token()).isEqualTo(alice.jwt());
        assertThat(workerContext.storage().accessKeyId()).isEqualTo("ACCESSA");

        // Workers also verify ConnectorIdentity on file open paths.
        HoglakeReadSessionContext workerIdentityContext = workerVerifier.authorizeStorageTarget(
                workerConnectorSession.getIdentity(),
                IMPORTS_TARGET,
                "example-remote",
                REGION,
                "imports/team_id=4/file.parquet");
        assertThat(workerIdentityContext.principal()).isEqualTo("example.alice");

        // Prefix boundary and sibling access fail on the worker.
        assertRedactedDenial(
                () -> workerVerifier.authorizeStorageTarget(
                        workerConnectorSession.getIdentity(),
                        EVENTS_TARGET,
                        "example-remote",
                        REGION,
                        "events/team_id=42/part-0.parquet"),
                TARGET_DENIED,
                alice.jwt(),
                "ACCESSA",
                "SECRETA",
                "TOKENA",
                "team_id=42");

        // A worker with different mounted public keys rejects the coordinator token.
        Path otherWorkerJwks = directory.resolve("other-worker-jwks.json");
        writeJwks(otherWorkerJwks, keyPair(), KID);
        HoglakeReadSessionVerifier foreignWorkerVerifier = verifier(otherWorkerJwks);
        assertRedactedDenial(
                () -> foreignWorkerVerifier.verify(workerConnectorSession),
                AUTHORITY_DENIED,
                alice.jwt(),
                "ACCESSA",
                "SECRETA",
                "TOKENA");
    }

    @Test
    void testDerivedEffectiveUserLosesTokenOnCoordinatorAndWorker()
    {
        KeyPair key = keyPair();
        Path jwks = directory.resolve("derived-jwks.json");
        writeJwks(jwks, key, KID);
        HoglakeReadSessionVerifier verifier = verifier(jwks);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        Identity original = identity(alice.mappedUser(), alice.principal(), alice.jwt());
        Identity effective = Identity.from(original).withUser("example.definer").build();
        Session derived = testSessionBuilder()
                .setCatalog("hoglake")
                .setSchema("team_4")
                .setIdentity(effective)
                .setOriginalIdentity(original)
                .build();

        ConnectorSession coordinatorConnectorSession = derived.toConnectorSession(CATALOG_HANDLE);
        assertThat(coordinatorConnectorSession.getIdentity().getExtraCredentials()).doesNotContainKey(SESSION_TOKEN);
        assertRedactedDenial(() -> verifier.verify(coordinatorConnectorSession), AUTHORITY_DENIED, alice.jwt(), "SECRETA", "TOKENA");

        Session workerDerived = derived.toSessionRepresentation()
                .toSession(new SessionPropertyManager(), derived.getIdentity().getExtraCredentials(), Optional.empty());
        ConnectorSession workerConnectorSession = workerDerived.toConnectorSession(CATALOG_HANDLE);
        assertThat(workerConnectorSession.getIdentity().getExtraCredentials()).doesNotContainKey(SESSION_TOKEN);
        assertRedactedDenial(() -> verifier.verify(workerConnectorSession), AUTHORITY_DENIED, alice.jwt(), "SECRETA", "TOKENA");
    }

    @Test
    void testTwoConcurrentCallersStayIsolatedAcrossCoordinatorAndWorker()
            throws Exception
    {
        KeyPair key = keyPair();
        Path coordinatorJwks = directory.resolve("concurrent-coordinator-jwks.json");
        Path workerJwks = directory.resolve("concurrent-worker-jwks.json");
        writeJwks(coordinatorJwks, key, KID);
        writeJwks(workerJwks, key, KID);

        HoglakeReadSessionVerifier coordinatorVerifier = verifier(coordinatorJwks);
        HoglakeReadSessionVerifier workerVerifier = verifier(workerJwks);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        CallerFixture bob = caller(key, "catalog-b", "tenant_2", "f4", "other.bob", "other.mapped_bob", 42, "ACCESSB", "SECRETB", "TOKENB");
        SessionPropertyManager properties = new SessionPropertyManager();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int index = 0; index < 24; index++) {
            tasks.add(() -> {
                assertCallerIsolated(coordinatorVerifier, workerVerifier, properties, alice, bob);
                return null;
            });
            tasks.add(() -> {
                assertCallerIsolated(coordinatorVerifier, workerVerifier, properties, bob, alice);
                return null;
            });
        }

        try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        }
    }

    @Test
    void testSharedConnectorRejectsDedicatedTokenAndKeepsLocalReadsDisabled()
    {
        KeyPair key = keyPair();
        Path jwks = directory.resolve("shared-connector-jwks.json");
        writeJwks(jwks, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        ConnectorSession callerSession = querySession(alice.mappedUser(), alice.principal(), alice.jwt()).toConnectorSession(CATALOG_HANDLE);

        for (boolean remoteRead : List.of(false, true)) {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("hoglake.multi-tenant", "true")
                    .put("hoglake.uri", "http://127.0.0.1:9")
                    .put("hoglake.shared.jwks-file", jwks.toString())
                    .put("hoglake.shared.issuer", ISSUER)
                    .put("hoglake.shared.backend-bindings", "{\"%s\":\"hoglite\"}".formatted(BACKEND_ID));
            if (remoteRead) {
                properties.put("hoglake.shared.remote-read", "true")
                        .put("s3.endpoint", "http://127.0.0.1:9")
                        .put("s3.region", REGION);
            }
            Connector connector = new HoglakeConnectorFactory().create("hoglake", properties.buildOrThrow(), new TestingConnectorContext());
            try {
                var transaction = connector.beginTransaction(READ_COMMITTED, true, true);
                assertThatThrownBy(() -> connector.getMetadata(callerSession, transaction))
                        .isInstanceOfSatisfying(TrinoException.class, failure ->
                                assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()))
                        .hasMessage("Shared Hoglake session authority is invalid or unavailable")
                        .hasNoCause();
            }
            finally {
                connector.shutdown();
            }
        }
    }

    private static void assertCallerIsolated(
            HoglakeReadSessionVerifier coordinatorVerifier,
            HoglakeReadSessionVerifier workerVerifier,
            SessionPropertyManager properties,
            CallerFixture caller,
            CallerFixture other)
    {
        Session coordinatorSession = querySession(caller.mappedUser(), caller.principal(), caller.jwt());
        ConnectorSession coordinatorConnector = coordinatorSession.toConnectorSession(CATALOG_HANDLE);
        Session workerSession = coordinatorSession.toSessionRepresentation()
                .toSession(properties, coordinatorSession.getIdentity().getExtraCredentials(), Optional.empty());
        ConnectorSession workerConnector = workerSession.toConnectorSession(CATALOG_HANDLE);

        HoglakeReadSessionContext coordinatorContext = coordinatorVerifier.authorizeStorageLocation(
                coordinatorConnector,
                EVENTS_TARGET,
                caller.eventsLocation(),
                REGION);
        HoglakeReadSessionContext workerContext = workerVerifier.authorizeStorageLocation(
                workerConnector.getIdentity(),
                EVENTS_TARGET,
                caller.eventsLocation(),
                REGION);

        assertThat(coordinatorContext.catalog()).isEqualTo(caller.catalog());
        assertThat(coordinatorContext.principal()).isEqualTo(caller.principal());
        assertThat(coordinatorContext.teamId()).isEqualTo(caller.teamId());
        assertThat(coordinatorContext.storage().accessKeyId()).isEqualTo(caller.accessKey());

        assertThat(workerContext.catalog()).isEqualTo(caller.catalog());
        assertThat(workerContext.principal()).isEqualTo(caller.principal());
        assertThat(workerContext.teamId()).isEqualTo(caller.teamId());
        assertThat(workerContext.storage().accessKeyId()).isEqualTo(caller.accessKey());

        // Neither node allows the caller to read another caller's storage path.
        assertRedactedDenial(
                () -> coordinatorVerifier.authorizeStorageLocation(coordinatorConnector, EVENTS_TARGET, other.eventsLocation(), REGION),
                TARGET_DENIED,
                caller.jwt(),
                caller.secretKey(),
                caller.sessionToken());
        assertRedactedDenial(
                () -> workerVerifier.authorizeStorageLocation(workerConnector.getIdentity(), EVENTS_TARGET, other.eventsLocation(), REGION),
                TARGET_DENIED,
                caller.jwt(),
                caller.secretKey(),
                caller.sessionToken());

        // Mismatched principal with another caller's JWT fails on both coordinator and worker.
        ConnectorSession mismatched = querySession(caller.mappedUser(), caller.principal(), other.jwt()).toConnectorSession(CATALOG_HANDLE);
        assertRedactedDenial(() -> coordinatorVerifier.verify(mismatched), AUTHORITY_DENIED, other.jwt(), other.secretKey(), other.sessionToken());
        assertRedactedDenial(() -> workerVerifier.verify(mismatched.getIdentity()), AUTHORITY_DENIED, other.jwt(), other.secretKey(), other.sessionToken());
    }

    private HoglakeReadSessionVerifier verifier(Path jwksFile)
    {
        return new HoglakeReadSessionVerifier(
                ISSUER,
                jwksFile,
                DEFAULT_PROFILE,
                REMOTE_TARGETS,
                ImmutableSet.of(BACKEND_UUID),
                Optional.of(REGION),
                Clock.fixed(NOW, ZoneOffset.UTC),
                () -> 0L);
    }

    private static CallerFixture caller(
            KeyPair key,
            String catalog,
            String tenant,
            String shard,
            String principal,
            String mappedUser,
            long teamId,
            String accessKey,
            String secretKey,
            String sessionTokenValue)
    {
        long iat = NOW.getEpochSecond() - 1;
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("iss", ISSUER);
        claims.put("aud", List.of("trino-session", "hoglake-session", "hoglite-session"));
        claims.put("sub", principal);
        claims.put("usr", principal);
        claims.put("jti", "token-" + catalog);
        claims.put("profile", DEFAULT_PROFILE);
        claims.put("version", 1);
        claims.put("purpose", "user-session");
        claims.put("ten", "scoped");
        claims.put("org", "org-" + catalog);
        claims.put("primary_catalog", catalog);
        claims.put("sid", "session-" + catalog);
        claims.put("generation", 1);
        claims.put("session_deadline", iat + 120);
        claims.put("iat", iat);
        claims.put("nbf", iat);
        claims.put("exp", iat + 60);
        claims.put("cat", Map.of(catalog, List.of("read")));
        claims.put("prd", Map.of("team_id", Map.of(Long.toString(teamId), "read")));
        claims.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", tenant));
        claims.put("remote_schemas", Map.of("team", List.of("read")));
        claims.put("aws", Map.of(
                "access_key_id", accessKey,
                "secret_access_key", secretKey,
                "session_token", sessionTokenValue,
                "region", REGION,
                "expires_at", iat + 60));
        claims.put("grants", List.of(
                Map.of(
                        "catalog", catalog,
                        "target", catalog,
                        "bucket", "example-customer",
                        "object_key", catalog + "/data.parquet",
                        "region", REGION,
                        "actions", List.of("s3:GetObject")),
                Map.of(
                        "catalog", catalog,
                        "target", BACKEND_ID,
                        "bucket", "example-owner",
                        "object_key", "root/" + shard + "/t/" + tenant + "/manifest",
                        "region", REGION,
                        "actions", List.of("s3:GetObject")),
                Map.of(
                        "catalog", catalog,
                        "target", EVENTS_TARGET,
                        "bucket", "example-remote",
                        "prefix", "events/team_id=" + teamId + "/",
                        "region", REGION,
                        "actions", List.of("s3:GetObject")),
                Map.of(
                        "catalog", catalog,
                        "target", IMPORTS_TARGET,
                        "bucket", "example-remote",
                        "prefix", "imports/team_id=" + teamId + "/",
                        "region", REGION,
                        "actions", List.of("s3:GetObject"))));
        String jwt = token(key, KID, claims);
        return new CallerFixture(catalog, principal, mappedUser, teamId, accessKey, secretKey, sessionTokenValue, jwt);
    }

    private static Identity identity(String mappedUser, String principal, String jwt)
    {
        Identity.Builder builder = Identity.forUser(mappedUser)
                .withPrincipal(new BasicPrincipal(principal));
        if (jwt != null) {
            builder.withExtraCredentials(Map.of(SESSION_TOKEN, jwt));
        }
        return builder.build();
    }

    private static Session querySession(String mappedUser, String principal, String jwt)
    {
        Identity identity = identity(mappedUser, principal, jwt);
        return testSessionBuilder()
                .setCatalog("hoglake")
                .setSchema("team_4")
                .setIdentity(identity)
                .setOriginalIdentity(identity)
                .build();
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
                    assertThat(trace.toString()).doesNotContain("Bearer ").doesNotContain(sensitive);
                });
    }

    private record CallerFixture(
            String catalog,
            String principal,
            String mappedUser,
            long teamId,
            String accessKey,
            String secretKey,
            String sessionToken,
            String jwt)
    {
        String eventsLocation()
        {
            return "s3://example-remote/events/team_id=" + teamId + "/part-0.parquet";
        }
    }
}
