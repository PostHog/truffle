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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.Plugin;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingConnectorContext;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeReadSessionVerifier.DEFAULT_PROFILE;
import static io.trino.plugin.hoglake.HoglakeReadSessionVerifier.SESSION_TOKEN;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.BACKEND_ID;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.ISSUER;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies dedicated local table lookup, pinned scan, and worker Parquet reads.
 */
final class TestHoglakeDedicatedLocalRead
{
    private static final Instant NOW = Instant.ofEpochSecond(2_000_000_001L);
    private static final UUID BACKEND_UUID = UUID.fromString(BACKEND_ID);
    private static final String KID = "synthetic-dedicated-key";
    private static final String REGION = "us-east-1";
    private static final String EVENTS_TARGET = "gigahog:posthog";
    private static final String IMPORTS_TARGET = "gigahog:imports";
    private static final Set<String> REMOTE_TARGETS = ImmutableSet.of(EVENTS_TARGET, IMPORTS_TARGET);
    private static final CatalogHandle CATALOG_HANDLE = createRootCatalogHandle(new CatalogName("hoglake"), new CatalogVersion("test"));
    private static final String ALICE_TABLE_UUID = "11111111-aaaa-4111-8111-111111111111";
    private static final String BOB_TABLE_UUID = "22222222-bbbb-4222-8222-222222222222";
    private static final String AUTHORITY_DENIED = "Dedicated Hoglake read session authority is invalid or unavailable";
    private static final String TARGET_DENIED = "Dedicated Hoglake read session does not allow this storage target";
    private static final String REQUEST_DENIED = "Dedicated Hoglake read session does not allow this request";
    private static final String LOCAL_TABLE_DENIED = "Dedicated Hoglake local read requires a valid pinned local table";

    @TempDir
    Path directory;

    @Test
    void testScanPinnedValidatesArgumentsAndSendsPinnedQuery()
            throws Exception
    {
        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, _ -> new Response(200, "[]"))) {
            try (HoglakeClient client = new HoglakeClient(server.baseUri().toASCIIString(), "catalog-a")) {
                assertThat(client.scanPinned("reports", "metrics", 5L, ALICE_TABLE_UUID)).isEmpty();
                assertThat(client.scan("reports", "metrics", 5L)).isEmpty();

                assertThatThrownBy(() -> client.scanPinned("reports", "metrics", -1L, ALICE_TABLE_UUID))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> client.scanPinned("reports", "metrics", 5L, null))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> client.scanPinned("reports", "metrics", 5L, "   "))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }

        assertThat(requests).extracting(RecordedRequest::method)
                .containsExactly("GET", "GET");
        assertThat(requests).extracting(RecordedRequest::rawPathAndQuery)
                .containsExactly(
                        "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID,
                        "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?snapshot=5");
    }

    @Test
    void testRouteGuardAllowsOnlyVerifiedCatalogPointRoutesAndPinnedScan()
    {
        HoglakeClient.RouteGuard guard = HoglakeDedicatedReadPath.createRouteGuard("", "catalog-a");

        guard.check("GET", URI.create("http://metadata.example.test/v1/local-only/catalogs/catalog-a/namespaces/reports"));
        guard.check("GET", URI.create("http://metadata.example.test/v1/local-only/catalogs/catalog-a/namespaces/reports/tables/metrics"));
        guard.check("GET", URI.create("http://metadata.example.test/v1/local-only/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?snapshot=0&table_uuid=" + ALICE_TABLE_UUID));
        guard.check("GET", URI.create("http://metadata.example.test/v1/local-only/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?table_uuid=" + ALICE_TABLE_UUID + "&snapshot=42"));

        String local = "/v1/local-only/catalogs/catalog-a";
        List<String> disallowedPaths = List.of(
                // The ordinary routes, which a remote schema can relay, are never sent.
                "/v1/catalogs/catalog-a/namespaces/reports",
                "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics",
                "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID,
                "/v1/local-only/catalogs/catalog-b/namespaces/reports",
                "/v1/local-only/namespaces/reports",
                local,
                local + "/namespaces",
                local + "/namespaces/reports/tables",
                local + "/namespaces/reports?x=1",
                local + "/namespaces/reports/tables/metrics?snapshot=5",
                local + "/namespaces/reports/tables/metrics/scan",
                local + "/namespaces/reports/tables/metrics/scan?snapshot=5",
                local + "/namespaces/reports/tables/metrics/scan?table_uuid=" + ALICE_TABLE_UUID,
                local + "/namespaces/reports/tables/metrics/scan?snapshot=-1&table_uuid=" + ALICE_TABLE_UUID,
                local + "/namespaces/reports/tables/metrics/scan?snapshot=05&table_uuid=" + ALICE_TABLE_UUID,
                local + "/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID.toUpperCase(ENGLISH),
                local + "/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=not-a-uuid",
                local + "/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID + "&extra=1",
                local + "/namespaces/reports/tables/metrics/scan?snapshot=5&snapshot=6&table_uuid=" + ALICE_TABLE_UUID,
                local + "/namespaces/..%2fother/tables/metrics",
                local + "/namespaces/reports/tables/metrics#fragment");

        for (String path : disallowedPaths) {
            assertRedactedDenial(
                    () -> guard.check("GET", URI.create("http://metadata.example.test" + path)),
                    REQUEST_DENIED,
                    path);
        }

        for (String method : List.of("POST", "PUT", "DELETE", "PATCH")) {
            assertRedactedDenial(
                    () -> guard.check(method, URI.create("http://metadata.example.test/v1/local-only/catalogs/catalog-a/namespaces/reports")),
                    REQUEST_DENIED,
                    "catalog-a");
        }
    }

    @Test
    void testRemoteRouteGuardAllowsOnlyTheFourRoutesOfTheTeamSchema()
    {
        HoglakeClient.RouteGuard guard = HoglakeDedicatedReadPath.createRemoteRouteGuard("", "catalog-a", "team_4");
        String team = "/v1/catalogs/catalog-a/namespaces/team_4";

        guard.check("GET", URI.create("http://metadata.example.test" + team));
        guard.check("GET", URI.create("http://metadata.example.test" + team + "/tables"));
        guard.check("GET", URI.create("http://metadata.example.test" + team + "/tables/events"));
        guard.check("GET", URI.create("http://metadata.example.test" + team + "/tables/events/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID));

        List<String> disallowedPaths = List.of(
                // Another team, a local schema, another catalog and the local-only routes are never sent.
                "/v1/catalogs/catalog-a/namespaces/team_5",
                "/v1/catalogs/catalog-a/namespaces/team_04",
                "/v1/catalogs/catalog-a/namespaces/TEAM_4",
                "/v1/catalogs/catalog-a/namespaces/reports",
                "/v1/catalogs/catalog-a/namespaces/team_5/tables",
                "/v1/catalogs/catalog-a/namespaces/team_5/tables/events",
                "/v1/catalogs/catalog-a/namespaces/team_5/tables/events/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID,
                "/v1/catalogs/catalog-b/namespaces/team_4",
                "/v1/local-only/catalogs/catalog-a/namespaces/team_4",
                "/v1/catalogs/catalog-a",
                "/v1/catalogs/catalog-a/namespaces",
                team + "?x=1",
                team + "/tables?snapshot=5",
                team + "/tables/events?snapshot=5",
                team + "/tables/events?totals=true",
                team + "/tables/events/files",
                team + "/tables/events/scan",
                team + "/tables/events/scan?snapshot=5",
                team + "/tables/events/scan?table_uuid=" + ALICE_TABLE_UUID,
                team + "/tables/events/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID + "&include=column_stats",
                team + "/tables/..%2fother",
                team + "/tables/events#fragment");
        for (String path : disallowedPaths) {
            assertRedactedDenial(
                    () -> guard.check("GET", URI.create("http://metadata.example.test" + path)),
                    REQUEST_DENIED,
                    path);
        }
        for (String method : List.of("POST", "PUT", "DELETE", "PATCH")) {
            assertRedactedDenial(() -> guard.check(method, URI.create("http://metadata.example.test" + team)), REQUEST_DENIED, "catalog-a");
        }
    }

    @Test
    void testRemoteTeamSchemaReadsAndCountsOnlyGrantCheckedFiles()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("remote-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L, 303L), List.of("alpha", "beta", "gamma"));
        String dataPath = "s3://example-remote/events/team_id=4/part-0.parquet";
        CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                Map.of(alice.credentials(), Map.of(dataPath, parquetBytes)));

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeRemoteTable(request, "catalog-a", "team_4", "events", ALICE_TABLE_UUID, 5L, dataPath, parquetBytes.length, 999L, null));
                HoglakeDedicatedReadPath readPath = HoglakeDedicatedReadPath.forRemoteTeamSchema(server.baseUri(), Duration.ofSeconds(10), verifier(jwksFile), fileSystems, REMOTE_TARGETS);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");

            MaterializedResult rows = runner.execute(session, "SELECT id, label FROM hoglake.team_4.events ORDER BY id");
            assertThat(rows.getMaterializedRows())
                    .extracting(row -> List.of(row.getField(0), row.getField(1)))
                    .containsExactly(List.of(101L, "alpha"), List.of(202L, "beta"), List.of(303L, "gamma"));

            // The Host reports 999 rows for the table and for the file. The count comes from the Parquet file.
            int readsBeforeCount = fileSystems.reads().size();
            assertThat(runner.execute(session, "SELECT count(*) FROM hoglake.team_4.events").getOnlyValue()).isEqualTo(3L);
            assertThat(fileSystems.reads().size()).isGreaterThan(readsBeforeCount);

            // The session sees its own team schema and that schema's tables, and no other schema.
            assertThat(runner.execute(session, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("information_schema", "team_4");
            assertThat(runner.execute(session, "SHOW TABLES FROM hoglake.team_4").getOnlyColumnAsSet())
                    .containsExactly("events");
            // The columns of its own tables are listed, from the pinned table.
            assertThat(runner.execute(session, "DESCRIBE hoglake.team_4.events").getMaterializedRows())
                    .extracting(row -> row.getField(0))
                    .containsExactly("id", "label");
            assertThat(runner.execute(session, "SELECT table_name, column_name FROM hoglake.information_schema.columns WHERE table_schema = 'team_4'").getMaterializedRows())
                    .extracting(row -> List.of(row.getField(0), row.getField(1)))
                    .containsExactlyInAnyOrder(List.of("events", "id"), List.of("events", "label"));
            for (String refused : List.of(
                    "SELECT * FROM hoglake.team_5.events",
                    "SELECT * FROM hoglake.reports.metrics",
                    "SHOW TABLES FROM hoglake.team_5",
                    "INSERT INTO hoglake.team_4.events VALUES (404, 'delta')",
                    "DELETE FROM hoglake.team_4.events")) {
                assertThatThrownBy(() -> runner.execute(session, refused)).as(refused).isInstanceOf(QueryFailedException.class);
            }

            // Every request is the caller's exact token on one of the four relayed routes, never a local-only one.
            assertThat(requests).isNotEmpty().allSatisfy(request -> {
                assertThat(request.method()).isEqualTo("GET");
                assertThat(request.authorization()).isEqualTo("Bearer " + alice.jwt());
                assertThat(request.isLocalOnly()).isFalse();
                assertThat(request.rawPathAndQuery()).isIn(
                        "/v1/catalogs/catalog-a/namespaces/team_4",
                        "/v1/catalogs/catalog-a/namespaces/team_4/tables",
                        "/v1/catalogs/catalog-a/namespaces/team_4/tables/events",
                        "/v1/catalogs/catalog-a/namespaces/team_4/tables/events/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID);
            });
        }
        assertThat(fileSystems.reads()).isNotEmpty().allSatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isEqualTo(dataPath);
        });
    }

    @Test
    void testRemoteTeamSchemaRefusesAFileOutsideTheRemoteGrantsBeforeAnyRead()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("remote-denied-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L, 303L), List.of("alpha", "beta", "gamma"));
        String granted = "s3://example-remote/events/team_id=4/part-0.parquet";
        // Another team's prefix, the session's own local catalog prefix, a sibling prefix, and another bucket.
        List<String> outside = List.of(
                "s3://example-remote/events/team_id=5/part-0.parquet",
                "s3://example-remote/events/team_id=40/part-0.parquet",
                alice.tableFile("metrics/part-0.parquet"),
                "s3://example-other/events/team_id=4/part-0.parquet",
                "s3://example-remote/events/team_id=4/../team_id=5/part-0.parquet");
        for (String dataPath : outside) {
            for (boolean vector : List.of(false, true)) {
                // Either the data file is outside, or the data file is inside and only its deletion vector is outside.
                String file = vector ? granted : dataPath;
                String deletePath = vector ? dataPath : null;
                CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                        Map.of(alice.credentials(), Map.of(granted, parquetBytes, dataPath, parquetBytes)));
                List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
                try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeRemoteTable(request, "catalog-a", "team_4", "events", ALICE_TABLE_UUID, 5L, file, parquetBytes.length, 999L, deletePath));
                        HoglakeDedicatedReadPath readPath = HoglakeDedicatedReadPath.forRemoteTeamSchema(server.baseUri(), Duration.ofSeconds(10), verifier(jwksFile), fileSystems, REMOTE_TARGETS);
                        StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
                    Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");
                    // A count that metadata alone could answer is refused like a read of the rows.
                    for (String sql : List.of("SELECT count(*) FROM hoglake.team_4.events", "SELECT id FROM hoglake.team_4.events")) {
                        assertThatThrownBy(() -> runner.execute(session, sql))
                                .as("%s %s %s", dataPath, vector, sql)
                                .isInstanceOf(QueryFailedException.class)
                                .hasMessage(TARGET_DENIED)
                                .satisfies(failure -> assertThat(failure.toString()).doesNotContain("part-0", "team_id", alice.jwt()));
                    }
                }
                assertThat(fileSystems.reads()).as("%s %s", dataPath, vector).isEmpty();
            }
        }
    }

    @Test
    void testRemoteTeamSchemaRefusesALocalTableAndAnotherSessionsTeam()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("remote-local-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(Map.of());
        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        // The server answers team_4 as a local namespace with a local table: a name collision, not a relay.
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeLocalTable(request, "catalog-a", "team_4", "events", ALICE_TABLE_UUID, 5L, "s3://example-remote/events/team_id=4/part-0.parquet", 10, 1L, null));
                HoglakeDedicatedReadPath readPath = HoglakeDedicatedReadPath.forRemoteTeamSchema(server.baseUri(), Duration.ofSeconds(10), verifier(jwksFile), fileSystems, REMOTE_TARGETS);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");
            assertThatThrownBy(() -> runner.execute(session, "SELECT id FROM hoglake.team_4.events"))
                    .isInstanceOf(QueryFailedException.class);
            assertThatThrownBy(() -> runner.execute(session, "SELECT id FROM hoglake.team_5.events"))
                    .isInstanceOf(QueryFailedException.class);
            assertThat(requests).extracting(RecordedRequest::rawPathAndQuery)
                    .allSatisfy(target -> assertThat(target).startsWith("/v1/catalogs/catalog-a/namespaces/team_4"))
                    .noneMatch(target -> target.contains("/scan"));
        }
        assertThat(fileSystems.reads()).isEmpty();
    }

    @Test
    void testRemoteTeamSchemaRequiresARemoteNamespaceBeforeItResolvesATable()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("remote-namespace-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L), List.of("alpha"));
        String dataPath = "s3://example-remote/events/team_id=4/part-0.parquet";
        String namespace = "/v1/catalogs/catalog-a/namespaces/team_4";
        // The table and its scan answer as remote. Only the namespace differs: local, then missing.
        for (Response namespaceAnswer : List.of(new Response(200, "{\"name\":\"team_4\",\"remote\":false}"), new Response(404, "{}"))) {
            CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                    Map.of(alice.credentials(), Map.of(dataPath, parquetBytes)));
            List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
            try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> request.rawPathAndQuery().equals(namespace)
                    ? namespaceAnswer
                    : routeRemoteTable(request, "catalog-a", "team_4", "events", ALICE_TABLE_UUID, 5L, dataPath, parquetBytes.length, 1L, null));
                    HoglakeDedicatedReadPath readPath = HoglakeDedicatedReadPath.forRemoteTeamSchema(server.baseUri(), Duration.ofSeconds(10), verifier(jwksFile), fileSystems, REMOTE_TARGETS);
                    StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
                Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");
                for (String sql : List.of("SELECT id FROM hoglake.team_4.events", "SELECT count(*) FROM hoglake.team_4.events")) {
                    assertThatThrownBy(() -> runner.execute(session, sql))
                            .as("%s %s", namespaceAnswer.status(), sql)
                            .isInstanceOf(QueryFailedException.class);
                }
                // Neither the table nor its scan is asked for.
                assertThat(requests).extracting(RecordedRequest::rawPathAndQuery).containsOnly(namespace);
            }
            assertThat(fileSystems.reads()).isEmpty();
        }
    }

    @Test
    void testRemoteTeamSchemaDiscoveryFailuresNameNoRequestOrResponse()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("remote-redacted-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        String namespace = "/v1/catalogs/catalog-a/namespaces/team_4";
        for (String failing : List.of(namespace, namespace + "/tables", namespace + "/tables/events")) {
            List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
            try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> request.rawPathAndQuery().equals(failing)
                    ? new Response(500, "{\"error\":\"sensitive-response-body\"}")
                    : routeRemoteTable(request, "catalog-a", "team_4", "events", ALICE_TABLE_UUID, 5L, null, 0, 1L, null));
                    HoglakeDedicatedReadPath readPath = HoglakeDedicatedReadPath.forRemoteTeamSchema(server.baseUri(), Duration.ofSeconds(10), verifier(jwksFile), new CredentialCheckingFileSystemFactory(Map.of()), REMOTE_TARGETS);
                    StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
                Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");
                String sql = failing.endsWith("/tables") ? "SHOW TABLES FROM hoglake.team_4" : "SELECT id FROM hoglake.team_4.events";
                assertThatThrownBy(() -> runner.execute(session, sql))
                        .as(failing)
                        .isInstanceOf(QueryFailedException.class)
                        .hasMessageEndingWith("Hoglake read session metadata request failed")
                        .satisfies(failure -> {
                            StringWriter trace = new StringWriter();
                            failure.printStackTrace(new PrintWriter(trace));
                            assertThat(trace.toString()).doesNotContain("sensitive-response-body", "catalog-a", "127.0.0.1", alice.jwt());
                        });
            }
        }
    }

    @Test
    void testSharedCatalogSelectsTheReadSessionPathByAudience()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("shared-read-session-jwks.json");
        writeJwks(jwksFile, key, KID);
        CallerFixture alice = callerAt(key, Instant.now(), "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSLIVE", "SECRETLIVE", "TOKENLIVE");
        Map<String, Object> storageSession = claims("catalog-a", "example.alice", "tenant_1", Instant.now());
        storageSession.put("profile", "storage-session");
        storageSession.put("version", 1);
        storageSession.put("purpose", "user-session");
        storageSession.put("primary_catalog", "catalog-a");
        storageSession.put("remote_schemas", Map.of("team", List.of("read")));
        storageSession.put("aws", Map.of(
                "access_key_id", "synthetic-access-key",
                "secret_access_key", "synthetic-secret-key",
                "session_token", "synthetic-session-token",
                "region", REGION,
                "expires_at", ((Number) storageSession.get("exp")).longValue() + 60));
        String storageSessionJwt = token(key, KID, storageSession);

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        // Both team schemas are remote and empty, so storage is never reached.
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> {
            Response team4 = routeRemoteTable(request, "catalog-a", "team_4", "events", ALICE_TABLE_UUID, 5L, null, 0, 999L, null);
            return team4.status() != 404 ? team4 : routeRemoteTable(request, "catalog-a", "team_7", "events", BOB_TABLE_UUID, 6L, null, 0, 999L, null);
        })) {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("hoglake.multi-tenant", "true")
                    .put("hoglake.uri", server.baseUri().toString())
                    .put("hoglake.shared.jwks-file", jwksFile.toString())
                    .put("hoglake.shared.issuer", ISSUER)
                    .put("hoglake.shared.backend-bindings", "{\"%s\":\"hoglite\"}".formatted(BACKEND_ID))
                    .put("hoglake.shared.remote-read", "true")
                    .put("s3.endpoint", "http://127.0.0.1:9")
                    .put("s3.region", REGION);
            Map<String, String> withoutReadSessions = properties.buildOrThrow();
            Map<String, String> withReadSessions = ImmutableMap.<String, String>builder()
                    .putAll(withoutReadSessions)
                    .put("hoglake.shared.read-session-profile", DEFAULT_PROFILE)
                    .put("hoglake.shared.read-session-remote-targets", EVENTS_TARGET + "," + IMPORTS_TARGET)
                    .buildOrThrow();

            Session readSession = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");
            Session shared = querySession(alice.mappedUser(), alice.principal(), storageSessionJwt, "team_7");
            try (StandaloneQueryRunner runner = sharedRunner(withReadSessions)) {
                // The read session: its own team schema, with a pinned scan that names the table.
                assertThat(runner.execute(readSession, "SELECT count(*) FROM hoglake.team_4.events").getOnlyValue()).isEqualTo(0L);
                assertThat(runner.execute(readSession, "SHOW TABLES FROM hoglake.team_4").getOnlyColumnAsSet()).containsExactly("events");
                assertThat(requests).isNotEmpty().allSatisfy(request -> {
                    assertThat(request.authorization()).isEqualTo("Bearer " + alice.jwt());
                    assertThat(request.isLocalOnly()).isFalse();
                    assertThat(request.rawPathAndQuery()).startsWith("/v1/catalogs/catalog-a/namespaces/team_4");
                });
                assertThat(requests).extracting(RecordedRequest::rawPathAndQuery)
                        .contains("/v1/catalogs/catalog-a/namespaces/team_4/tables/events/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID);
                // It reads no other team's schema, even one that the catalog relays.
                assertThatThrownBy(() -> runner.execute(readSession, "SELECT count(*) FROM hoglake.team_7.events"))
                        .isInstanceOf(QueryFailedException.class);
                assertThat(requests).extracting(RecordedRequest::rawPathAndQuery).noneMatch(target -> target.contains("team_7"));

                // The first profile is unchanged on the same catalog.
                requests.clear();
                assertThat(runner.execute(shared, "SELECT count(*) FROM hoglake.team_7.events").getOnlyValue()).isEqualTo(0L);
                assertThat(requests).isNotEmpty().allSatisfy(request -> assertThat(request.authorization()).isEqualTo("Bearer " + storageSessionJwt));
            }

            // Without the two properties the catalog refuses the read session, as before.
            requests.clear();
            try (StandaloneQueryRunner runner = sharedRunner(withoutReadSessions)) {
                assertThatThrownBy(() -> runner.execute(readSession, "SELECT count(*) FROM hoglake.team_4.events"))
                        .isInstanceOf(QueryFailedException.class)
                        .hasMessageContaining("Shared Hoglake session");
                assertThat(requests).isEmpty();
            }

            // One property without the other, or either without remote read, does not start.
            for (Map<String, String> invalid : List.of(
                    ImmutableMap.<String, String>builder().putAll(withoutReadSessions).put("hoglake.shared.read-session-profile", DEFAULT_PROFILE).buildOrThrow(),
                    ImmutableMap.<String, String>builder().putAll(withoutReadSessions).put("hoglake.shared.read-session-remote-targets", EVENTS_TARGET + "," + IMPORTS_TARGET).buildOrThrow(),
                    ImmutableMap.<String, String>builder().putAll(withoutReadSessions).put("hoglake.shared.read-session-profile", DEFAULT_PROFILE).put("hoglake.shared.read-session-remote-targets", EVENTS_TARGET).buildOrThrow())) {
                assertThatThrownBy(() -> sharedRunner(invalid).close()).as(invalid.toString()).isInstanceOf(RuntimeException.class);
            }
        }
    }

    private static StandaloneQueryRunner sharedRunner(Map<String, String> properties)
    {
        StandaloneQueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("team_4").build());
        try {
            runner.installPlugin(new HoglakePlugin());
            runner.createCatalog("hoglake", "hoglake", properties);
            return runner;
        }
        catch (RuntimeException | Error exception) {
            runner.close();
            throw exception;
        }
    }

    /**
     * A relayed team schema with one table. A null {@code dataPath} is an empty table. The scan
     * answers any query, so a test can assert the query the reader sent.
     */
    private static Response routeRemoteTable(
            RecordedRequest request,
            String catalog,
            String namespace,
            String table,
            String tableUuid,
            long readSnapshotId,
            String dataPath,
            long fileSizeBytes,
            long metadataRecordCount,
            String deletePath)
    {
        String prefix = "/v1/catalogs/" + catalog + "/namespaces/" + namespace;
        if (request.isLocalOnly()) {
            return new Response(404, "{}");
        }
        if (request.rawPathAndQuery().equals(prefix)) {
            return new Response(200, "{\"name\":\"%s\",\"remote\":true}".formatted(namespace));
        }
        if (request.rawPathAndQuery().equals(prefix + "/tables")) {
            return new Response(200, "[{\"name\":\"%s\",\"table_uuid\":\"%s\"}]".formatted(table, tableUuid));
        }
        if (request.rawPath().equals(prefix + "/tables/" + table + "/scan") && dataPath == null) {
            return new Response(200, "[]");
        }
        if (request.rawPath().equals(prefix + "/tables/" + table)) {
            // The table answers any query: the first profile's reader sends totals=false or a snapshot.
            request = new RecordedRequest(request.method(), request.rawPath(), request.rawPath(), request.authorization(), request.securityToken(), request.range(), false, request.wirePathAndQuery());
        }
        Response local = routeLocalTable(request, catalog, namespace, table, tableUuid, readSnapshotId, dataPath, fileSizeBytes, metadataRecordCount, deletePath);
        if (local.status() == 200 && request.rawPath().equals(prefix + "/tables/" + table)) {
            return new Response(200, new String(local.body(), UTF_8).replace("\"remote\": false", "\"remote\": true"));
        }
        return local;
    }

    @Test
    void testEndToEndRealParquetReadAndCountThroughCoordinatorAndWorker()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("dedicated-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L, 303L), List.of("alpha", "beta", "gamma"));
        String dataPath = alice.tableFile("metrics/part-0.parquet");

        CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                Map.of(alice.credentials(), Map.of(dataPath, parquetBytes)));

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeLocalTable(
                request,
                alice.catalog(),
                "reports",
                "metrics",
                ALICE_TABLE_UUID,
                5L,
                dataPath,
                parquetBytes.length,
                999L,
                null));
                HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                        server.baseUri(),
                        Duration.ofSeconds(10),
                        verifier(jwksFile),
                        fileSystems);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "reports");

            MaterializedResult rows = runner.execute(session, "SELECT id, label FROM hoglake.reports.metrics ORDER BY id");
            assertThat(rows.getMaterializedRows())
                    .extracting(row -> List.of(row.getField(0), row.getField(1)))
                    .containsExactly(
                            List.of(101L, "alpha"),
                            List.of(202L, "beta"),
                            List.of(303L, "gamma"));

            int readsBeforeCount = fileSystems.reads().size();
            assertThat(readsBeforeCount).isGreaterThan(0);

            // The fake catalog reports 999 rows, so SELECT count(*) reads the 3-row Parquet file.
            MaterializedResult count = runner.execute(session, "SELECT count(*) FROM hoglake.reports.metrics");
            assertThat(count.getOnlyValue()).isEqualTo(3L);
            assertThat(fileSystems.reads().size()).isGreaterThan(readsBeforeCount);

            // Verify schemaExists uses the point namespace lookup route without listing.
            ConnectorTransactionHandle transaction = new HoglakeTransactionHandle(UUID.randomUUID());
            ConnectorSession connectorSession = session.toConnectorSession(CATALOG_HANDLE);
            try {
                ConnectorMetadata metadata = readPath.metadata(connectorSession, transaction);
                assertThat(metadata.schemaExists(connectorSession, "reports")).isTrue();
                assertThat(metadata.schemaExists(connectorSession, "missing")).isFalse();
            }
            finally {
                readPath.end(transaction);
            }

            // Confirm that every read HTTP request forwards Alice's exact JWT on allowed GET routes in local-only mode.
            assertThat(requests).isNotEmpty();
            assertThat(requests).allSatisfy(request -> {
                assertThat(request.method()).isEqualTo("GET");
                assertThat(request.authorization()).isEqualTo("Bearer " + alice.jwt());
                assertThat(request.isLocalOnly()).isTrue();
                assertThat(request.rawPathAndQuery()).isIn(
                        "/v1/catalogs/catalog-a/namespaces/reports",
                        "/v1/catalogs/catalog-a/namespaces/missing",
                        "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics",
                        "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID);
            });
            assertThat(requests).extracting(RecordedRequest::rawPathAndQuery)
                    .contains(
                            "/v1/catalogs/catalog-a/namespaces/reports",
                            "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics",
                            "/v1/catalogs/catalog-a/namespaces/reports/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID);

            // Refuse listing and write queries without sending listing or write HTTP requests.
            for (String disallowedSql : List.of(
                    "SHOW SCHEMAS FROM hoglake",
                    "SHOW TABLES FROM hoglake.reports",
                    "CREATE SCHEMA hoglake.new_schema",
                    "CREATE TABLE hoglake.reports.new_table (id bigint)",
                    "INSERT INTO hoglake.reports.metrics VALUES (404, 'delta')",
                    "DELETE FROM hoglake.reports.metrics")) {
                assertThatThrownBy(() -> runner.execute(session, disallowedSql))
                        .isInstanceOf(QueryFailedException.class);
            }
        }

        // Each worker read uses Alice's exact JWT AWS credentials and authorized S3 path.
        assertThat(fileSystems.reads()).isNotEmpty();
        assertThat(fileSystems.reads()).allSatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isEqualTo(dataPath);
        });

        // Every request uses GET and avoids catalog listing routes.
        assertThat(requests).allSatisfy(request -> {
            assertThat(request.method()).isEqualTo("GET");
            assertThat(request.authorization()).isEqualTo("Bearer " + alice.jwt());
            assertThat(request.rawPath()).doesNotEndWith("/namespaces");
            assertThat(request.rawPath()).doesNotEndWith("/tables");
        });
    }

    @Test
    void testTwoDistinctCallersGetIsolatedExactCredentialsAndData()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("multi-caller-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        CallerFixture bob = caller(key, "catalog-b", "tenant_2", "f4", "example.bob", "example.mapped_bob", 9, "ACCESSB", "SECRETB", "TOKENB");

        byte[] aliceParquet = writeMetricsParquet(List.of(101L, 202L), List.of("alpha", "beta"));
        byte[] bobParquet = writeMetricsParquet(List.of(901L), List.of("omega"));
        String alicePath = alice.tableFile("metrics/part-0.parquet");
        String bobPath = bob.tableFile("metrics/part-0.parquet");

        CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(Map.of(
                alice.credentials(), Map.of(alicePath, aliceParquet),
                bob.credentials(), Map.of(bobPath, bobParquet)));

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> {
            if (request.rawPath().startsWith("/v1/catalogs/catalog-a/")) {
                return routeLocalTable(request, alice.catalog(), "reports", "metrics", ALICE_TABLE_UUID, 5L, alicePath, aliceParquet.length, 500L, null);
            }
            if (request.rawPath().startsWith("/v1/catalogs/catalog-b/")) {
                return routeLocalTable(request, bob.catalog(), "reports", "metrics", BOB_TABLE_UUID, 9L, bobPath, bobParquet.length, 700L, null);
            }
            return new Response(404, "{}");
        });
                HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                        server.baseUri(),
                        Duration.ofSeconds(10),
                        verifier(jwksFile),
                        fileSystems);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session aliceSession = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "reports");
            Session bobSession = querySession(bob.mappedUser(), bob.principal(), bob.jwt(), "reports");

            try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
                Callable<MaterializedResult> aliceQuery = () -> runner.execute(aliceSession, "SELECT id, label FROM hoglake.reports.metrics ORDER BY id");
                Callable<MaterializedResult> bobQuery = () -> runner.execute(bobSession, "SELECT id, label FROM hoglake.reports.metrics ORDER BY id");
                List<Future<MaterializedResult>> futures = executor.invokeAll(List.of(aliceQuery, bobQuery));

                assertThat(futures.get(0).get().getMaterializedRows())
                        .extracting(row -> List.of(row.getField(0), row.getField(1)))
                        .containsExactly(List.of(101L, "alpha"), List.of(202L, "beta"));
                assertThat(futures.get(1).get().getMaterializedRows())
                        .extracting(row -> List.of(row.getField(0), row.getField(1)))
                        .containsExactly(List.of(901L, "omega"));
            }
        }

        // Each caller's worker opens only its own file with its own exact AWS session credentials.
        assertThat(fileSystems.reads()).anySatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isEqualTo(alicePath);
        });
        assertThat(fileSystems.reads()).anySatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(bob.credentials());
            assertThat(read.location()).isEqualTo(bobPath);
        });
        assertThat(fileSystems.reads()).allSatisfy(read -> {
            if (read.credentials().equals(alice.credentials())) {
                assertThat(read.location()).isEqualTo(alicePath);
            }
            else {
                assertThat(read.credentials()).isEqualTo(bob.credentials());
                assertThat(read.location()).isEqualTo(bobPath);
            }
        });

        // Each catalog request carries the matching caller's exact original JWT.
        assertThat(requests).allSatisfy(request -> {
            if (request.rawPath().startsWith("/v1/catalogs/catalog-a/")) {
                assertThat(request.authorization()).isEqualTo("Bearer " + alice.jwt());
            }
            else {
                assertThat(request.rawPath()).startsWith("/v1/catalogs/catalog-b/");
                assertThat(request.authorization()).isEqualTo("Bearer " + bob.jwt());
            }
        });
    }

    @Test
    void testRefusalsAreRedactedPermissionDenied()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("refusals-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        CallerFixture expiredAws = callerWithAwsExpiry(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESS_EXP", "SECRET_EXP", "TOKEN_EXP", NOW.getEpochSecond());

        String validDataPath = alice.tableFile("metrics/part-0.parquet");
        String outOfGrantDataPath = "s3://example-other/catalog-b/tables/metrics/part-0.parquet";
        String remoteTargetPath = "s3://example-remote/events/team_id=4/part-0.parquet";
        String outOfGrantDeletePath = "s3://example-other/catalog-b/tables/metrics/part-0.dv";
        AtomicInteger fileSystemCalls = new AtomicInteger();

        TrinoFileSystemFactory countingFailFactory = _ -> {
            fileSystemCalls.incrementAndGet();
            throw new AssertionError("Unauthorized call must not open storage");
        };

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeFailureFixture(
                request,
                validDataPath,
                outOfGrantDataPath,
                remoteTargetPath,
                outOfGrantDeletePath));
                HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                        server.baseUri(),
                        Duration.ofSeconds(10),
                        verifier(jwksFile),
                        countingFailFactory)) {
            ConnectorSession aliceSession = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "reports").toConnectorSession(CATALOG_HANDLE);
            ConnectorSession missingJwtSession = querySession(alice.mappedUser(), alice.principal(), null, "reports").toConnectorSession(CATALOG_HANDLE);
            ConnectorSession wrongPrincipalSession = querySession(alice.mappedUser(), "example.bob", alice.jwt(), "reports").toConnectorSession(CATALOG_HANDLE);
            ConnectorSession expiredAwsSession = querySession(expiredAws.mappedUser(), expiredAws.principal(), expiredAws.jwt(), "reports").toConnectorSession(CATALOG_HANDLE);

            HoglakeTableHandle validHandle = localHandle("reports", "metrics", 5L, ALICE_TABLE_UUID);
            HoglakeSplit validSplit = localSplit(validDataPath, Optional.empty());

            // Missing JWT, wrong caller principal, and expired aws.expires_at (zero skew) fail at coordinator and worker.
            for (ConnectorSession badSession : List.of(missingJwtSession, wrongPrincipalSession, expiredAwsSession)) {
                ConnectorTransactionHandle tx = new HoglakeTransactionHandle(UUID.randomUUID());
                assertRedactedDenial(
                        () -> readPath.metadata(badSession, tx),
                        AUTHORITY_DENIED,
                        alice.jwt(),
                        expiredAws.jwt(),
                        alice.secretKey(),
                        expiredAws.secretKey(),
                        alice.sessionToken(),
                        expiredAws.sessionToken());
                assertRedactedDenial(
                        () -> readPath.splitManager().getSplits(tx, badSession, validHandle, Set.of(), Constraint.alwaysTrue()),
                        AUTHORITY_DENIED,
                        alice.jwt(),
                        expiredAws.jwt(),
                        alice.secretKey(),
                        expiredAws.secretKey());
                assertRedactedDenial(
                        () -> readPath.pageSourceProvider().createPageSource(
                                tx,
                                badSession,
                                validSplit,
                                validHandle,
                                Optional.empty(),
                                List.of(),
                                DynamicFilter.EMPTY,
                                MemoryContext.NO_LIMIT),
                        AUTHORITY_DENIED,
                        alice.jwt(),
                        expiredAws.jwt(),
                        alice.secretKey(),
                        expiredAws.secretKey());
                readPath.end(tx);
            }

            // Coordinator refuses remote tables, missing table_uuid, invalid table_uuid, missing snapshot, and negative snapshot.
            for (String badTable : List.of("remote_tbl", "missing_uuid", "uppercase_uuid", "missing_snapshot", "negative_snapshot")) {
                ConnectorTransactionHandle tx = new HoglakeTransactionHandle(UUID.randomUUID());
                try {
                    ConnectorMetadata metadata = readPath.metadata(aliceSession, tx);
                    assertRedactedDenial(
                            () -> metadata.getTableHandle(aliceSession, new SchemaTableName("reports", badTable), Optional.empty(), Optional.empty()),
                            LOCAL_TABLE_DENIED,
                            alice.jwt(),
                            alice.secretKey(),
                            badTable);
                }
                finally {
                    readPath.end(tx);
                }
            }

            // Coordinator refuses mismatched pinned snapshot or table_uuid during getSplits.
            ConnectorTransactionHandle pinTx = new HoglakeTransactionHandle(UUID.randomUUID());
            try {
                ConnectorMetadata metadata = readPath.metadata(aliceSession, pinTx);
                ConnectorTableHandle pinned = metadata.getTableHandle(aliceSession, new SchemaTableName("reports", "valid_tbl"), Optional.empty(), Optional.empty());
                assertThat(pinned).isInstanceOf(HoglakeTableHandle.class);

                HoglakeTableHandle wrongSnapshot = localHandle("reports", "valid_tbl", 6L, ALICE_TABLE_UUID);
                HoglakeTableHandle wrongUuid = localHandle("reports", "valid_tbl", 5L, BOB_TABLE_UUID);
                HoglakeTableHandle remoteHandle = new HoglakeTableHandle(
                        "reports",
                        "valid_tbl",
                        5L,
                        ALICE_TABLE_UUID,
                        List.of(new HoglakeColumnHandle("id", 1, BIGINT, false)),
                        TupleDomain.all(),
                        List.of(),
                        List.of(),
                        false,
                        OptionalLong.empty(),
                        true);

                for (ConnectorTableHandle badHandle : List.of(wrongSnapshot, wrongUuid, remoteHandle)) {
                    assertRedactedDenial(
                            () -> readPath.splitManager().getSplits(pinTx, aliceSession, badHandle, Set.of(), Constraint.alwaysTrue()),
                            LOCAL_TABLE_DENIED,
                            ALICE_TABLE_UUID,
                            BOB_TABLE_UUID,
                            alice.jwt());
                }
            }
            finally {
                readPath.end(pinTx);
            }

            // Coordinator refuses out-of-grant data path, remote-target data path, and out-of-grant delete path BEFORE emitting splits.
            for (String badScanTable : List.of("out_of_grant_data", "remote_target_data", "out_of_grant_delete")) {
                ConnectorTransactionHandle tx = new HoglakeTransactionHandle(UUID.randomUUID());
                try {
                    ConnectorMetadata metadata = readPath.metadata(aliceSession, tx);
                    ConnectorTableHandle handle = metadata.getTableHandle(aliceSession, new SchemaTableName("reports", badScanTable), Optional.empty(), Optional.empty());
                    assertRedactedDenial(
                            () -> readPath.splitManager().getSplits(tx, aliceSession, handle, Set.of(), Constraint.alwaysTrue()),
                            TARGET_DENIED,
                            outOfGrantDataPath,
                            remoteTargetPath,
                            outOfGrantDeletePath,
                            "example-other",
                            "example-remote",
                            alice.jwt(),
                            alice.secretKey(),
                            alice.sessionToken());
                }
                finally {
                    readPath.end(tx);
                }
            }

            // Worker refuses out-of-grant data path, remote-target data path, and out-of-grant delete path BEFORE opening any file.
            ConnectorTransactionHandle workerTx = new HoglakeTransactionHandle(UUID.randomUUID());
            for (HoglakeSplit unauthorizedSplit : List.of(
                    localSplit(outOfGrantDataPath, Optional.empty()),
                    localSplit(remoteTargetPath, Optional.empty()),
                    localSplit(validDataPath, Optional.of(outOfGrantDeletePath)))) {
                assertRedactedDenial(
                        () -> readPath.pageSourceProvider().createPageSource(
                                workerTx,
                                aliceSession,
                                unauthorizedSplit,
                                validHandle,
                                Optional.empty(),
                                List.of(),
                                DynamicFilter.EMPTY,
                                MemoryContext.NO_LIMIT),
                        TARGET_DENIED,
                        outOfGrantDataPath,
                        remoteTargetPath,
                        outOfGrantDeletePath,
                        "example-other",
                        "example-remote",
                        alice.jwt(),
                        alice.secretKey(),
                        alice.sessionToken());
            }
        }

        assertThat(fileSystemCalls.get()).isZero();
    }

    @Test
    void testWorkerRealS3AccessDeniedIsRedacted()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("s3-denied-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        String dataPath = alice.tableFile("metrics/part-0.parquet");
        List<RecordedRequest> s3Requests = new CopyOnWriteArrayList<>();

        // Real S3 connector path against a local HTTP server returning S3 403 AccessDenied.
        try (DedicatedTestServer s3Server = DedicatedTestServer.start(s3Requests, _ -> new Response(
                403,
                "application/xml",
                """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error>
                  <Code>AccessDenied</Code>
                  <Message>Access Denied for s3://example-customer/catalog-a/tables/metrics/part-0.parquet</Message>
                </Error>
                """));
                DedicatedTestServer metadataServer = DedicatedTestServer.start(new CopyOnWriteArrayList<>(), _ -> new Response(404, "{}"))) {
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("hoglake.uri", metadataServer.baseUri().toASCIIString())
                    .put("hoglake.dedicated.local-read", "true")
                    .put("hoglake.dedicated.jwks-file", jwksFile.toString())
                    .put("hoglake.dedicated.issuer", ISSUER)
                    .put("hoglake.dedicated.remote-targets", EVENTS_TARGET + "," + IMPORTS_TARGET)
                    .put("hoglake.dedicated.hoglite-backends", BACKEND_ID)
                    .put("s3.region", REGION)
                    .put("s3.endpoint", s3Server.baseUri().toASCIIString())
                    .put("s3.path-style-access", "true")
                    .buildOrThrow();

            // Use a live token valid against Clock.systemUTC() because HoglakeConnectorFactory uses system UTC.
            CallerFixture liveAlice = callerAt(key, Instant.now(), "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSLIVE", "SECRETLIVE", "TOKENLIVE");
            ConnectorSession liveSession = querySession(liveAlice.mappedUser(), liveAlice.principal(), liveAlice.jwt(), "reports")
                    .toConnectorSession(CATALOG_HANDLE);

            Connector connector = new HoglakeConnectorFactory().create("hoglake", properties, new TestingConnectorContext());
            try {
                ConnectorTransactionHandle tx = connector.beginTransaction(READ_COMMITTED, true, true);
                HoglakeTableHandle handle = localHandle("reports", "metrics", 5L, ALICE_TABLE_UUID);
                HoglakeSplit split = localSplit(dataPath, Optional.empty());
                assertRedactedDenial(
                        () -> {
                            try (ConnectorPageSource pageSource = connector.getPageSourceProvider().createPageSource(
                                    tx,
                                    liveSession,
                                    split,
                                    handle,
                                    Optional.empty(),
                                    List.of(new HoglakeColumnHandle("id", 1, BIGINT, false)),
                                    DynamicFilter.EMPTY,
                                    MemoryContext.NO_LIMIT)) {
                                while (!pageSource.isFinished()) {
                                    pageSource.getNextSourcePage();
                                }
                            }
                            catch (IOException failure) {
                                throw new RuntimeException(failure);
                            }
                        },
                        TARGET_DENIED,
                        "example-customer",
                        dataPath,
                        liveAlice.jwt(),
                        liveAlice.accessKey(),
                        liveAlice.secretKey(),
                        liveAlice.sessionToken());
            }
            finally {
                connector.shutdown();
            }

            // Confirm that the AWS S3 client signs each request with the caller's JWT AWS credentials.
            assertThat(s3Requests).isNotEmpty();
            assertThat(s3Requests).allSatisfy(request -> {
                assertThat(request.authorization()).contains("Credential=ACCESSLIVE/");
                assertThat(request.securityToken()).isEqualTo("TOKENLIVE");
            });
        }
    }

    @Test
    void testWorkerRealS3DeletionVectorHeadForbiddenIsRedacted()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("s3-dv-head-denied-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture liveAlice = callerAt(key, Instant.now(), "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSLIVE", "SECRETLIVE", "TOKENLIVE");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L, 303L), List.of("alpha", "beta", "gamma"));
        String dataPath = liveAlice.tableFile("metrics/part-0.parquet");
        String deletePath = liveAlice.tableFile("metrics/part-0.puffin");
        String dataRequestPath = "/example-customer/catalog-a/tables/metrics/part-0.parquet";
        String deleteRequestPath = "/example-customer/catalog-a/tables/metrics/part-0.puffin";
        List<RecordedRequest> s3Requests = new CopyOnWriteArrayList<>();

        // Serve the Parquet file normally and deny the deletion vector HEAD with a bodyless HTTP 403.
        try (DedicatedTestServer s3Server = DedicatedTestServer.start(s3Requests, request -> {
            if (request.rawPath().equals(dataRequestPath)) {
                return serveS3Object(request, parquetBytes);
            }
            if (request.rawPath().equals(deleteRequestPath)) {
                return Response.bodyless(403);
            }
            return Response.bodyless(404);
        });
                DedicatedTestServer metadataServer = DedicatedTestServer.start(new CopyOnWriteArrayList<>(), _ -> new Response(404, "{}"))) {
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("hoglake.uri", metadataServer.baseUri().toASCIIString())
                    .put("hoglake.dedicated.local-read", "true")
                    .put("hoglake.dedicated.jwks-file", jwksFile.toString())
                    .put("hoglake.dedicated.issuer", ISSUER)
                    .put("hoglake.dedicated.remote-targets", EVENTS_TARGET + "," + IMPORTS_TARGET)
                    .put("hoglake.dedicated.hoglite-backends", BACKEND_ID)
                    .put("s3.region", REGION)
                    .put("s3.endpoint", s3Server.baseUri().toASCIIString())
                    .put("s3.path-style-access", "true")
                    .buildOrThrow();

            ConnectorSession liveSession = querySession(liveAlice.mappedUser(), liveAlice.principal(), liveAlice.jwt(), "reports")
                    .toConnectorSession(CATALOG_HANDLE);

            Connector connector = new HoglakeConnectorFactory().create("hoglake", properties, new TestingConnectorContext());
            try {
                ConnectorTransactionHandle tx = connector.beginTransaction(READ_COMMITTED, true, true);
                HoglakeTableHandle handle = localHandle("reports", "metrics", 5L, ALICE_TABLE_UUID);
                HoglakeSplit split = localSplit(dataPath, parquetBytes.length, Optional.of(deletePath));
                assertRedactedDenial(
                        () -> {
                            try (ConnectorPageSource pageSource = connector.getPageSourceProvider().createPageSource(
                                    tx,
                                    liveSession,
                                    split,
                                    handle,
                                    Optional.empty(),
                                    List.of(new HoglakeColumnHandle("id", 1, BIGINT, false)),
                                    DynamicFilter.EMPTY,
                                    MemoryContext.NO_LIMIT)) {
                                while (!pageSource.isFinished()) {
                                    pageSource.getNextSourcePage();
                                }
                            }
                            catch (IOException failure) {
                                throw new RuntimeException(failure);
                            }
                        },
                        TARGET_DENIED,
                        "example-customer",
                        dataPath,
                        deletePath,
                        liveAlice.jwt(),
                        liveAlice.accessKey(),
                        liveAlice.secretKey(),
                        liveAlice.sessionToken());
            }
            finally {
                connector.shutdown();
            }

            assertThat(s3Requests).anySatisfy(request -> {
                assertThat(request.method()).isEqualTo("HEAD");
                assertThat(request.rawPath()).isEqualTo(deleteRequestPath);
                assertThat(request.authorization()).contains("Credential=ACCESSLIVE/");
                assertThat(request.securityToken()).isEqualTo("TOKENLIVE");
            });
        }
    }

    @Test
    void testEndToEndRealParquetReadWithInGrantDeletionVector()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("dv-read-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L, 303L), List.of("alpha", "beta", "gamma"));
        String dataPath = alice.tableFile("metrics/part-0.parquet");
        String deletePath = alice.tableFile("metrics/part-0.puffin");
        byte[] deletionVectorBytes = PuffinDeletionVectorFixtures.deletionVector(dataPath, 1L);

        CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                Map.of(alice.credentials(), Map.of(
                        dataPath, parquetBytes,
                        deletePath, deletionVectorBytes)));

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeLocalTable(
                request,
                alice.catalog(),
                "reports",
                "metrics",
                ALICE_TABLE_UUID,
                5L,
                dataPath,
                parquetBytes.length,
                999L,
                deletePath));
                HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                        server.baseUri(),
                        Duration.ofSeconds(10),
                        verifier(jwksFile),
                        fileSystems);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "reports");

            MaterializedResult rows = runner.execute(session, "SELECT id, label FROM hoglake.reports.metrics ORDER BY id");
            assertThat(rows.getMaterializedRows())
                    .extracting(row -> List.of(row.getField(0), row.getField(1)))
                    .containsExactly(
                            List.of(101L, "alpha"),
                            List.of(303L, "gamma"));

            MaterializedResult count = runner.execute(session, "SELECT count(*) FROM hoglake.reports.metrics");
            assertThat(count.getOnlyValue()).isEqualTo(2L);
        }

        // Assert Alice's exact JWT AWS credentials opened both the Parquet file and the deletion vector.
        assertThat(fileSystems.reads()).anySatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isEqualTo(dataPath);
        });
        assertThat(fileSystems.reads()).anySatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isEqualTo(deletePath);
        });
        assertThat(fileSystems.reads()).allSatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isIn(dataPath, deletePath);
        });
    }

    @Test
    void testRepeatedTableLookupAndSelfJoinReusePinnedHandle()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("self-join-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L, 303L), List.of("alpha", "beta", "gamma"));
        String dataPath = alice.tableFile("metrics/part-0.parquet");
        String tableRoute = "/v1/catalogs/" + alice.catalog() + "/namespaces/reports/tables/metrics";
        String pinnedScanRoute = tableRoute + "/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID;

        for (boolean dropAfterFirstLookup : List.of(false, true)) {
            CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                    Map.of(alice.credentials(), Map.of(dataPath, parquetBytes)));
            AtomicInteger tableLookups = new AtomicInteger();
            List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

            try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> {
                if (request.rawPathAndQuery().equals(tableRoute)) {
                    int lookup = tableLookups.incrementAndGet();
                    if (lookup > 1) {
                        if (dropAfterFirstLookup) {
                            return new Response(404, "{}");
                        }
                        return routeLocalTable(request, alice.catalog(), "reports", "metrics", BOB_TABLE_UUID, 6L, dataPath, parquetBytes.length, 3L, null);
                    }
                }
                return routeLocalTable(request, alice.catalog(), "reports", "metrics", ALICE_TABLE_UUID, 5L, dataPath, parquetBytes.length, 3L, null);
            });
                    HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                            server.baseUri(),
                            Duration.ofSeconds(10),
                            verifier(jwksFile),
                            fileSystems);
                    StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
                Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "reports");
                ConnectorSession connectorSession = session.toConnectorSession(CATALOG_HANDLE);

                // Direct repeated getTableHandle in one transaction reuses the pinned handle without another HTTP call.
                ConnectorTransactionHandle tx = new HoglakeTransactionHandle(UUID.randomUUID());
                try {
                    ConnectorMetadata metadata = readPath.metadata(connectorSession, tx);
                    SchemaTableName metrics = new SchemaTableName("reports", "metrics");
                    ConnectorTableHandle first = metadata.getTableHandle(connectorSession, metrics, Optional.empty(), Optional.empty());
                    ConnectorTableHandle second = metadata.getTableHandle(connectorSession, metrics, Optional.empty(), Optional.empty());
                    assertThat(second).isEqualTo(first);
                    assertThat(((HoglakeTableHandle) second).snapshotId()).isEqualTo(5L);
                    assertThat(((HoglakeTableHandle) second).tableUuid()).isEqualTo(ALICE_TABLE_UUID);
                    assertThat(tableLookups.get()).isEqualTo(1);
                }
                finally {
                    readPath.end(tx);
                }

                // End-to-end self-join succeeds in one transaction even when the table advances or disappears after lookup 1.
                tableLookups.set(0);
                requests.clear();
                MaterializedResult joined = runner.execute(
                        session,
                        "SELECT a.id, b.label FROM hoglake.reports.metrics a JOIN hoglake.reports.metrics b ON a.id = b.id ORDER BY a.id");
                assertThat(joined.getMaterializedRows())
                        .extracting(row -> List.of(row.getField(0), row.getField(1)))
                        .containsExactly(
                                List.of(101L, "alpha"),
                                List.of(202L, "beta"),
                                List.of(303L, "gamma"));
                assertThat(tableLookups.get()).isEqualTo(1);
                assertThat(requests).filteredOn(request -> request.rawPathAndQuery().equals(tableRoute)).hasSize(1);
                assertThat(requests).filteredOn(request -> request.rawPath().endsWith("/scan"))
                        .isNotEmpty()
                        .allSatisfy(request -> assertThat(request.rawPathAndQuery()).isEqualTo(pinnedScanRoute));
            }
        }
    }

    @Test
    void testLocalNamespaceWinsTeamRelayCollisionForMetadataAndPinnedRead()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("collision-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        byte[] parquetBytes = writeMetricsParquet(List.of(101L, 202L), List.of("alpha", "beta"));
        String dataPath = alice.tableFile("metrics/part-0.parquet");
        CredentialCheckingFileSystemFactory fileSystems = new CredentialCheckingFileSystemFactory(
                Map.of(alice.credentials(), Map.of(dataPath, parquetBytes)));

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        List<RecordedRequest> relayed = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeCustomerHoglake(
                request,
                relayed,
                alice.catalog(),
                dataPath,
                parquetBytes.length));
                HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                        server.baseUri(),
                        Duration.ofSeconds(10),
                        verifier(jwksFile),
                        fileSystems);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");

            // The local namespace with the relayed team name answers the metadata lookup and the pinned scan.
            MaterializedResult rows = runner.execute(session, "SELECT id, label FROM hoglake.team_4.metrics ORDER BY id");
            assertThat(rows.getMaterializedRows())
                    .extracting(row -> List.of(row.getField(0), row.getField(1)))
                    .containsExactly(List.of(101L, "alpha"), List.of(202L, "beta"));

            ConnectorTransactionHandle transaction = new HoglakeTransactionHandle(UUID.randomUUID());
            ConnectorSession connectorSession = session.toConnectorSession(CATALOG_HANDLE);
            try {
                assertThat(readPath.metadata(connectorSession, transaction).schemaExists(connectorSession, "team_4")).isTrue();
            }
            finally {
                readPath.end(transaction);
            }

            assertThat(relayed).isEmpty();
            assertThat(requests).extracting(RecordedRequest::rawPathAndQuery)
                    .contains(
                            "/v1/catalogs/catalog-a/namespaces/team_4/tables/metrics",
                            "/v1/catalogs/catalog-a/namespaces/team_4/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID);
            assertThat(requests).allSatisfy(request -> assertThat(request.isLocalOnly()).isTrue());
            assertThat(requests).extracting(RecordedRequest::wirePathAndQuery)
                    .contains(
                            "/v1/local-only/catalogs/catalog-a/namespaces/team_4/tables/metrics",
                            "/v1/local-only/catalogs/catalog-a/namespaces/team_4/tables/metrics/scan?snapshot=5&table_uuid=" + ALICE_TABLE_UUID);

            // The same service still relays the name on the ordinary route, as the shared catalog sends it.
            try (HttpClient http = HttpClient.newHttpClient();
                    HoglakeClient shared = new HoglakeClient(
                            server.baseUri().toASCIIString(),
                            alice.catalog(),
                            Duration.ofSeconds(10),
                            new HoglakeTokenSource.Fixed(alice.jwt()),
                            http,
                            HoglakeClient.RouteGuard.ALLOW_ALL)) {
                assertThat(shared.findNamespace("team_4")).hasValueSatisfying(namespace -> assertThat(namespace.isRemote()).isTrue());
            }
            assertThat(relayed).hasSize(1);
            assertThat(relayed.getFirst().isLocalOnly()).isFalse();
            assertThat(relayed.getFirst().wirePathAndQuery()).isEqualTo("/v1/catalogs/catalog-a/namespaces/team_4");
        }

        assertThat(fileSystems.reads()).isNotEmpty().allSatisfy(read -> {
            assertThat(read.credentials()).isEqualTo(alice.credentials());
            assertThat(read.location()).isEqualTo(dataPath);
        });
    }

    @Test
    void testAbsentLocalNamesAreOrdinaryAbsenceWithoutRelay()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("absence-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        String dataPath = alice.tableFile("metrics/part-0.parquet");
        TrinoFileSystemFactory noStorage = _ -> {
            throw new AssertionError("An absent table must not open storage");
        };

        List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        List<RecordedRequest> relayed = new CopyOnWriteArrayList<>();
        try (DedicatedTestServer server = DedicatedTestServer.start(requests, request -> routeCustomerHoglake(request, relayed, alice.catalog(), dataPath, 128L));
                HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                        server.baseUri(),
                        Duration.ofSeconds(10),
                        verifier(jwksFile),
                        noStorage);
                StandaloneQueryRunner runner = createRunner(readPath, verifier(jwksFile))) {
            Session session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "team_4");
            ConnectorSession connectorSession = session.toConnectorSession(CATALOG_HANDLE);

            ConnectorTransactionHandle transaction = new HoglakeTransactionHandle(UUID.randomUUID());
            try {
                ConnectorMetadata metadata = readPath.metadata(connectorSession, transaction);
                // A relayed team name with no local namespace, and an ordinary missing name.
                assertThat(metadata.schemaExists(connectorSession, "team_7")).isFalse();
                assertThat(metadata.schemaExists(connectorSession, "reports")).isFalse();
                assertThat(metadata.getTableHandle(connectorSession, new SchemaTableName("team_7", "events"), Optional.empty(), Optional.empty())).isNull();
                assertThat(metadata.getTableHandle(connectorSession, new SchemaTableName("team_4", "missing"), Optional.empty(), Optional.empty())).isNull();
            }
            finally {
                readPath.end(transaction);
            }

            assertThatThrownBy(() -> runner.execute(session, "SELECT * FROM hoglake.team_7.events"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining("does not exist");

            assertThat(requests).isNotEmpty().allSatisfy(request -> assertThat(request.isLocalOnly()).isTrue());
            assertThat(relayed).isEmpty();
        }
    }

    @Test
    void testUnconfirmedAnswersFailClosedDuringVersionSkew()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("skew-jwks.json");
        writeJwks(jwksFile, key, KID);

        CallerFixture alice = caller(key, "catalog-a", "tenant_1", "ad", "example.alice", "example.mapped_alice", 4, "ACCESSA", "SECRETA", "TOKENA");
        String dataPath = alice.tableFile("metrics/part-0.parquet");
        TrinoFileSystemFactory noStorage = _ -> {
            throw new AssertionError("An unconfirmed answer must not open storage");
        };

        // A Hoglake that predates the local-only routes, or a proxy in front of it: an unknown-path 404,
        // a 401, or a busy answer, never with the marker. None is absence, and nothing is retried or relayed.
        for (Response oldAnswer : List.of(new Response(404, "{}"), new Response(401, "{}"), new Response(503, "{}"))) {
            List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
            try (DedicatedTestServer server = DedicatedTestServer.start(
                    requests,
                    request -> routeLocalTable(request, alice.catalog(), "reports", "metrics", ALICE_TABLE_UUID, 5L, dataPath, 128L, 1L, null),
                    Optional.of(oldAnswer));
                    HoglakeDedicatedReadPath readPath = new HoglakeDedicatedReadPath(
                            server.baseUri(),
                            Duration.ofSeconds(10),
                            verifier(jwksFile),
                            noStorage)) {
                ConnectorSession session = querySession(alice.mappedUser(), alice.principal(), alice.jwt(), "reports").toConnectorSession(CATALOG_HANDLE);
                ConnectorTransactionHandle transaction = new HoglakeTransactionHandle(UUID.randomUUID());
                try {
                    ConnectorMetadata metadata = readPath.metadata(session, transaction);
                    List<Runnable> lookups = List.of(
                            () -> metadata.schemaExists(session, "reports"),
                            () -> metadata.schemaExists(session, "missing"),
                            () -> metadata.getTableHandle(session, new SchemaTableName("reports", "metrics"), Optional.empty(), Optional.empty()),
                            () -> metadata.getTableHandle(session, new SchemaTableName("reports", "missing"), Optional.empty(), Optional.empty()),
                            () -> readPath.splitManager().getSplits(transaction, session, localHandle("reports", "metrics", 5L, ALICE_TABLE_UUID), Set.of(), Constraint.alwaysTrue()));
                    for (Runnable lookup : lookups) {
                        assertThatThrownBy(lookup::run)
                                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                                    assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_INVALID_RESPONSE.toErrorCode());
                                    assertThat(failure.getMessage()).isEqualTo("Hoglake did not confirm the local-only read mode");
                                });
                    }
                }
                finally {
                    readPath.end(transaction);
                }
            }

            assertThat(requests).describedAs("old server answer %s", oldAnswer.status())
                    .hasSize(5)
                    .allSatisfy(request -> {
                        assertThat(request.isLocalOnly()).isTrue();
                        assertThat(request.wirePathAndQuery()).startsWith("/v1/local-only/catalogs/catalog-a/namespaces/");
                    });
        }
    }

    /**
     * Models one Customer Hoglake that serves both Trino catalogs. A configured remote schema relays every
     * canonical team namespace on the ordinary routes. On the local-only routes, only the local warehouse
     * answers: here, the local namespace {@code team_4} with the table {@code metrics}.
     */
    private static Response routeCustomerHoglake(RecordedRequest request, List<RecordedRequest> relayed, String catalog, String dataPath, long fileSizeBytes)
    {
        String teamPrefix = "/v1/catalogs/" + catalog + "/namespaces/team_";
        if (!request.isLocalOnly()) {
            if (!request.rawPath().startsWith(teamPrefix)) {
                return new Response(404, "{}");
            }
            relayed.add(request);
            String namespace = request.rawPath().substring(("/v1/catalogs/" + catalog + "/namespaces/").length()).split("/", -1)[0];
            return new Response(200,
                    """
                    {"name":"%s","remote":true}
                    """.formatted(namespace));
        }
        return routeLocalTable(request, catalog, "team_4", "metrics", ALICE_TABLE_UUID, 5L, dataPath, fileSizeBytes, 2L, null);
    }

    @Test
    void testConnectorFactoryDedicatedLocalReadIsDefaultOffAndIsolated()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwksFile = directory.resolve("factory-jwks.json");
        writeJwks(jwksFile, key, KID);

        Map<String, String> validDedicated = ImmutableMap.<String, String>builder()
                .put("hoglake.uri", "https://metadata.example.test")
                .put("hoglake.dedicated.local-read", "true")
                .put("hoglake.dedicated.jwks-file", jwksFile.toString())
                .put("hoglake.dedicated.issuer", ISSUER)
                .put("hoglake.dedicated.remote-targets", EVENTS_TARGET + "," + IMPORTS_TARGET)
                .put("hoglake.dedicated.hoglite-backends", BACKEND_ID)
                .put("s3.region", REGION)
                .buildOrThrow();

        // Valid dedicated configuration boots a HoglakeDedicatedConnector.
        Connector dedicated = new HoglakeConnectorFactory().create("hoglake", validDedicated, new TestingConnectorContext());
        try {
            assertThat(dedicated).isInstanceOf(HoglakeDedicatedConnector.class);
        }
        finally {
            dedicated.shutdown();
        }

        // Refuse static credentials, ambient IAM roles, caches, and conflicting connector modes.
        List<Map<String, String>> invalidConfigs = List.of(
                withProperty(validDedicated, "s3.aws-access-key", "STATIC_KEY"),
                withProperty(validDedicated, "s3.aws-secret-key", "STATIC_SECRET"),
                withProperty(validDedicated, "s3.iam-role", "arn:aws:iam::000000000000:role/example"),
                withProperty(validDedicated, "fs.cache.enabled", "true"),
                withProperty(validDedicated, "fs.native-s3.enabled", "true"),
                withProperty(validDedicated, "hoglake.multi-tenant", "true"),
                withProperty(validDedicated, "hoglake.dedicated.local-read", "maybe"),
                withoutProperty(validDedicated, "s3.region"));

        for (Map<String, String> invalid : invalidConfigs) {
            assertThatThrownBy(() -> new HoglakeConnectorFactory().create("hoglake", invalid, new TestingConnectorContext()))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    private static Map<String, String> withProperty(Map<String, String> base, String key, String value)
    {
        Map<String, String> copy = new LinkedHashMap<>(base);
        copy.put(key, value);
        return ImmutableMap.copyOf(copy);
    }

    private static Map<String, String> withoutProperty(Map<String, String> base, String key)
    {
        Map<String, String> copy = new LinkedHashMap<>(base);
        copy.remove(key);
        return ImmutableMap.copyOf(copy);
    }

    private static StandaloneQueryRunner createRunner(HoglakeDedicatedReadPath readPath, HoglakeReadSessionVerifier verifier)
    {
        Session baseSession = testSessionBuilder().setCatalog("hoglake").setSchema("reports").build();
        StandaloneQueryRunner runner = new StandaloneQueryRunner(baseSession);
        runner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return List.of(new ConnectorFactory()
                {
                    @Override
                    public String getName()
                    {
                        return "hoglake_dedicated_test";
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        return new HoglakeDedicatedConnector(readPath, verifier, () -> {});
                    }
                });
            }
        });
        runner.createCatalog("hoglake", "hoglake_dedicated_test", Map.of());
        return runner;
    }

    private static byte[] writeMetricsParquet(List<Long> ids, List<String> labels)
            throws IOException
    {
        List<Object> idValues = new ArrayList<>(ids);
        List<Object> labelValues = new ArrayList<>(labels);
        return ConnectorTestFixtures.writeParquet(
                List.of(
                        new FileColumn(Types.required(PrimitiveTypeName.INT64).id(1).named("id"), BIGINT, idValues),
                        new FileColumn(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(2).named("label"), VARCHAR, labelValues)),
                ParquetWriterOptions.builder().build());
    }

    private static Response routeLocalTable(
            RecordedRequest request,
            String catalog,
            String namespace,
            String table,
            String tableUuid,
            long readSnapshotId,
            String dataPath,
            long fileSizeBytes,
            long metadataRecordCount,
            String deletePath)
    {
        String prefix = "/v1/catalogs/" + catalog;
        if (request.rawPathAndQuery().equals(prefix + "/namespaces/" + namespace)) {
            return new Response(200,
                    """
                    {"name":"%s","remote":false}
                    """.formatted(namespace));
        }
        if (request.rawPathAndQuery().equals(prefix + "/namespaces/" + namespace + "/tables/" + table)) {
            return new Response(200,
                    """
                    {
                      "name": "%s",
                      "namespace": "%s",
                      "table_uuid": "%s",
                      "read_snapshot_id": %d,
                      "remote": false,
                      "columns": [
                        {"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false},
                        {"field_id": 2, "ordinal": 1, "name": "label", "type": "string", "nullable": true}
                      ],
                      "record_count": %d,
                      "file_count": 1,
                      "file_size_bytes": %d
                    }
                    """.formatted(table, namespace, tableUuid, readSnapshotId, metadataRecordCount, fileSizeBytes));
        }
        if (request.rawPathAndQuery().equals(prefix + "/namespaces/" + namespace + "/tables/" + table + "/scan?snapshot=" + readSnapshotId + "&table_uuid=" + tableUuid)) {
            String deleteJson = deletePath == null
                    ? ""
                    : """
                      , "delete_file": {
                        "delete_file_id": 11,
                        "data_file_id": 1,
                        "path": "%s",
                        "delete_count": 1,
                        "file_size_bytes": 16,
                        "file_format": "%s"
                      }
                      """.formatted(deletePath, HoglakeDeletionVector.PUFFIN_DELETION_VECTOR_FORMAT);
            return new Response(200,
                    """
                    [
                      {
                        "data_file": {
                          "data_file_id": 1,
                          "path": "%s",
                          "record_count": %d,
                          "file_size_bytes": %d,
                          "file_format": "parquet",
                          "begin_snapshot": %d
                        }%s
                      }
                    ]
                    """.formatted(dataPath, metadataRecordCount, fileSizeBytes, readSnapshotId, deleteJson));
        }
        return new Response(404, "{}");
    }

    private static Response routeFailureFixture(
            RecordedRequest request,
            String validDataPath,
            String outOfGrantDataPath,
            String remoteTargetPath,
            String outOfGrantDeletePath)
    {
        String path = request.rawPathAndQuery();
        if (path.endsWith("/tables/remote_tbl")) {
            return new Response(200,
                    """
                    {
                      "name": "remote_tbl",
                      "namespace": "reports",
                      "table_uuid": "%s",
                      "read_snapshot_id": 5,
                      "remote": true,
                      "columns": [{"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false}]
                    }
                    """.formatted(ALICE_TABLE_UUID));
        }
        if (path.endsWith("/tables/missing_uuid")) {
            return new Response(200,
                    """
                    {
                      "name": "missing_uuid",
                      "namespace": "reports",
                      "read_snapshot_id": 5,
                      "remote": false,
                      "columns": [{"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false}]
                    }
                    """);
        }
        if (path.endsWith("/tables/uppercase_uuid")) {
            return new Response(200,
                    """
                    {
                      "name": "uppercase_uuid",
                      "namespace": "reports",
                      "table_uuid": "%s",
                      "read_snapshot_id": 5,
                      "remote": false,
                      "columns": [{"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false}]
                    }
                    """.formatted(ALICE_TABLE_UUID.toUpperCase(ENGLISH)));
        }
        if (path.endsWith("/tables/missing_snapshot")) {
            return new Response(200,
                    """
                    {
                      "name": "missing_snapshot",
                      "namespace": "reports",
                      "table_uuid": "%s",
                      "remote": false,
                      "columns": [{"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false}]
                    }
                    """.formatted(ALICE_TABLE_UUID));
        }
        if (path.endsWith("/tables/negative_snapshot")) {
            return new Response(200,
                    """
                    {
                      "name": "negative_snapshot",
                      "namespace": "reports",
                      "table_uuid": "%s",
                      "read_snapshot_id": -1,
                      "remote": false,
                      "columns": [{"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false}]
                    }
                    """.formatted(ALICE_TABLE_UUID));
        }
        if (path.contains("/tables/valid_tbl")) {
            return routeLocalTable(request, "catalog-a", "reports", "valid_tbl", ALICE_TABLE_UUID, 5L, validDataPath, 128L, 1L, null);
        }
        if (path.contains("/tables/out_of_grant_data")) {
            return routeLocalTable(request, "catalog-a", "reports", "out_of_grant_data", ALICE_TABLE_UUID, 5L, outOfGrantDataPath, 128L, 1L, null);
        }
        if (path.contains("/tables/remote_target_data")) {
            return routeLocalTable(request, "catalog-a", "reports", "remote_target_data", ALICE_TABLE_UUID, 5L, remoteTargetPath, 128L, 1L, null);
        }
        if (path.contains("/tables/out_of_grant_delete")) {
            return routeLocalTable(request, "catalog-a", "reports", "out_of_grant_delete", ALICE_TABLE_UUID, 5L, validDataPath, 128L, 1L, outOfGrantDeletePath);
        }
        return new Response(404, "{}");
    }

    private static HoglakeTableHandle localHandle(String schema, String table, long snapshotId, String tableUuid)
    {
        return new HoglakeTableHandle(
                schema,
                table,
                snapshotId,
                tableUuid,
                List.of(new HoglakeColumnHandle("id", 1, BIGINT, false)),
                TupleDomain.all(),
                List.of(),
                List.of(),
                false,
                OptionalLong.empty(),
                false);
    }

    private static HoglakeSplit localSplit(String dataPath, Optional<String> deletePath)
    {
        return localSplit(dataPath, 128L, deletePath);
    }

    private static HoglakeSplit localSplit(String dataPath, long fileSizeBytes, Optional<String> deletePath)
    {
        return new HoglakeSplit(
                1L,
                dataPath,
                fileSizeBytes,
                -1L,
                deletePath,
                deletePath.isPresent() ? 1L : 0L,
                deletePath.map(_ -> HoglakeDeletionVector.PUFFIN_DELETION_VECTOR_FORMAT),
                0L,
                fileSizeBytes,
                SplitWeight.standard(),
                OptionalLong.empty(),
                Optional.empty());
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
        return callerAt(key, NOW, catalog, tenant, shard, principal, mappedUser, teamId, accessKey, secretKey, sessionTokenValue, NOW.getEpochSecond() + 59);
    }

    private static CallerFixture callerAt(
            KeyPair key,
            Instant now,
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
        return callerAt(key, now, catalog, tenant, shard, principal, mappedUser, teamId, accessKey, secretKey, sessionTokenValue, now.getEpochSecond() + 59);
    }

    private static CallerFixture callerWithAwsExpiry(
            KeyPair key,
            String catalog,
            String tenant,
            String shard,
            String principal,
            String mappedUser,
            long teamId,
            String accessKey,
            String secretKey,
            String sessionTokenValue,
            long awsExpiresAt)
    {
        return callerAt(key, NOW, catalog, tenant, shard, principal, mappedUser, teamId, accessKey, secretKey, sessionTokenValue, awsExpiresAt);
    }

    private static CallerFixture callerAt(
            KeyPair key,
            Instant now,
            String catalog,
            String tenant,
            String shard,
            String principal,
            String mappedUser,
            long teamId,
            String accessKey,
            String secretKey,
            String sessionTokenValue,
            long awsExpiresAt)
    {
        long iat = now.getEpochSecond() - 1;
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("iss", ISSUER);
        claims.put("aud", List.of("trino-session", "hoglake-session", "hoglite-session"));
        claims.put("sub", principal);
        claims.put("usr", principal);
        claims.put("jti", "token-" + catalog + "-" + awsExpiresAt);
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
        claims.put("exp", awsExpiresAt);
        claims.put("cat", Map.of(catalog, List.of("read")));
        claims.put("prd", Map.of("team_id", Map.of(Long.toString(teamId), "read")));
        claims.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", tenant));
        claims.put("remote_schemas", Map.of("team", List.of("read")));
        claims.put("aws", Map.of(
                "access_key_id", accessKey,
                "secret_access_key", secretKey,
                "session_token", sessionTokenValue,
                "region", REGION,
                "expires_at", awsExpiresAt));
        claims.put("grants", List.of(
                Map.of(
                        "catalog", catalog,
                        "target", catalog,
                        "bucket", "example-customer",
                        "prefix", catalog + "/tables/",
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
        return new CallerFixture(catalog, principal, mappedUser, accessKey, secretKey, sessionTokenValue, jwt);
    }

    private static Session querySession(String mappedUser, String principal, String jwt, String schema)
    {
        Identity.Builder builder = Identity.forUser(mappedUser)
                .withPrincipal(new BasicPrincipal(principal));
        if (jwt != null) {
            builder.withExtraCredentials(Map.of(SESSION_TOKEN, jwt));
        }
        Identity identity = builder.build();
        return testSessionBuilder()
                .setCatalog("hoglake")
                .setSchema(schema)
                .setIdentity(identity)
                .setOriginalIdentity(identity)
                .build();
    }

    private static void assertRedactedDenial(Runnable action, String message, String... sensitive)
    {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo(message);
                    assertThat(failure.getCause()).isNull();
                    StringWriter stack = new StringWriter();
                    failure.printStackTrace(new PrintWriter(stack));
                    String trace = stack.toString();
                    for (String secret : sensitive) {
                        if (secret != null && !secret.isEmpty()) {
                            assertThat(trace).doesNotContain(secret);
                        }
                    }
                });
    }

    private record AwsCredentials(String accessKeyId, String secretAccessKey, String sessionToken) {}

    private record RecordedRead(AwsCredentials credentials, String location) {}

    private record CallerFixture(
            String catalog,
            String principal,
            String mappedUser,
            String accessKey,
            String secretKey,
            String sessionToken,
            String jwt)
    {
        AwsCredentials credentials()
        {
            return new AwsCredentials(accessKey, secretKey, sessionToken);
        }

        String tableFile(String relativePath)
        {
            return "s3://example-customer/" + catalog + "/tables/" + relativePath;
        }
    }

    /**
     * One request to the fake service. For a request on a local-only route, {@code rawPath} and
     * {@code rawPathAndQuery} are the ordinary route that it names, so one handler serves both.
     * {@code wirePathAndQuery} is the target as sent.
     */
    private record RecordedRequest(String method, String rawPath, String rawPathAndQuery, String authorization, String securityToken, String range, boolean isLocalOnly, String wirePathAndQuery) {}

    private record Response(int status, String contentType, byte[] body, Map<String, String> headers)
    {
        Response(int status, String body)
        {
            this(status, "application/json", body.getBytes(UTF_8), Map.of());
        }

        Response(int status, String contentType, String body)
        {
            this(status, contentType, body.getBytes(UTF_8), Map.of());
        }

        static Response bodyless(int status)
        {
            return new Response(status, "application/octet-stream", null, Map.of());
        }

        static Response bodyless(int status, Map<String, String> headers)
        {
            return new Response(status, "application/octet-stream", null, headers);
        }

        static Response bytes(int status, String contentType, byte[] body, Map<String, String> headers)
        {
            return new Response(status, contentType, body, headers);
        }
    }

    private static Response serveS3Object(RecordedRequest request, byte[] content)
    {
        if (request.method().equals("HEAD")) {
            return Response.bodyless(200, Map.of("Content-Length", Integer.toString(content.length)));
        }
        String range = request.range();
        if (range != null && range.startsWith("bytes=")) {
            String[] bounds = range.substring("bytes=".length()).split("-", 2);
            int start;
            int end;
            if (bounds[0].isEmpty()) {
                int suffix = Integer.parseInt(bounds[1]);
                start = Math.max(0, content.length - suffix);
                end = content.length - 1;
            }
            else {
                start = Integer.parseInt(bounds[0]);
                end = bounds[1].isEmpty()
                        ? content.length - 1
                        : Math.min(Integer.parseInt(bounds[1]), content.length - 1);
            }
            byte[] slice = Arrays.copyOfRange(content, start, end + 1);
            return Response.bytes(
                    206,
                    "application/octet-stream",
                    slice,
                    Map.of("Content-Range", "bytes %d-%d/%d".formatted(start, end, content.length)));
        }
        return Response.bytes(200, "application/octet-stream", content, Map.of());
    }

    private static final class DedicatedTestServer
            implements AutoCloseable
    {
        private final HttpServer server;

        private DedicatedTestServer(HttpServer server)
        {
            this.server = server;
        }

        /**
         * A server that serves the local-only routes with the marker on each answer, as Hoglake does.
         */
        static DedicatedTestServer start(List<RecordedRequest> recorded, Function<RecordedRequest, Response> handler)
                throws IOException
        {
            return start(recorded, handler, Optional.empty());
        }

        /**
         * With {@code oldServer} present, the server models a Hoglake that predates the local-only routes.
         * It has no route under the prefix, so it answers each local-only request with that fixed answer and
         * without the marker. The handler never sees such a request.
         */
        static DedicatedTestServer start(List<RecordedRequest> recorded, Function<RecordedRequest, Response> handler, Optional<Response> oldServer)
                throws IOException
        {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> handle(exchange, recorded, handler, oldServer));
            server.start();
            return new DedicatedTestServer(server);
        }

        URI baseUri()
        {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        }

        @Override
        public void close()
        {
            server.stop(0);
        }

        private static void handle(HttpExchange exchange, List<RecordedRequest> recorded, Function<RecordedRequest, Response> handler, Optional<Response> oldServer)
                throws IOException
        {
            URI uri = exchange.getRequestURI();
            String wirePath = uri.getRawPath();
            String rawQuery = uri.getRawQuery();
            String localOnlyCatalogs = HoglakeClient.LOCAL_ONLY_PREFIX + "/catalogs/";
            boolean localOnly = wirePath.startsWith(localOnlyCatalogs);
            String rawPath = wirePath;
            if (localOnly) {
                rawPath = "/v1/catalogs/" + wirePath.substring(localOnlyCatalogs.length());
            }
            String query = rawQuery == null ? "" : "?" + rawQuery;
            RecordedRequest request = new RecordedRequest(
                    exchange.getRequestMethod(),
                    rawPath,
                    rawPath + query,
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    exchange.getRequestHeaders().getFirst("X-Amz-Security-Token"),
                    exchange.getRequestHeaders().getFirst("Range"),
                    localOnly,
                    wirePath + query);
            recorded.add(request);
            Response response;
            if (localOnly && oldServer.isPresent()) {
                response = oldServer.get();
            }
            else {
                response = handler.apply(request);
            }
            exchange.getResponseHeaders().set("Content-Type", response.contentType());
            if (localOnly && oldServer.isEmpty()) {
                exchange.getResponseHeaders().set(HoglakeClient.LOCAL_ONLY_MARKER, "true");
            }
            response.headers().forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
            if (exchange.getRequestMethod().equals("HEAD") || response.body() == null) {
                exchange.sendResponseHeaders(response.status(), -1);
                exchange.close();
                return;
            }
            byte[] bytes = response.body();
            exchange.sendResponseHeaders(response.status(), bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }
    }

    /**
     * Test file system factory that requires exact per-caller AWS session credentials on every worker open.
     */
    private static final class CredentialCheckingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final Map<AwsCredentials, Map<String, byte[]>> filesByCredentials;
        private final List<RecordedRead> reads = new CopyOnWriteArrayList<>();

        private CredentialCheckingFileSystemFactory(Map<AwsCredentials, Map<String, byte[]>> filesByCredentials)
        {
            this.filesByCredentials = filesByCredentials;
        }

        List<RecordedRead> reads()
        {
            return ImmutableList.copyOf(reads);
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            Map<String, String> extra = identity.getExtraCredentials();
            String accessKey = extra.get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY);
            String secretKey = extra.get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY);
            String sessionToken = extra.get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY);
            if (accessKey == null || secretKey == null || sessionToken == null) {
                throw new AccessDeniedException("Missing AWS session credentials");
            }
            AwsCredentials credentials = new AwsCredentials(accessKey, secretKey, sessionToken);
            Map<String, byte[]> files = filesByCredentials.get(credentials);
            if (files == null) {
                throw new AccessDeniedException("Unknown AWS session credentials");
            }
            return new InMemoryCredentialFileSystem(credentials, files, reads);
        }
    }

    private static final class InMemoryCredentialFileSystem
            implements TrinoFileSystem
    {
        private final AwsCredentials credentials;
        private final Map<String, byte[]> files;
        private final List<RecordedRead> reads;

        private InMemoryCredentialFileSystem(AwsCredentials credentials, Map<String, byte[]> files, List<RecordedRead> reads)
        {
            this.credentials = credentials;
            this.files = files;
            this.reads = reads;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return open(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return open(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return open(location);
        }

        private TrinoInputFile open(Location location)
        {
            byte[] content = files.get(location.toString());
            if (content == null) {
                throw new AccessDeniedException("Storage object denied for caller credentials");
            }
            reads.add(new RecordedRead(credentials, location.toString()));
            return new MemoryInputFile(location, Slices.wrappedBuffer(content));
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIterator listFiles(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw new UnsupportedOperationException();
        }
    }
}
