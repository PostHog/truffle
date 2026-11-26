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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.Session;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_EXCLUSIVE_CREATE_PROPERTY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeLocalWrites
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ISSUER = "https://issuer.example.test";
    private static final String PROFILE = "example.aws-session";
    private static final String CATALOG = "catalog-a";
    private static final String REGION = "us-east-1";
    private static final String EVENTS_TARGET = "remote:events";
    private static final String IMPORTS_TARGET = "remote:imports";
    private static final Set<String> REMOTE_TARGETS = ImmutableSet.of(EVENTS_TARGET, IMPORTS_TARGET);
    private static final UUID BACKEND_ID = UUID.fromString("11111111-2222-4333-8444-555555555555");
    private static final Instant NOW = Instant.parse("2026-11-26T12:00:00Z");

    @Test
    void testWritableLocalLifecycleAndReadOnlyAndReservedRefusals(@TempDir Path tempDir)
            throws Exception
    {
        KeyPair keyPair = generateEcKeyPair();
        Path jwksFile = tempDir.resolve("jwks.json");
        writeJwks(jwksFile, (ECPublicKey) keyPair.getPublic(), "key-1");

        FakeLocalOnlyServer server = new FakeLocalOnlyServer();
        server.start();
        try {
            MemoryFileSystemFactory memoryFs = new MemoryFileSystemFactory();
            List<Boolean> seenExclusiveCreate = new CopyOnWriteArrayList<>();
            TrinoFileSystemFactory trackingFs = identity -> {
                seenExclusiveCreate.add("true".equals(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_EXCLUSIVE_CREATE_PROPERTY)));
                TrinoFileSystem delegate = memoryFs.create(identity);
                return new S3ToMemoryFileSystem(delegate);
            };

            HoglakeReadSessionVerifier verifier = new HoglakeReadSessionVerifier(
                    ISSUER,
                    jwksFile,
                    PROFILE,
                    REMOTE_TARGETS,
                    ImmutableSet.of(BACKEND_ID),
                    Optional.of(REGION),
                    60,
                    900,
                    Clock.fixed(NOW, ZoneOffset.UTC),
                    System::nanoTime);

            HoglakeDedicatedReadPath path = HoglakeDedicatedReadPath.forSharedReadSession(
                    server.uri(),
                    Duration.ofSeconds(10),
                    verifier,
                    trackingFs,
                    REMOTE_TARGETS,
                    true,
                    "test",
                    null);

            try (StandaloneQueryRunner runner = createRunner(path)) {
                String writableJwt = sign(claims("db.alice", true), keyPair.getPrivate(), "key-1");
                String readOnlyJwt = sign(claims("db.bob", false), keyPair.getPrivate(), "key-1");
                Session writer = session("db.alice", writableJwt);
                Session reader = session("db.bob", readOnlyJwt);

                // SHOW SCHEMAS merges team_4 + pre-existing local_team_4 for both read-only and writable sessions
                assertThat(runner.execute(reader, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                        .contains("team_4", "local_team_4");
                assertThat(runner.execute(writer, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                        .contains("team_4", "local_team_4");

                // Writable session: full DDL + DML + DELETE + DROP lifecycle
                runner.execute(writer, "CREATE SCHEMA hoglake.scratch");
                assertThat(runner.execute(writer, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                        .contains("team_4", "local_team_4", "scratch");

                runner.execute(writer, "CREATE TABLE hoglake.scratch.items (id BIGINT, name VARCHAR)");
                assertThat(runner.execute(writer, "SHOW TABLES FROM hoglake.scratch").getOnlyColumnAsSet())
                        .containsExactly("items");
                assertThat(runner.execute(writer, "DESCRIBE hoglake.scratch.items").getMaterializedRows())
                        .extracting(row -> row.getField(0) + ":" + row.getField(1))
                        .containsExactly("id:bigint", "name:varchar");

                // CTAS is refused until atomic create-and-publish is implemented
                assertThatThrownBy(() -> runner.execute(writer, "CREATE TABLE hoglake.scratch.ctas_blocked AS SELECT 1 AS id"))
                        .hasMessageContaining("CREATE TABLE AS SELECT is not supported");

                runner.execute(writer, "INSERT INTO hoglake.scratch.items VALUES (1, 'alpha'), (2, 'beta')");
                MaterializedResult inserted = runner.execute(writer, "SELECT id, name FROM hoglake.scratch.items ORDER BY id");
                assertThat(inserted.getMaterializedRows())
                        .extracting(row -> row.getField(0) + ":" + row.getField(1))
                        .containsExactly("1:alpha", "2:beta");

                // Every writable S3 identity requested exclusive-create (If-None-Match: *)
                assertThat(seenExclusiveCreate).contains(true);

                // Read-only session can read the written table
                MaterializedResult readBack = runner.execute(reader, "SELECT id, name FROM hoglake.scratch.items ORDER BY id");
                assertThat(readBack.getMaterializedRows())
                        .extracting(row -> row.getField(0) + ":" + row.getField(1))
                        .containsExactly("1:alpha", "2:beta");

                // DELETE via Puffin DV on local table
                runner.execute(writer, "DELETE FROM hoglake.scratch.items WHERE id = 1");
                MaterializedResult afterDelete = runner.execute(writer, "SELECT id, name FROM hoglake.scratch.items ORDER BY id");
                assertThat(afterDelete.getMaterializedRows())
                        .extracting(row -> row.getField(0) + ":" + row.getField(1))
                        .containsExactly("2:beta");

                // UPDATE via atomic DV + new data file commit on local table
                runner.execute(writer, "UPDATE hoglake.scratch.items SET name = 'beta2' WHERE id = 2");
                MaterializedResult afterUpdate = runner.execute(writer, "SELECT id, name FROM hoglake.scratch.items ORDER BY id");
                assertThat(afterUpdate.getMaterializedRows())
                        .extracting(row -> row.getField(0) + ":" + row.getField(1))
                        .containsExactly("2:beta2");

                // Unfiltered DELETE reaches the row-level merge writer via applyDelete returning empty
                runner.execute(writer, "DELETE FROM hoglake.scratch.items");
                assertThat(runner.execute(writer, "SELECT count(*) FROM hoglake.scratch.items").getOnlyValue()).isEqualTo(0L);

                // Default-precision TIMESTAMP normalization and NOT NULL constraint enforcement
                runner.execute(writer, "CREATE TABLE hoglake.scratch.strict_items (id BIGINT NOT NULL, ts TIMESTAMP)");
                runner.execute(writer, "INSERT INTO hoglake.scratch.strict_items VALUES (10, TIMESTAMP '2026-11-26 12:00:00')");
                assertThat(runner.execute(writer, "SELECT id FROM hoglake.scratch.strict_items").getOnlyValue()).isEqualTo(10L);
                assertThatThrownBy(() -> runner.execute(writer, "INSERT INTO hoglake.scratch.strict_items VALUES (NULL, TIMESTAMP '2026-11-26 12:00:00')"))
                        .hasMessageContaining("NULL");
                runner.execute(writer, "DROP TABLE hoglake.scratch.strict_items");

                // Explicit non-autocommit transactions are refused for local DDL and DML
                for (String txSql : List.of(
                        "CREATE TABLE hoglake.scratch.tx_t (id BIGINT)",
                        "INSERT INTO hoglake.scratch.items VALUES (9, 'tx')")) {
                    TransactionId txId = runner.getTransactionManager().beginTransaction(false);
                    try {
                        Session txSession = Session.builder(writer).setTransactionId(txId).build();
                        assertThatThrownBy(() -> runner.execute(txSession, txSql))
                                .as(txSql)
                                .hasMessageContaining("autocommit");
                    }
                    finally {
                        runner.getTransactionManager().asyncAbort(txId).get();
                    }
                }

                // Read-only session is refused on EVERY write with zero upstream write requests
                int writesBeforeReadOnly = server.writeRequests.get();
                for (String sql : List.of(
                        "CREATE SCHEMA hoglake.ro_denied",
                        "CREATE TABLE hoglake.scratch.ro_t (id BIGINT)",
                        "INSERT INTO hoglake.scratch.items VALUES (3, 'gamma')",
                        "DELETE FROM hoglake.scratch.items WHERE id = 2",
                        "DROP TABLE hoglake.scratch.items",
                        "DROP SCHEMA hoglake.local_team_4")) {
                    assertThatThrownBy(() -> runner.execute(reader, sql))
                            .as(sql)
                            .hasMessageContaining("does not allow local writes");
                }
                assertThat(server.writeRequests.get()).isEqualTo(writesBeforeReadOnly);

                // Writable session is refused on reserved & protected schemas with zero upstream write requests
                int writesBeforeReserved = server.writeRequests.get();
                for (String sql : List.of(
                        "CREATE SCHEMA hoglake.team_99",
                        "CREATE SCHEMA hoglake.posthog",
                        "CREATE SCHEMA hoglake.local_team_99",
                        "DROP SCHEMA hoglake.local_team_4",
                        "CREATE TABLE hoglake.team_4.blocked (id BIGINT)")) {
                    assertThatThrownBy(() -> runner.execute(writer, sql))
                            .as(sql)
                            .hasMessageMatching(".*(read-only|cannot be created or dropped).*");
                }
                assertThat(server.writeRequests.get()).isEqualTo(writesBeforeReserved);

                // Clean DROP TABLE + DROP SCHEMA
                runner.execute(writer, "DROP TABLE hoglake.scratch.items");
                assertThat(runner.execute(writer, "SHOW TABLES FROM hoglake.scratch").getOnlyColumnAsSet())
                        .isEmpty();
                runner.execute(writer, "DROP SCHEMA hoglake.scratch");
                assertThat(runner.execute(writer, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                        .doesNotContain("scratch");
            }
        }
        finally {
            server.stop();
        }
    }

    private static Session session(String principal, String jwt)
    {
        Identity identity = Identity.forUser(principal)
                .withPrincipal(new BasicPrincipal(principal))
                .withExtraCredentials(ImmutableMap.of(HoglakeReadSessionVerifier.SESSION_TOKEN, jwt))
                .build();
        return testSessionBuilder()
                .setCatalog("hoglake")
                .setSchema("local_team_4")
                .setIdentity(identity)
                .setOriginalIdentity(identity)
                .build();
    }

    private static StandaloneQueryRunner createRunner(HoglakeDedicatedReadPath path)
    {
        Session base = testSessionBuilder().setCatalog("hoglake").setSchema("local_team_4").build();
        StandaloneQueryRunner runner = new StandaloneQueryRunner(base);
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
                        return "hoglake_local_write_test";
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        return new Connector()
                        {
                            private final Set<ConnectorTransactionHandle> nonAutoCommit = ConcurrentHashMap.newKeySet();

                            @Override
                            public Set<ConnectorCapabilities> getCapabilities()
                            {
                                return path.localWritesEnabled() ? Set.of(ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT) : Set.of();
                            }

                            @Override
                            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
                            {
                                HoglakeTransactionHandle handle = new HoglakeTransactionHandle(UUID.randomUUID());
                                if (!autoCommit) {
                                    nonAutoCommit.add(handle);
                                }
                                return handle;
                            }

                            @Override
                            public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
                            {
                                return path.metadata(session, transactionHandle, !nonAutoCommit.contains(transactionHandle));
                            }

                            @Override
                            public void commit(ConnectorTransactionHandle transactionHandle)
                            {
                                nonAutoCommit.remove(transactionHandle);
                                path.end(transactionHandle);
                            }

                            @Override
                            public void rollback(ConnectorTransactionHandle transactionHandle)
                            {
                                nonAutoCommit.remove(transactionHandle);
                                path.end(transactionHandle);
                            }

                            @Override
                            public List<PropertyMetadata<?>> getSessionProperties()
                            {
                                return new HoglakeSessionProperties(new HoglakeConfig()).getSessionProperties();
                            }

                            @Override
                            public ConnectorSplitManager getSplitManager()
                            {
                                return path.splitManager();
                            }

                            @Override
                            public ConnectorPageSourceProvider getPageSourceProvider()
                            {
                                return path.pageSourceProvider();
                            }

                            @Override
                            public ConnectorPageSinkProvider getPageSinkProvider()
                            {
                                return path.pageSinkProvider();
                            }

                            @Override
                            public void shutdown()
                            {
                                path.close();
                            }
                        };
                    }
                });
            }
        });
        runner.createCatalog("hoglake", "hoglake_local_write_test", ImmutableMap.of());
        return runner;
    }

    private static Map<String, Object> claims(String principal, boolean writable)
    {
        long iat = NOW.getEpochSecond() - 1;
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("iss", ISSUER);
        claims.put("aud", List.of("trino-session", "hoglake-session", "hoglite-session"));
        claims.put("sub", principal);
        claims.put("usr", principal);
        claims.put("jti", "tok-" + (writable ? "rw" : "ro"));
        claims.put("profile", PROFILE);
        claims.put("version", 1);
        claims.put("purpose", "user-session");
        claims.put("ten", "scoped");
        claims.put("org", "example-org");
        claims.put("primary_catalog", CATALOG);
        claims.put("sid", "sid-1");
        claims.put("generation", 1);
        claims.put("iat", iat);
        claims.put("nbf", iat);
        claims.put("exp", iat + 300);
        claims.put("session_deadline", iat + 600);
        claims.put("cat", Map.of(CATALOG, writable ? List.of("read", "write") : List.of("read")));
        claims.put("remote_schemas", Map.of("team", List.of("read")));
        claims.put("prd", Map.of("team_id", Map.of("4", writable ? "write" : "read")));
        claims.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID.toString(), "t", "fixture_3"));
        claims.put("aws", Map.of(
                "access_key_id", "ASIAEXAMPLE000000001",
                "secret_access_key", "secret-example",
                "session_token", "session-token-example",
                "region", REGION,
                "expires_at", iat + 300));
        claims.put("grants", List.of(
                Map.of(
                        "catalog", CATALOG,
                        "target", CATALOG,
                        "bucket", "example-catalog",
                        "region", REGION,
                        "prefix", "data/team_4/",
                        "actions", writable ? HoglakeReadSessionVerifier.WRITE_ACTIONS : HoglakeReadSessionVerifier.READ_ACTIONS),
                Map.of(
                        "catalog", CATALOG,
                        "target", BACKEND_ID.toString(),
                        "bucket", "example-owner",
                        "region", REGION,
                        "object_key", "root/2c/t/fixture_3/manifest",
                        "actions", HoglakeReadSessionVerifier.READ_ACTIONS),
                Map.of(
                        "catalog", CATALOG,
                        "target", EVENTS_TARGET,
                        "bucket", "example-remote",
                        "region", REGION,
                        "prefix", "events/team_id=4/",
                        "actions", HoglakeReadSessionVerifier.READ_ACTIONS),
                Map.of(
                        "catalog", CATALOG,
                        "target", IMPORTS_TARGET,
                        "bucket", "example-remote",
                        "region", REGION,
                        "prefix", "imports/team_id=4/",
                        "actions", HoglakeReadSessionVerifier.READ_ACTIONS)));
        return claims;
    }

    private static final class FakeLocalOnlyServer
    {
        private final AtomicInteger writeRequests = new AtomicInteger();
        private final AtomicLong nextId = new AtomicLong(10);
        private final AtomicLong snapshot = new AtomicLong(1);
        private final Map<String, Long> namespaces = new ConcurrentHashMap<>();
        private final Map<String, Map<String, TableState>> tables = new ConcurrentHashMap<>();
        private HttpServer httpServer;

        private FakeLocalOnlyServer()
        {
            namespaces.put("local_team_4", 1L);
            tables.put("local_team_4", new ConcurrentHashMap<>());
        }

        private void start()
                throws IOException
        {
            httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            httpServer.createContext("/", this::handle);
            httpServer.start();
        }

        private URI uri()
        {
            return URI.create("http://127.0.0.1:" + httpServer.getAddress().getPort());
        }

        private void stop()
        {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }

        private void handle(HttpExchange exchange)
                throws IOException
        {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getRawQuery();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

            if (!"GET".equals(method)) {
                writeRequests.incrementAndGet();
            }

            // Relayed remote team_4 schema
            String remoteNs = "/v1/catalogs/" + CATALOG + "/namespaces/team_4";
            if (path.equals(remoteNs) && "GET".equals(method)) {
                reply(exchange, 200, false, Map.of("namespace_id", 99, "name", "team_4", "remote", true));
                return;
            }
            if (path.equals(remoteNs + "/tables") && "GET".equals(method)) {
                reply(exchange, 200, false, List.of());
                return;
            }

            String localPrefix = HoglakeClient.LOCAL_ONLY_PREFIX + "/catalogs/" + CATALOG;
            if (!path.startsWith(localPrefix)) {
                reply(exchange, 404, false, Map.of("error", "not_found"));
                return;
            }
            String suffix = path.substring(localPrefix.length());

            if (suffix.equals("/namespaces") && "GET".equals(method)) {
                List<Map<String, Object>> list = namespaces.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> Map.<String, Object>of("namespace_id", e.getValue(), "name", e.getKey(), "remote", false))
                        .toList();
                reply(exchange, 200, true, Map.of("namespaces", list));
                return;
            }
            if (suffix.equals("/namespaces") && "POST".equals(method)) {
                String name = MAPPER.readTree(body).get("name").asText();
                if (namespaces.containsKey(name)) {
                    reply(exchange, 409, true, Map.of("error", "already_exists", "message", "exists"));
                    return;
                }
                long id = nextId.incrementAndGet();
                namespaces.put(name, id);
                tables.put(name, new ConcurrentHashMap<>());
                reply(exchange, 201, true, Map.of("namespace_id", id, "name", name, "snapshot_id", snapshot.incrementAndGet()));
                return;
            }

            String[] parts = suffix.split("/");
            // ["", "namespaces", ns]
            if (parts.length == 3 && parts[1].equals("namespaces")) {
                String ns = parts[2];
                Long nsId = namespaces.get(ns);
                if (nsId == null) {
                    reply(exchange, 404, true, Map.of("error", "not_found"));
                    return;
                }
                if ("GET".equals(method)) {
                    reply(exchange, 200, true, Map.of("namespace_id", nsId, "name", ns, "remote", false));
                    return;
                }
                if ("DELETE".equals(method)) {
                    if (!tables.get(ns).isEmpty()) {
                        reply(exchange, 409, true, Map.of("error", "conflict"));
                        return;
                    }
                    namespaces.remove(ns);
                    tables.remove(ns);
                    reply(exchange, 200, true, Map.of("namespace_id", nsId, "snapshot_id", snapshot.incrementAndGet()));
                    return;
                }
            }

            // ["", "namespaces", ns, "tables"]
            if (parts.length == 4 && parts[1].equals("namespaces") && parts[3].equals("tables")) {
                String ns = parts[2];
                Map<String, TableState> nsTables = tables.get(ns);
                if (nsTables == null) {
                    reply(exchange, 404, true, Map.of("error", "not_found"));
                    return;
                }
                if ("GET".equals(method)) {
                    List<Map<String, Object>> list = nsTables.values().stream()
                            .map(t -> Map.<String, Object>of("name", t.name, "table_uuid", t.uuid))
                            .toList();
                    reply(exchange, 200, true, Map.of("tables", list));
                    return;
                }
                if ("POST".equals(method)) {
                    JsonNode req = MAPPER.readTree(body);
                    String name = req.get("name").asText();
                    if (nsTables.containsKey(name)) {
                        reply(exchange, 409, true, Map.of("error", "already_exists"));
                        return;
                    }
                    List<Map<String, Object>> cols = new ArrayList<>();
                    int colId = 1;
                    for (JsonNode c : req.get("columns")) {
                        cols.add(Map.of(
                                "field_id", colId++,
                                "name", c.get("name").asText(),
                                "type", c.get("type").asText(),
                                "nullable", c.path("nullable").asBoolean(true)));
                    }
                    long snap = snapshot.incrementAndGet();
                    TableState state = new TableState(ns, name, UUID.randomUUID().toString(), snap, cols);
                    nsTables.put(name, state);
                    reply(exchange, 201, true, state.toDto());
                    return;
                }
            }

            // ["", "namespaces", ns, "tables", table]
            if (parts.length == 5 && parts[1].equals("namespaces") && parts[3].equals("tables")) {
                String ns = parts[2];
                String tbl = parts[4];
                Map<String, TableState> nsTables = tables.get(ns);
                TableState state = nsTables == null ? null : nsTables.get(tbl);
                if (state == null) {
                    reply(exchange, 404, true, Map.of("error", "not_found"));
                    return;
                }
                if ("GET".equals(method)) {
                    reply(exchange, 200, true, state.toDto());
                    return;
                }
                if ("DELETE".equals(method)) {
                    if (query == null || !query.equals("table_uuid=" + state.uuid)) {
                        reply(exchange, 409, true, Map.of("error", "conflict"));
                        return;
                    }
                    nsTables.remove(tbl);
                    reply(exchange, 200, true, Map.of("table_uuid", state.uuid, "snapshot_id", snapshot.incrementAndGet()));
                    return;
                }
            }

            // ["", "namespaces", ns, "tables", table, "scan"|"commit"]
            if (parts.length == 6 && parts[1].equals("namespaces") && parts[3].equals("tables")) {
                String ns = parts[2];
                String tbl = parts[4];
                String op = parts[5];
                Map<String, TableState> nsTables = tables.get(ns);
                TableState state = nsTables == null ? null : nsTables.get(tbl);
                if (state == null) {
                    reply(exchange, 404, true, Map.of("error", "not_found"));
                    return;
                }
                if ("scan".equals(op) && "GET".equals(method)) {
                    List<Map<String, Object>> scanFiles = new ArrayList<>();
                    for (FileState file : state.files) {
                        Map<String, Object> entry = new LinkedHashMap<>();
                        entry.put("data_file", Map.of(
                                "data_file_id", file.id,
                                "path", file.path,
                                "record_count", file.recordCount,
                                "file_size_bytes", file.fileSizeBytes,
                                "begin_snapshot", file.beginSnapshot));
                        if (file.dv != null) {
                            entry.put("delete_file", file.dv);
                        }
                        scanFiles.add(entry);
                    }
                    reply(exchange, 200, true, scanFiles);
                    return;
                }
                if ("commit".equals(op) && "POST".equals(method)) {
                    JsonNode req = MAPPER.readTree(body);
                    if (!state.uuid.equals(req.get("table_uuid").asText())) {
                        reply(exchange, 409, true, Map.of("error", "conflict"));
                        return;
                    }
                    long snap = snapshot.incrementAndGet();
                    state.readSnapshotId = snap;
                    for (JsonNode add : req.path("add_files")) {
                        state.files.add(new FileState(
                                nextId.incrementAndGet(),
                                add.get("path").asText(),
                                add.get("record_count").asLong(),
                                add.get("file_size_bytes").asLong(),
                                snap));
                    }
                    for (JsonNode dv : req.path("add_dvs")) {
                        long dataFileId = dv.get("data_file_id").asLong();
                        for (FileState file : state.files) {
                            if (file.id == dataFileId) {
                                file.dv = Map.of(
                                        "delete_file_id", nextId.incrementAndGet(),
                                        "data_file_id", dataFileId,
                                        "path", dv.get("path").asText(),
                                        "file_format", HoglakeDeletionVector.PUFFIN_DELETION_VECTOR_FORMAT,
                                        "delete_count", dv.get("delete_count").asLong(),
                                        "file_size_bytes", dv.get("file_size_bytes").asLong(),
                                        "begin_snapshot", snap);
                            }
                        }
                    }
                    reply(exchange, 200, true, Map.of("snapshot_id", snap, "table_uuid", state.uuid));
                    return;
                }
            }

            reply(exchange, 404, true, Map.of("error", "not_found"));
        }

        private static void reply(HttpExchange exchange, int status, boolean localMarker, Object payload)
                throws IOException
        {
            byte[] json = MAPPER.writeValueAsBytes(payload);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if (localMarker) {
                exchange.getResponseHeaders().set(HoglakeClient.LOCAL_ONLY_MARKER, "true");
            }
            exchange.sendResponseHeaders(status, json.length);
            exchange.getResponseBody().write(json);
            exchange.close();
        }
    }

    private static final class TableState
    {
        private final String namespace;
        private final String name;
        private final String uuid;
        private volatile long readSnapshotId;
        private final List<Map<String, Object>> columns;
        private final List<FileState> files = new CopyOnWriteArrayList<>();

        private TableState(String namespace, String name, String uuid, long readSnapshotId, List<Map<String, Object>> columns)
        {
            this.namespace = namespace;
            this.name = name;
            this.uuid = uuid;
            this.readSnapshotId = readSnapshotId;
            this.columns = ImmutableList.copyOf(columns);
        }

        private Map<String, Object> toDto()
        {
            return Map.of(
                    "namespace", namespace,
                    "name", name,
                    "table_uuid", uuid,
                    "read_snapshot_id", readSnapshotId,
                    "remote", false,
                    "columns", columns);
        }
    }

    private static final class FileState
    {
        private final long id;
        private final String path;
        private final long recordCount;
        private final long fileSizeBytes;
        private final long beginSnapshot;
        private volatile Map<String, Object> dv;

        private FileState(long id, String path, long recordCount, long fileSizeBytes, long beginSnapshot)
        {
            this.id = id;
            this.path = path;
            this.recordCount = recordCount;
            this.fileSizeBytes = fileSizeBytes;
            this.beginSnapshot = beginSnapshot;
        }
    }

    private static final class S3ToMemoryFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;

        private S3ToMemoryFileSystem(TrinoFileSystem delegate)
        {
            this.delegate = delegate;
        }

        private static Location rewrite(Location location)
        {
            String raw = location.toString();
            return raw.startsWith("s3://") ? Location.of("memory:///" + raw.substring("s3://".length())) : location;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return delegate.newInputFile(rewrite(location));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return delegate.newInputFile(rewrite(location), length);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return delegate.newInputFile(rewrite(location), length, lastModified);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            return delegate.newOutputFile(rewrite(location));
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            delegate.deleteFile(rewrite(location));
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            delegate.deleteDirectory(rewrite(location));
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            delegate.renameFile(rewrite(source), rewrite(target));
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            return delegate.listFiles(rewrite(location));
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            return delegate.directoryExists(rewrite(location));
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            delegate.createDirectory(rewrite(location));
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            delegate.renameDirectory(rewrite(source), rewrite(target));
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            return delegate.listDirectories(rewrite(location));
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            return delegate.createTemporaryDirectory(rewrite(targetPath), temporaryPrefix, relativePrefix);
        }
    }

    private static KeyPair generateEcKeyPair()
            throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    private static void writeJwks(Path file, ECPublicKey key, String kid)
            throws IOException
    {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        Map<String, Object> jwk = Map.of(
                "kty", "EC",
                "crv", "P-256",
                "kid", kid,
                "alg", "ES256",
                "use", "sig",
                "x", b64.encodeToString(unsigned32(key.getW().getAffineX())),
                "y", b64.encodeToString(unsigned32(key.getW().getAffineY())));
        Files.write(file, MAPPER.writeValueAsBytes(Map.of("keys", List.of(jwk))));
    }

    private static String sign(Map<String, Object> claims, PrivateKey privateKey, String kid)
            throws Exception
    {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString(MAPPER.writeValueAsBytes(Map.of("alg", "ES256", "typ", "JWT", "kid", kid)));
        String payload = b64.encodeToString(MAPPER.writeValueAsBytes(claims));
        String signingInput = header + "." + payload;
        Signature sig = Signature.getInstance("SHA256withECDSAinP1363Format");
        sig.initSign(privateKey);
        sig.update(signingInput.getBytes(StandardCharsets.UTF_8));
        return signingInput + "." + b64.encodeToString(sig.sign());
    }

    private static byte[] unsigned32(BigInteger value)
    {
        byte[] raw = value.toByteArray();
        if (raw.length == 32) {
            return raw;
        }
        byte[] out = new byte[32];
        if (raw.length > 32) {
            System.arraycopy(raw, raw.length - 32, out, 0, 32);
        }
        else {
            System.arraycopy(raw, 0, out, 32 - raw.length, raw.length);
        }
        return out;
    }
}
