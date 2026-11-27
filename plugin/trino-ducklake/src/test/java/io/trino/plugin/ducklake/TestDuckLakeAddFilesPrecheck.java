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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeCommit;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionInfo;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckLakeAddFilesPrecheck
{
    @TempDir
    Path dataRoot;

    @Test
    void testRejectsOtherTableAndRecordedFilesWithoutOpeningParquetFooters()
            throws Exception
    {
        Path tableADir = Files.createDirectories(dataRoot.resolve("main/table_a"));
        Path tableBDir = Files.createDirectories(dataRoot.resolve("main/table_b"));
        Path droppedDir = Files.createDirectories(dataRoot.resolve("main/dropped_table"));
        Path importsDir = Files.createDirectories(dataRoot.resolve("main/imports"));

        Path tableBRegistered = tableBDir.resolve("registered.parquet");
        Path tableBUnregistered = tableBDir.resolve("unregistered.parquet");
        Path sharedRegistered = importsDir.resolve("shared.parquet");
        Path droppedOrphan = droppedDir.resolve("orphan.parquet");
        Path validFile = tableADir.resolve("valid.parquet");

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("COPY (SELECT 'confidential'::VARCHAR secret_col) TO '%s' (FORMAT PARQUET)".formatted(tableBRegistered));
            statement.execute("COPY (SELECT 'unregistered_secret'::VARCHAR secret_col) TO '%s' (FORMAT PARQUET)".formatted(tableBUnregistered));
            statement.execute("COPY (SELECT 'shared_secret'::VARCHAR secret_col) TO '%s' (FORMAT PARQUET)".formatted(sharedRegistered));
            statement.execute("COPY (SELECT 'dropped_secret'::VARCHAR dropped_col) TO '%s' (FORMAT PARQUET)".formatted(droppedOrphan));
            statement.execute("COPY (SELECT 7::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(validFile));
        }

        List<DuckLakeCommit.TableLocation> tableLocations = ImmutableList.of(
                new DuckLakeCommit.TableLocation(1L, "table_a/", true, "main/", true, true),
                new DuckLakeCommit.TableLocation(2L, "table_b/", true, "main/", true, true),
                new DuckLakeCommit.TableLocation(3L, "dropped_table/", true, "main/", true, false));
        List<DuckLakeCommit.RecordedFile> recordedFiles = ImmutableList.of(
                new DuckLakeCommit.RecordedFile(OptionalLong.of(2L), "registered.parquet", true),
                new DuckLakeCommit.RecordedFile(OptionalLong.of(2L), "../imports/shared.parquet", true));

        RecordingFileSystemFactory recordingFactory = new RecordingFileSystemFactory(new LocalFileSystemFactory(dataRoot));
        StubMetastore metastore = new StubMetastore(tableLocations, recordedFiles);
        DuckLakeMetadata metadata = newMetadata(metastore, recordingFactory);
        ConnectorSession session = TestingConnectorSession.SESSION;
        DuckLakeTableHandle tableA = new DuckLakeTableHandle(
                "main",
                "table_a",
                1L,
                1L,
                "local:///main/table_a/",
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty());

        List<Map.Entry<List<String>, String>> cases = ImmutableList.of(
                Map.entry(ImmutableList.of("local:///main/table_b/registered.parquet"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/table_b/unregistered.parquet"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/table_b/missing.parquet"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/table_b"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/table_b/"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/imports/shared.parquet"), "already registered"),
                Map.entry(ImmutableList.of("local:///main/dropped_table/orphan.parquet"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/table_a/valid.parquet", "local:///main/table_b/unregistered.parquet"), "directory of another table"),
                Map.entry(ImmutableList.of("local:///main/table_a/valid.parquet", "local:///main/imports/shared.parquet"), "already registered"));

        int attempt = 0;
        for (boolean allowMissing : ImmutableList.of(false, true)) {
            for (Map.Entry<List<String>, String> testCase : cases) {
                recordingFactory.reset();
                ConnectorTableExecuteHandle handle = metadata.getTableHandleForExecute(
                                session,
                                new AllowAllAccessControl(),
                                tableA,
                                DuckLakeAddFilesProcedure.NAME,
                                ImmutableMap.of(
                                        "files", testCase.getKey(),
                                        "import_id", "probe_" + attempt++,
                                        "allow_missing", allowMissing,
                                        "replace", false),
                                NO_RETRIES)
                        .orElseThrow();
                assertThatThrownBy(() -> metadata.executeTableExecute(session, handle))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageContaining(testCase.getValue())
                        .hasMessageNotContaining("secret_col")
                        .hasMessageNotContaining("dropped_col")
                        .hasMessageNotContaining("Parquet");
                assertThat(recordingFactory.touchedLocations()).isEmpty();
                assertThat(recordingFactory.fileSystemCreations()).isZero();
            }
        }
    }

    @Test
    void testConcurrentRetryReceiptShortCircuitsBeforeOwnershipAndFileReads()
    {
        List<DuckLakeCommit.TableLocation> tableLocations = ImmutableList.of(
                new DuckLakeCommit.TableLocation(1L, "table_a/", true, "main/", true, true));
        List<DuckLakeCommit.RecordedFile> recordedFiles = ImmutableList.of(
                new DuckLakeCommit.RecordedFile(OptionalLong.of(1L), "already_committed.parquet", true));

        RecordingFileSystemFactory recordingFactory = new RecordingFileSystemFactory(new LocalFileSystemFactory(dataRoot));
        StubMetastore metastore = new StubMetastore(tableLocations, recordedFiles);
        DuckLakeMetadata metadata = newMetadata(metastore, recordingFactory);
        ConnectorSession session = TestingConnectorSession.SESSION;
        DuckLakeTableHandle tableA = new DuckLakeTableHandle(
                "main",
                "table_a",
                1L,
                1L,
                "local:///main/table_a/",
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty());

        DuckLakeAddFilesHandle handle = (DuckLakeAddFilesHandle) metadata.getTableHandleForExecute(
                        session,
                        new AllowAllAccessControl(),
                        tableA,
                        DuckLakeAddFilesProcedure.NAME,
                        ImmutableMap.of(
                                "files", ImmutableList.of("local:///main/table_a/already_committed.parquet"),
                                "import_id", "retry_id",
                                "replace", false),
                        NO_RETRIES)
                .orElseThrow();

        // Simulates a concurrent retry that committed the file and receipt after tableA's snapshot
        metastore.setConcurrentReceipt(Optional.of(DuckLakeFileImporter.fingerprint(handle)));
        assertThat(metadata.executeTableExecute(session, handle)).isEqualTo(ImmutableMap.of("added_data_files", 0L));
        assertThat(recordingFactory.touchedLocations()).isEmpty();
        assertThat(recordingFactory.fileSystemCreations()).isZero();

        // Same import_id with a different fingerprint is rejected without reading files
        metastore.setConcurrentReceipt(Optional.of("different-fingerprint"));
        assertThatThrownBy(() -> metadata.executeTableExecute(session, handle))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("import_id was already used with different files or options");
        assertThat(recordingFactory.touchedLocations()).isEmpty();
        assertThat(recordingFactory.fileSystemCreations()).isZero();
    }

    @Test
    void testRecreatedTableOwnsItsDirectoryWhileDroppedTableDirectoryBlocksOtherTables()
            throws Exception
    {
        Path recreatedDir = Files.createDirectories(dataRoot.resolve("main/recreated"));
        Path file = recreatedDir.resolve("valid.parquet");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("COPY (SELECT 1::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(file));
        }

        // Old table id 10 was dropped, new table id 11 was created at the same directory
        List<DuckLakeCommit.TableLocation> tableLocations = ImmutableList.of(
                new DuckLakeCommit.TableLocation(10L, "recreated/", true, "main/", true, false),
                new DuckLakeCommit.TableLocation(11L, "recreated/", true, "main/", true, true),
                new DuckLakeCommit.TableLocation(12L, "other/", true, "main/", true, true));

        RecordingFileSystemFactory recordingFactory = new RecordingFileSystemFactory(new LocalFileSystemFactory(dataRoot));
        StubMetastore metastore = new StubMetastore(tableLocations, ImmutableList.of());
        DuckLakeMetadata metadata = newMetadata(metastore, recordingFactory);
        ConnectorSession session = TestingConnectorSession.SESSION;

        DuckLakeTableHandle otherTable = new DuckLakeTableHandle(
                "main",
                "other",
                12L,
                1L,
                "local:///main/other/",
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty());
        ConnectorTableExecuteHandle otherHandle = metadata.getTableHandleForExecute(
                        session,
                        new AllowAllAccessControl(),
                        otherTable,
                        DuckLakeAddFilesProcedure.NAME,
                        ImmutableMap.of(
                                "files", ImmutableList.of("local:///main/recreated/valid.parquet"),
                                "import_id", "other_probe",
                                "replace", false),
                        NO_RETRIES)
                .orElseThrow();
        assertThatThrownBy(() -> metadata.executeTableExecute(session, otherHandle))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("directory of another table");
        assertThat(recordingFactory.touchedLocations()).isEmpty();

        // The visible recreated table (id 11) passes pre-read ownership verification and opens its own file
        DuckLakeTableHandle recreatedTable = new DuckLakeTableHandle(
                "main",
                "recreated",
                11L,
                1L,
                "local:///main/recreated/",
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty());
        ConnectorTableExecuteHandle ownHandle = metadata.getTableHandleForExecute(
                        session,
                        new AllowAllAccessControl(),
                        recreatedTable,
                        DuckLakeAddFilesProcedure.NAME,
                        ImmutableMap.of(
                                "files", ImmutableList.of("local:///main/recreated/valid.parquet"),
                                "import_id", "own_import",
                                "replace", false),
                        NO_RETRIES)
                .orElseThrow();
        // Fails only at commit (stub metastore has no JDBC connection), after reading the owned Parquet file
        assertThatThrownBy(() -> metadata.executeTableExecute(session, ownHandle))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("directory of another table")
                .hasMessageNotContaining("already registered");
        assertThat(recordingFactory.touchedLocations()).contains(Location.of("local:///main/recreated/valid.parquet"));
    }

    private static DuckLakeMetadata newMetadata(JdbcDuckLakeMetastore metastore, TrinoFileSystemFactory fileSystemFactory)
    {
        DuckLakeConfig config = new DuckLakeConfig()
                .setDataPath("local:///")
                .setAddFilesProcedureEnabled(true);
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        ParquetReaderConfig readerConfig = new ParquetReaderConfig();
        DuckLakePageSourceProvider pageSourceProvider = new DuckLakePageSourceProvider(fileSystemFactory, stats, readerConfig, metastore);
        DuckLakeRangeReplacement rangeReplacement = new DuckLakeRangeReplacement(metastore, pageSourceProvider, fileSystemFactory, stats, readerConfig, config, TESTING_TYPE_MANAGER);
        return new DuckLakeMetadata(
                metastore,
                config.getDataPath(),
                JsonCodec.jsonCodec(DuckLakeDataFile.class),
                JsonCodec.jsonCodec(DuckLakeMergeFragment.class),
                JsonCodec.jsonCodec(ConnectorViewDefinition.class),
                fileSystemFactory,
                new DuckLakeWriterFactory(NodeVersion.UNKNOWN, new ParquetWriterConfig()),
                stats,
                readerConfig.toParquetReaderOptions(),
                true,
                4,
                rangeReplacement,
                TESTING_TYPE_MANAGER);
    }

    private static final class StubMetastore
            extends JdbcDuckLakeMetastore
    {
        private final List<DuckLakeCommit.TableLocation> tableLocations;
        private final List<DuckLakeCommit.RecordedFile> recordedFiles;
        private Optional<String> concurrentReceipt = Optional.empty();

        private StubMetastore(List<DuckLakeCommit.TableLocation> tableLocations, List<DuckLakeCommit.RecordedFile> recordedFiles)
        {
            super(() -> {
                throw new SQLException("Stub metastore does not open JDBC connections");
            }, new DuckLakeConfig());
            this.tableLocations = ImmutableList.copyOf(tableLocations);
            this.recordedFiles = ImmutableList.copyOf(recordedFiles);
        }

        public void setConcurrentReceipt(Optional<String> concurrentReceipt)
        {
            this.concurrentReceipt = concurrentReceipt;
        }

        @Override
        public boolean fileRegistrationSupported()
        {
            return true;
        }

        @Override
        public Optional<String> tag(long snapshotId, long objectId, String key)
        {
            return Optional.empty();
        }

        @Override
        public List<DuckLakeColumnEntry> columns(long snapshotId, long tableId)
        {
            return ImmutableList.of(new DuckLakeColumnEntry(
                    1L,
                    0L,
                    "id",
                    "int32",
                    OptionalLong.empty(),
                    true,
                    Optional.empty(),
                    Optional.empty()));
        }

        @Override
        public Optional<DuckLakePartitionInfo> partitionInfo(long snapshotId, long tableId)
        {
            return Optional.empty();
        }

        @Override
        public DuckLakeCommit.FileRegistrationState fileRegistrationState(Set<String> fileNames)
        {
            return fileRegistrationState(fileNames, 0L, "");
        }

        @Override
        public DuckLakeCommit.FileRegistrationState fileRegistrationState(Set<String> fileNames, long tableId, String receiptKey)
        {
            if (concurrentReceipt.isPresent()) {
                return new DuckLakeCommit.FileRegistrationState(fileNames, ImmutableList.of(), ImmutableList.of(), 100L, concurrentReceipt);
            }
            List<DuckLakeCommit.RecordedFile> matching = recordedFiles.stream()
                    .filter(recorded -> fileNames.contains(recorded.path().substring(recorded.path().lastIndexOf('/') + 1)))
                    .toList();
            return new DuckLakeCommit.FileRegistrationState(fileNames, tableLocations, matching, 100L, Optional.empty());
        }
    }

    private static final class RecordingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final TrinoFileSystemFactory delegate;
        private final List<Location> touchedLocations = new CopyOnWriteArrayList<>();
        private int fileSystemCreations;

        private RecordingFileSystemFactory(TrinoFileSystemFactory delegate)
        {
            this.delegate = delegate;
        }

        public void reset()
        {
            touchedLocations.clear();
            fileSystemCreations = 0;
        }

        public List<Location> touchedLocations()
        {
            return ImmutableList.copyOf(touchedLocations);
        }

        public int fileSystemCreations()
        {
            return fileSystemCreations;
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            fileSystemCreations++;
            return new RecordingFileSystem(delegate.create(identity), touchedLocations);
        }
    }

    private static final class RecordingFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;
        private final List<Location> touchedLocations;

        private RecordingFileSystem(TrinoFileSystem delegate, List<Location> touchedLocations)
        {
            this.delegate = delegate;
            this.touchedLocations = touchedLocations;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            touchedLocations.add(location);
            return delegate.newInputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            touchedLocations.add(location);
            return delegate.newInputFile(location, length);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            touchedLocations.add(location);
            return delegate.newInputFile(location, length, lastModified);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            touchedLocations.add(location);
            return delegate.newOutputFile(location);
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            touchedLocations.add(location);
            delegate.deleteFile(location);
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            touchedLocations.add(location);
            delegate.deleteDirectory(location);
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            touchedLocations.add(source);
            touchedLocations.add(target);
            delegate.renameFile(source, target);
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            touchedLocations.add(location);
            return delegate.listFiles(location);
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            touchedLocations.add(location);
            return delegate.directoryExists(location);
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            touchedLocations.add(location);
            delegate.createDirectory(location);
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            touchedLocations.add(source);
            touchedLocations.add(target);
            delegate.renameDirectory(source, target);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            touchedLocations.add(location);
            return delegate.listDirectories(location);
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            touchedLocations.add(targetPath);
            return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
        }
    }
}
