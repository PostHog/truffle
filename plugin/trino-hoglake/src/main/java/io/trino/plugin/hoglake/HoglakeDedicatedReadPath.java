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
import io.airlift.slice.Slice;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.hoglake.HoglakeReadSessionContext.StorageSession;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.spi.ErrorCode;
import io.trino.spi.RefreshType;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorWritableTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Dedicated local table read path backed by offline read session verification.
 * Each transaction uses only the caller's verified JWT and AWS session credentials.
 * Every metadata and pinned scan request uses Hoglake's local-only routes, so a remote
 * schema is never relayed, even one whose name collides with a local namespace.
 */
final class HoglakeDedicatedReadPath
        implements AutoCloseable
{
    private static final String AUTHORITY_DENIED = "Dedicated Hoglake read session authority is invalid or unavailable";
    private static final String TARGET_DENIED = "Dedicated Hoglake read session does not allow this storage target";
    private static final String REQUEST_DENIED = "Dedicated Hoglake read session does not allow this request";
    private static final String LOCAL_TABLE_DENIED = "Dedicated Hoglake local read requires a valid pinned local table";

    private static final String DISCOVERY_FAILED = "Hoglake read session metadata request failed";
    private static final String REMOTE_TABLE_DENIED = "Hoglake read session requires a valid pinned table of its own team schema";
    private static final Pattern TEAM_SCHEMA_ANY = Pattern.compile("team_[0-9]+");
    private static final Pattern NAMESPACE_ROUTE = Pattern.compile("/namespaces/([^/]+)");
    private static final Pattern TABLES_ROUTE = Pattern.compile("/namespaces/([^/]+)/tables");
    private static final Pattern TABLE_ROUTE = Pattern.compile("/namespaces/([^/]+)/tables/([^/]+)");
    private static final Pattern SCAN_ROUTE = Pattern.compile("/namespaces/([^/]+)/tables/([^/]+)/scan");
    private static final Pattern NON_NEGATIVE_INTEGER = Pattern.compile("0|[1-9][0-9]*");

    private final HoglakeReadSessionVerifier verifier;
    private final Scope scope;
    private final String baseUri;
    private final String basePath;
    private final Duration requestTimeout;
    private final HttpClient httpClient;
    private final TrinoFileSystemFactory sessionFileSystems;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final Map<ConnectorTransactionHandle, Reader> readers = new ConcurrentHashMap<>();

    HoglakeDedicatedReadPath(HoglakeDedicatedConfig config, HoglakeReadSessionVerifier verifier, TrinoFileSystemFactory fileSystemFactory)
    {
        this(config.getUri(),
                Duration.ofNanos(config.getRequestTimeout().roundTo(NANOSECONDS)),
                verifier,
                fileSystemFactory);
    }

    HoglakeDedicatedReadPath(URI baseUri, Duration requestTimeout, HoglakeReadSessionVerifier verifier, TrinoFileSystemFactory fileSystemFactory)
    {
        this(baseUri, requestTimeout, verifier, fileSystemFactory, Scope.Mode.LOCAL_ONLY, ImmutableSet.of());
    }

    /**
     * The same read session on the relayed routes of its own team schema. Every file must be
     * inside a grant of one of {@code remoteTargets}; a local table is refused.
     */
    static HoglakeDedicatedReadPath forRemoteTeamSchema(
            URI baseUri,
            Duration requestTimeout,
            HoglakeReadSessionVerifier verifier,
            TrinoFileSystemFactory fileSystemFactory,
            Set<String> remoteTargets)
    {
        if (remoteTargets == null || remoteTargets.size() != 2) {
            throw new IllegalArgumentException("Invalid Hoglake read session configuration");
        }
        return new HoglakeDedicatedReadPath(baseUri, requestTimeout, verifier, fileSystemFactory, Scope.Mode.REMOTE_ONLY, remoteTargets);
    }

    /**
     * Combined read session on the shared catalog: the caller's own team schema uses the relayed
     * remote routes and {@code remoteTargets} grants; non-reserved local schemas use local-only
     * routes and the primary-catalog grant. Other reserved schemas are absent with no fallback.
     */
    static HoglakeDedicatedReadPath forSharedReadSession(
            URI baseUri,
            Duration requestTimeout,
            HoglakeReadSessionVerifier verifier,
            TrinoFileSystemFactory fileSystemFactory,
            Set<String> remoteTargets)
    {
        if (remoteTargets == null || remoteTargets.size() != 2) {
            throw new IllegalArgumentException("Invalid Hoglake read session configuration");
        }
        return new HoglakeDedicatedReadPath(baseUri, requestTimeout, verifier, fileSystemFactory, Scope.Mode.COMBINED, remoteTargets);
    }

    private HoglakeDedicatedReadPath(
            URI baseUri,
            Duration requestTimeout,
            HoglakeReadSessionVerifier verifier,
            TrinoFileSystemFactory fileSystemFactory,
            Scope.Mode mode,
            Set<String> remoteTargets)
    {
        this.scope = new Scope(mode, remoteTargets);
        this.verifier = requireNonNull(verifier, "verifier is null");
        this.baseUri = HoglakeClient.validateBaseUri(requireNonNull(baseUri, "baseUri is null").toASCIIString()).toString();
        String path = URI.create(this.baseUri).getRawPath();
        this.basePath = path == null ? "" : path;
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        this.sessionFileSystems = new DedicatedSessionFileSystemFactory(
                verifier,
                requireNonNull(fileSystemFactory, "fileSystemFactory is null"),
                scope);
        this.splitManager = new DedicatedSplitManager();
        // Disable footer and data caches so workers never share state across callers.
        this.pageSourceProvider = new DedicatedPageSourceProvider(verifier, new HoglakePageSourceProvider(sessionFileSystems), scope);
    }

    ConnectorMetadata metadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        reader(session, transaction);
        return new DedicatedMetadata(transaction);
    }

    ConnectorSplitManager splitManager()
    {
        return splitManager;
    }

    ConnectorPageSourceProvider pageSourceProvider()
    {
        return pageSourceProvider;
    }

    TrinoFileSystemFactory sessionFileSystems()
    {
        return sessionFileSystems;
    }

    HoglakeClient.RouteGuard routeGuard(String verifiedCatalog)
    {
        return createRouteGuard(basePath, verifiedCatalog);
    }

    /**
     * Translate S3 access denial exceptions into a redacted permission error.
     */
    static RuntimeException storageDenial(RuntimeException failure)
    {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (isStorageAccessDenied(cause)) {
                return targetDenied();
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return failure;
    }

    private static boolean isStorageAccessDenied(Throwable cause)
    {
        if (cause instanceof java.nio.file.AccessDeniedException
                || cause instanceof AccessDeniedException) {
            return true;
        }
        // Check AWS SDK classes by name because the SDK belongs to the filesystem dependency.
        String name = cause.getClass().getName();
        if (name.equals("software.amazon.awssdk.services.s3.model.AccessDeniedException")) {
            return true;
        }
        return isS3ForbiddenException(cause);
    }

    private static boolean isS3ForbiddenException(Throwable cause)
    {
        for (Class<?> type = cause.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().equals("software.amazon.awssdk.services.s3.model.S3Exception")) {
                try {
                    Object status = type.getMethod("statusCode").invoke(cause);
                    return Integer.valueOf(403).equals(status);
                }
                catch (ReflectiveOperationException _) {
                    return false;
                }
            }
        }
        return false;
    }

    void end(ConnectorTransactionHandle transaction)
    {
        Reader reader = readers.remove(transaction);
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public void close()
    {
        readers.clear();
        httpClient.shutdownNow();
    }

    private Reader reader(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        HoglakeReadSessionContext context = verifier.verify(session);
        Reader reader = readers.computeIfAbsent(transaction, _ -> newReader(context));
        if (!reader.token().equals(context.token())) {
            throw authorityDenied();
        }
        return reader;
    }

    private Reader newReader(HoglakeReadSessionContext context)
    {
        Optional<HoglakeClient> remoteClient = scope.supportsRemote()
                ? Optional.of(new HoglakeClient(
                baseUri,
                context.catalog(),
                requestTimeout,
                new HoglakeTokenSource.Fixed(context.token()),
                httpClient,
                createRemoteRouteGuard(basePath, context.catalog(), scope.teamSchema(context)),
                false))
                : Optional.empty();
        // Local-only routes: Hoglake reads only the caller's own local warehouse and never relays a
        // remote schema. A missing name is ordinary absence. An answer without the marker is refused.
        Optional<HoglakeClient> localClient = scope.supportsLocal()
                ? Optional.of(new HoglakeClient(
                baseUri,
                context.catalog(),
                requestTimeout,
                new HoglakeTokenSource.Fixed(context.token()),
                httpClient,
                createRouteGuard(basePath, context.catalog()),
                true))
                : Optional.empty();
        return new Reader(context.token(), context, remoteClient, localClient, new ConcurrentHashMap<>(), scope);
    }

    /**
     * The four relayed routes of one team schema, and nothing else: the schema, its table
     * list, a table, and a pinned scan. No route takes any other query.
     */
    static HoglakeClient.RouteGuard createRemoteRouteGuard(String basePath, String verifiedCatalog, String teamSchema)
    {
        requireNonNull(basePath, "basePath is null");
        requireNonNull(verifiedCatalog, "verifiedCatalog is null");
        requireNonNull(teamSchema, "teamSchema is null");
        String catalogPath = basePath + "/v1/catalogs/" + verifiedCatalog;
        return (method, uri) -> {
            if (!"GET".equals(method) || uri == null || uri.getRawFragment() != null) {
                throw requestDenied();
            }
            String rawPath = uri.getRawPath();
            if (rawPath == null || !rawPath.startsWith(catalogPath)) {
                throw requestDenied();
            }
            String suffix = rawPath.substring(catalogPath.length());
            String rawQuery = uri.getRawQuery();

            Matcher namespaceMatch = NAMESPACE_ROUTE.matcher(suffix);
            Matcher tablesMatch = TABLES_ROUTE.matcher(suffix);
            Matcher tableMatch = TABLE_ROUTE.matcher(suffix);
            Matcher scanMatch = SCAN_ROUTE.matcher(suffix);
            if (namespaceMatch.matches() || tablesMatch.matches()) {
                Matcher match = namespaceMatch.matches() ? namespaceMatch : tablesMatch;
                if (!teamSchema.equals(match.group(1)) || rawQuery != null) {
                    throw requestDenied();
                }
                return;
            }
            if (tableMatch.matches()) {
                if (!teamSchema.equals(tableMatch.group(1)) || !isValidSegment(tableMatch.group(2)) || rawQuery != null) {
                    throw requestDenied();
                }
                return;
            }
            if (scanMatch.matches()) {
                if (!teamSchema.equals(scanMatch.group(1)) || !isValidSegment(scanMatch.group(2)) || !isValidPinnedScanQuery(rawQuery)) {
                    throw requestDenied();
                }
                return;
            }
            throw requestDenied();
        };
    }

    static HoglakeClient.RouteGuard createRouteGuard(String basePath, String verifiedCatalog)
    {
        requireNonNull(basePath, "basePath is null");
        requireNonNull(verifiedCatalog, "verifiedCatalog is null");
        // Only the local-only routes. An ordinary route, which a remote schema can relay, is denied.
        String catalogPath = basePath + HoglakeClient.LOCAL_ONLY_PREFIX + "/catalogs/" + verifiedCatalog;
        return (method, uri) -> {
            if (!"GET".equals(method) || uri == null || uri.getRawFragment() != null) {
                throw requestDenied();
            }
            String rawPath = uri.getRawPath();
            if (rawPath == null || !rawPath.startsWith(catalogPath)) {
                throw requestDenied();
            }
            String suffix = rawPath.substring(catalogPath.length());
            String rawQuery = uri.getRawQuery();

            Matcher namespaceMatch = NAMESPACE_ROUTE.matcher(suffix);
            if (namespaceMatch.matches()) {
                if (!isValidSegment(namespaceMatch.group(1)) || rawQuery != null) {
                    throw requestDenied();
                }
                return;
            }

            Matcher tableMatch = TABLE_ROUTE.matcher(suffix);
            if (tableMatch.matches()) {
                if (!isValidSegment(tableMatch.group(1)) || !isValidSegment(tableMatch.group(2)) || rawQuery != null) {
                    throw requestDenied();
                }
                return;
            }

            Matcher scanMatch = SCAN_ROUTE.matcher(suffix);
            if (scanMatch.matches()) {
                if (!isValidSegment(scanMatch.group(1)) || !isValidSegment(scanMatch.group(2)) || !isValidPinnedScanQuery(rawQuery)) {
                    throw requestDenied();
                }
                return;
            }

            throw requestDenied();
        };
    }

    private static boolean isValidSegment(String segment)
    {
        if (segment == null || segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
            return false;
        }
        String lower = segment.toLowerCase(ENGLISH);
        return !lower.contains("%2f") && !lower.contains("%5c");
    }

    private static boolean isValidPinnedScanQuery(String rawQuery)
    {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return false;
        }
        String[] pairs = rawQuery.split("&", -1);
        if (pairs.length != 2) {
            return false;
        }
        String snapshotValue = null;
        String tableUuidValue = null;
        for (String pair : pairs) {
            int separator = pair.indexOf('=');
            if (separator <= 0 || separator >= pair.length() - 1) {
                return false;
            }
            String name = pair.substring(0, separator);
            String value = pair.substring(separator + 1);
            if (name.equals("snapshot")) {
                if (snapshotValue != null) {
                    return false;
                }
                snapshotValue = value;
            }
            else if (name.equals("table_uuid")) {
                if (tableUuidValue != null) {
                    return false;
                }
                tableUuidValue = value;
            }
            else {
                return false;
            }
        }
        if (snapshotValue == null || tableUuidValue == null) {
            return false;
        }
        if (!NON_NEGATIVE_INTEGER.matcher(snapshotValue).matches()) {
            return false;
        }
        try {
            if (Long.parseLong(snapshotValue) < 0) {
                return false;
            }
        }
        catch (NumberFormatException _) {
            return false;
        }
        return isCanonicalUuid(tableUuidValue);
    }

    private static boolean isCanonicalUuid(String value)
    {
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            return UUID.fromString(value).toString().equals(value);
        }
        catch (IllegalArgumentException _) {
            return false;
        }
    }

    private static HoglakeTableHandle validateLocalTableHandle(ConnectorTableHandle table, Reader reader)
    {
        HoglakeTableHandle handle = reader.scope().validate(table, reader.context());
        HoglakeTableHandle pinned = reader.pinnedTables().get(handle.schemaTableName());
        if (pinned != null && (pinned.snapshotId() != handle.snapshotId()
                || !pinned.tableUuid().equals(handle.tableUuid())
                || pinned.remote() != handle.remote())) {
            throw reader.scope().tableDenied(handle.remote());
        }
        return handle;
    }

    /**
     * What one read path reads. LOCAL_ONLY reads local tables under the primary-catalog grant.
     * REMOTE_ONLY reads the session's own team schema under {@code remoteTargets}.
     * COMBINED serves the session's own team schema on relayed routes and non-reserved schemas on
     * local-only routes, with strict grant isolation and no fallback between them.
     */
    private record Scope(Mode mode, Set<String> remoteTargets)
    {
        enum Mode
        {
            LOCAL_ONLY,
            REMOTE_ONLY,
            COMBINED,
        }

        private Scope
        {
            requireNonNull(mode, "mode is null");
            remoteTargets = ImmutableSet.copyOf(requireNonNull(remoteTargets, "remoteTargets is null"));
        }

        boolean supportsRemote()
        {
            return mode == Mode.REMOTE_ONLY || mode == Mode.COMBINED;
        }

        boolean supportsLocal()
        {
            return mode == Mode.LOCAL_ONLY || mode == Mode.COMBINED;
        }

        boolean redactsDiscovery()
        {
            return mode != Mode.LOCAL_ONLY;
        }

        String teamSchema(HoglakeReadSessionContext context)
        {
            return "team_" + context.teamId();
        }

        static boolean isReservedSchema(String schemaName)
        {
            if (schemaName == null) {
                return false;
            }
            String lower = schemaName.toLowerCase(ENGLISH);
            return lower.equals("posthog") || TEAM_SCHEMA_ANY.matcher(lower).matches();
        }

        Optional<Boolean> resolveRemote(String schemaName, HoglakeReadSessionContext context)
        {
            if (schemaName == null || !schemaName.equals(schemaName.toLowerCase(ENGLISH))) {
                return Optional.empty();
            }
            return switch (mode) {
                case LOCAL_ONLY -> Optional.of(false);
                case REMOTE_ONLY -> schemaName.equals(teamSchema(context)) ? Optional.of(true) : Optional.empty();
                case COMBINED -> {
                    if (schemaName.equals(teamSchema(context))) {
                        yield Optional.of(true);
                    }
                    if (isReservedSchema(schemaName)) {
                        yield Optional.empty();
                    }
                    yield Optional.of(false);
                }
            };
        }

        TrinoException listingDenied()
        {
            return supportsRemote()
                    ? new TrinoException(NOT_SUPPORTED, "Hoglake read session does not support this listing")
                    : HoglakeDedicatedReadPath.listingDenied();
        }

        TrinoException tableDenied(boolean remote)
        {
            return remote && supportsRemote() ? new TrinoException(PERMISSION_DENIED, REMOTE_TABLE_DENIED) : localTableDenied();
        }

        void requireLocation(HoglakeReadSessionContext context, String location, boolean remote)
        {
            String region = context.storage().region();
            if (!remote) {
                if (!supportsLocal()) {
                    throw targetDenied();
                }
                context.requireStorageLocation(context.catalog(), location, region);
                return;
            }
            if (!supportsRemote()) {
                throw targetDenied();
            }
            for (String target : remoteTargets) {
                if (context.allowsStorageLocation(target, location, region)) {
                    return;
                }
            }
            throw targetDenied();
        }

        void requireAnyConfiguredLocation(HoglakeReadSessionContext context, String location)
        {
            String region = context.storage().region();
            if (supportsLocal() && context.allowsStorageLocation(context.catalog(), location, region)) {
                return;
            }
            if (supportsRemote()) {
                for (String target : remoteTargets) {
                    if (context.allowsStorageLocation(target, location, region)) {
                        return;
                    }
                }
            }
            throw targetDenied();
        }

        HoglakeTableHandle validate(ConnectorTableHandle table, HoglakeReadSessionContext context)
        {
            if (!(table instanceof HoglakeTableHandle handle)) {
                throw tableDenied(mode == Mode.REMOTE_ONLY);
            }
            boolean validMode = handle.remote()
                    ? supportsRemote() && teamSchema(context).equals(handle.schemaName())
                    : supportsLocal() && (mode != Mode.COMBINED
                    || (handle.schemaName().equals(handle.schemaName().toLowerCase(ENGLISH)) && !isReservedSchema(handle.schemaName())));
            if (!validMode
                    || handle.snapshotId() < 0
                    || !isCanonicalUuid(handle.tableUuid())
                    || !handle.stagedFiles().isEmpty()
                    || !handle.stagedDeletes().isEmpty()) {
                throw tableDenied(handle.remote());
            }
            return handle;
        }
    }

    private static List<HoglakeSplit> toLocalSplits(List<HoglakeDtos.ScanFile> scanFiles, long maxSplitSize)
    {
        ImmutableList.Builder<HoglakeSplit> splits = ImmutableList.builder();
        for (HoglakeDtos.ScanFile file : scanFiles) {
            HoglakeDtos.DataFile dataFile = file.dataFile();
            OptionalLong footerSize = dataFile.footerSize() == null ? OptionalLong.empty() : OptionalLong.of(dataFile.footerSize());
            HoglakeDtos.DeleteFile deleteFile = file.deleteFile();
            Optional<String> deletePath = deleteFile == null ? Optional.empty() : Optional.of(deleteFile.path());
            long deleteCount = deleteFile == null ? 0 : deleteFile.deleteCount();
            Optional<String> deleteFormat = deleteFile == null ? Optional.empty() : Optional.ofNullable(deleteFile.fileFormat());

            // Leave recordCount negative so count queries always read the Parquet file.
            HoglakeSplit wholeFile = new HoglakeSplit(
                    dataFile.dataFileId(),
                    dataFile.path(),
                    dataFile.fileSizeBytes(),
                    -1,
                    deletePath,
                    deleteCount,
                    deleteFormat,
                    0,
                    dataFile.fileSizeBytes(),
                    SplitWeight.standard(),
                    footerSize,
                    Optional.empty());
            long fileSizeBytes = dataFile.fileSizeBytes();
            if (fileSizeBytes <= maxSplitSize) {
                splits.add(wholeFile);
                continue;
            }
            for (HoglakeSplitManager.ByteRange range : HoglakeSplitManager.planRanges(fileSizeBytes, footerSize, dataFile.splitOffsets(), maxSplitSize)) {
                splits.add(wholeFile.withRange(
                        range.start(),
                        range.length(),
                        HoglakeSplitManager.splitWeight(range.length(), maxSplitSize),
                        Optional.empty()));
            }
        }
        return splits.build();
    }

    private static HoglakeSplit stripMetadataCount(HoglakeSplit split)
    {
        if (split.recordCount() < 0 && split.affinityKey().isEmpty()) {
            return split;
        }
        return new HoglakeSplit(
                split.dataFileId(),
                split.path(),
                split.fileSizeBytes(),
                -1,
                split.deleteFilePath(),
                split.deleteCount(),
                split.deleteFileFormat(),
                split.start(),
                split.length(),
                split.splitWeight(),
                split.footerSize(),
                Optional.empty());
    }

    private static HoglakeTableHandle stripMetadataCount(HoglakeTableHandle handle)
    {
        if (!handle.countOnly() && handle.recordCount().isEmpty()) {
            return handle;
        }
        return new HoglakeTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.snapshotId(),
                handle.tableUuid(),
                handle.columns(),
                handle.constraint(),
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                OptionalLong.empty(),
                handle.remote());
    }

    private static HoglakeColumnHandle toColumnHandle(HoglakeDtos.Column column)
    {
        try {
            return HoglakeMetadata.toColumnHandle(column);
        }
        catch (RuntimeException _) {
            throw new TrinoException(NOT_SUPPORTED, "Dedicated Hoglake column metadata is not supported");
        }
    }

    private static TrinoException authorityDenied()
    {
        return new TrinoException(PERMISSION_DENIED, AUTHORITY_DENIED);
    }

    private static TrinoException targetDenied()
    {
        return new TrinoException(PERMISSION_DENIED, TARGET_DENIED);
    }

    /**
     * A metadata request of a remote read session, the scan included. A failure keeps its error code and
     * loses its message and cause: the client's message can hold the request URI and
     * the response body.
     */
    private <T> T discover(Supplier<T> request)
    {
        if (!scope.redactsDiscovery()) {
            return request.get();
        }
        try {
            return request.get();
        }
        catch (TrinoException e) {
            if (PERMISSION_DENIED.toErrorCode().equals(e.getErrorCode())) {
                throw requestDenied();
            }
            ErrorCode code = e.getErrorCode();
            throw new TrinoException(() -> code, DISCOVERY_FAILED);
        }
    }

    private static TrinoException requestDenied()
    {
        return new TrinoException(PERMISSION_DENIED, REQUEST_DENIED);
    }

    private static TrinoException localTableDenied()
    {
        return new TrinoException(PERMISSION_DENIED, LOCAL_TABLE_DENIED);
    }

    private static TrinoException listingDenied()
    {
        return new TrinoException(NOT_SUPPORTED, "Dedicated Hoglake local read does not support catalog listing");
    }

    private static TrinoException writeDenied()
    {
        return new TrinoException(NOT_SUPPORTED, "Dedicated Hoglake local read is read-only");
    }

    private record Reader(
            String token,
            HoglakeReadSessionContext context,
            Optional<HoglakeClient> remoteClient,
            Optional<HoglakeClient> localClient,
            Map<SchemaTableName, HoglakeTableHandle> pinnedTables,
            Scope scope)
    {
        HoglakeClient clientFor(ConnectorSession session, boolean remote)
        {
            return (remote ? remoteClient : localClient)
                    .orElseThrow(HoglakeDedicatedReadPath::requestDenied)
                    .forQuery(session.getQueryId());
        }

        void close()
        {
            remoteClient.ifPresent(HoglakeClient::close);
            localClient.ifPresent(HoglakeClient::close);
        }

        @Override
        public String toString()
        {
            return "Reader{redacted}";
        }
    }

    private final class DedicatedSplitManager
            implements ConnectorSplitManager
    {
        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                Set<ColumnHandle> dynamicFilterColumns,
                Constraint constraint)
        {
            Reader reader = reader(session, transaction);
            HoglakeTableHandle handle = validateLocalTableHandle(table, reader);
            if (handle.constraint().isNone() || constraint.getSummary().isNone()) {
                return new FixedSplitSource(ImmutableList.of());
            }

            List<HoglakeDtos.ScanFile> scanFiles = discover(() -> reader.clientFor(session, handle.remote())
                    .scanPinned(handle.schemaName(), handle.tableName(), handle.snapshotId(), handle.tableUuid()));

            HoglakeReadSessionContext context = reader.context();

            // Validate every returned file and deletion vector before emitting any split.
            for (HoglakeDtos.ScanFile file : scanFiles) {
                if (file == null || file.dataFile() == null) {
                    throw scope.tableDenied(handle.remote());
                }
                HoglakeDtos.DataFile dataFile = file.dataFile();
                scope.requireLocation(context, dataFile.path(), handle.remote());
                if (dataFile.fileSizeBytes() < 0) {
                    throw scope.tableDenied(handle.remote());
                }
                HoglakeDtos.DeleteFile deleteFile = file.deleteFile();
                if (deleteFile != null) {
                    scope.requireLocation(context, deleteFile.path(), handle.remote());
                    if (deleteFile.dataFileId() != dataFile.dataFileId()) {
                        throw scope.tableDenied(handle.remote());
                    }
                }
            }

            long maxSplitSize = HoglakeSessionProperties.getMaxSplitSize(session).toBytes();
            return new FixedSplitSource(toLocalSplits(scanFiles, maxSplitSize));
        }

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableFunctionHandle function)
        {
            verifier.verify(session);
            throw writeDenied();
        }
    }

    private final class DedicatedMetadata
            implements ConnectorMetadata
    {
        private final ConnectorTransactionHandle transaction;

        private DedicatedMetadata(ConnectorTransactionHandle transaction)
        {
            this.transaction = requireNonNull(transaction, "transaction is null");
        }

        @Override
        public boolean schemaExists(ConnectorSession session, String schemaName)
        {
            Reader reader = reader(session, transaction);
            Optional<Boolean> remote = scope.resolveRemote(schemaName, reader.context());
            if (remote.isEmpty()) {
                return false;
            }
            boolean isRemote = remote.orElseThrow();
            return discover(() -> reader.clientFor(session, isRemote).findNamespace(schemaName))
                    .filter(namespace -> schemaName.equals(namespace.name()) && namespace.isRemote() == isRemote)
                    .isPresent();
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            Reader reader = reader(session, transaction);
            if (!scope.supportsRemote()) {
                throw scope.listingDenied();
            }
            // The one remote schema this session can read. Other schemas are not listed to it.
            String teamSchema = scope.teamSchema(reader.context());
            return schemaExists(session, teamSchema) ? ImmutableList.of(teamSchema) : ImmutableList.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
        {
            Reader reader = reader(session, transaction);
            if (!scope.supportsRemote()) {
                throw scope.listingDenied();
            }
            String teamSchema = scope.teamSchema(reader.context());
            if ((schemaName.isPresent() && !schemaName.get().equals(teamSchema)) || !schemaExists(session, teamSchema)) {
                return ImmutableList.of();
            }
            return discover(() -> reader.clientFor(session, true).listTables(teamSchema)).stream()
                    .filter(table -> table != null && table.name() != null && !table.name().isBlank())
                    .map(table -> new SchemaTableName(teamSchema, table.name()))
                    .collect(toImmutableList());
        }

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion,
                Optional<ConnectorTableVersion> endVersion)
        {
            Reader reader = reader(session, transaction);
            if (startVersion.isPresent() || endVersion.isPresent()) {
                throw new TrinoException(NOT_SUPPORTED, scope.supportsRemote()
                        ? "Table versioning is not supported by a Hoglake read session"
                        : "Table versioning is not supported by dedicated Hoglake local read");
            }
            Optional<Boolean> remote = scope.resolveRemote(tableName.getSchemaName(), reader.context());
            if (remote.isEmpty()) {
                return null;
            }
            boolean isRemote = remote.orElseThrow();
            HoglakeTableHandle cached = reader.pinnedTables().get(tableName);
            if (cached != null) {
                return cached;
            }
            // The engine resolves a table without asking for its schema first. In remote and
            // combined modes the server must answer the namespace itself with the expected
            // locality before any table of it is pinned.
            if ((isRemote || scope.mode() == Scope.Mode.COMBINED) && !schemaExists(session, tableName.getSchemaName())) {
                return null;
            }
            Optional<HoglakeDtos.Table> found = discover(() -> reader.clientFor(session, isRemote)
                    .getTable(tableName.getSchemaName(), tableName.getTableName()));
            if (found.isEmpty()) {
                return null;
            }
            HoglakeDtos.Table table = found.get();
            if (table.isRemote() != isRemote) {
                throw scope.tableDenied(isRemote);
            }
            if (!tableName.getSchemaName().equals(table.namespace())
                    || !tableName.getTableName().equals(table.name())
                    || !isCanonicalUuid(table.tableUuid())
                    || table.readSnapshotId() == null
                    || table.readSnapshotId() < 0
                    || table.columns() == null
                    || table.columns().isEmpty()) {
                throw scope.tableDenied(isRemote);
            }

            List<HoglakeColumnHandle> columns = table.columns().stream()
                    .map(HoglakeDedicatedReadPath::toColumnHandle)
                    .collect(toImmutableList());
            if (columns.stream().anyMatch(column -> column.name() == null || column.name().isBlank())
                    || columns.stream().map(HoglakeColumnHandle::name).distinct().count() != columns.size()) {
                throw scope.tableDenied(isRemote);
            }
            HoglakeTableHandle handle = new HoglakeTableHandle(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    table.readSnapshotId(),
                    table.tableUuid(),
                    columns,
                    TupleDomain.all(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    false,
                    OptionalLong.empty(),
                    isRemote);
            HoglakeTableHandle existing = reader.pinnedTables().putIfAbsent(tableName, handle);
            return existing != null ? existing : handle;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
        {
            Reader reader = reader(session, transaction);
            HoglakeTableHandle handle = validateLocalTableHandle(table, reader);
            return new ConnectorTableMetadata(
                    handle.schemaTableName(),
                    handle.columns().stream()
                            .map(HoglakeColumnHandle::columnMetadata)
                            .collect(toImmutableList()));
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            Reader reader = reader(session, transaction);
            HoglakeTableHandle handle = validateLocalTableHandle(tableHandle, reader);
            ImmutableMap.Builder<String, ColumnHandle> columns = ImmutableMap.builder();
            for (HoglakeColumnHandle column : handle.columns()) {
                columns.put(column.name(), column);
            }
            return columns.buildOrThrow();
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(tableHandle, reader);
            return ((HoglakeColumnHandle) columnHandle).columnMetadata();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(tableHandle, reader);
            return TableStatistics.empty();
        }

        @Override
        public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(table, reader);
            return ConnectorMetadata.super.getTableProperties(session, table);
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
        {
            Reader reader = reader(session, transaction);
            HoglakeTableHandle handle = validateLocalTableHandle(table, reader);
            TupleDomain<HoglakeColumnHandle> predicate = handle.constraint()
                    .intersect(constraint.getSummary().transformKeys(HoglakeColumnHandle.class::cast));
            if (predicate.equals(handle.constraint())) {
                return Optional.empty();
            }
            return Optional.of(new ConstraintApplicationResult<>(
                    handle.withConstraint(predicate),
                    constraint.getSummary(),
                    constraint.getExpression(),
                    false));
        }

        @Override
        public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
                ConnectorSession session,
                ConnectorTableHandle table,
                List<ConnectorExpression> projections,
                Map<String, ColumnHandle> assignments)
        {
            Reader reader = reader(session, transaction);
            HoglakeTableHandle handle = validateLocalTableHandle(table, reader);
            if (projections.isEmpty()) {
                // Keep count queries on the Parquet read path instead of metadata counts.
                return Optional.empty();
            }
            return HoglakeVariantProjections.applyProjection(handle, projections, assignments);
        }

        @Override
        public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(handle, reader);
            return Optional.empty();
        }

        @Override
        public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(handle, reader);
            return Optional.empty();
        }

        @Override
        public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
                ConnectorSession session,
                ConnectorTableHandle handle,
                List<AggregateFunction> aggregates,
                Map<String, ColumnHandle> assignments,
                List<List<ColumnHandle>> groupingSets)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(handle, reader);
            return Optional.empty();
        }

        @Override
        public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
                ConnectorSession session,
                JoinType joinType,
                ConnectorTableHandle left,
                ConnectorTableHandle right,
                ConnectorExpression joinCondition,
                Map<String, ColumnHandle> leftAssignments,
                Map<String, ColumnHandle> rightAssignments,
                JoinStatistics statistics)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(left, reader);
            validateLocalTableHandle(right, reader);
            return Optional.empty();
        }

        @Override
        public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
                ConnectorSession session,
                ConnectorTableHandle handle,
                long topNCount,
                List<SortItem> sortItems,
                Map<String, ColumnHandle> assignments)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(handle, reader);
            return Optional.empty();
        }

        @Override
        public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(handle, reader);
        }

        @Override
        public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(tableHandle, reader);
            return Optional.empty();
        }

        @Override
        public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(tableHandle, reader);
            return false;
        }

        @Override
        public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            Reader reader = reader(session, transaction);
            validateLocalTableHandle(tableHandle, reader);
            return Optional.empty();
        }

        @Override
        public Iterator<RelationColumnsMetadata> streamRelationColumns(
                ConnectorSession session,
                Optional<String> schemaName,
                UnaryOperator<Set<SchemaTableName>> relationFilter)
        {
            Reader reader = reader(session, transaction);
            if (!scope.supportsRemote()) {
                throw scope.listingDenied();
            }
            // The columns of the session's own team schema, each from the table's pinned handle
            Set<SchemaTableName> tables = relationFilter.apply(ImmutableSet.copyOf(listTables(session, schemaName)));
            ImmutableList.Builder<RelationColumnsMetadata> relations = ImmutableList.builder();
            for (SchemaTableName name : tables) {
                if (!name.getSchemaName().equals(scope.teamSchema(reader.context()))) {
                    continue;
                }
                ConnectorTableHandle handle = getTableHandle(session, name, Optional.empty(), Optional.empty());
                if (handle != null) {
                    relations.add(RelationColumnsMetadata.forTable(name, getTableMetadata(session, handle).getColumns()));
                }
            }
            return relations.build().iterator();
        }

        @Override
        public Iterator<RelationCommentMetadata> streamRelationComments(
                ConnectorSession session,
                Optional<String> schemaName,
                UnaryOperator<Set<SchemaTableName>> relationFilter)
        {
            reader(session, transaction);
            if (scope.supportsRemote()) {
                // No comment is read on this path
                return ImmutableList.<RelationCommentMetadata>of().iterator();
            }
            throw scope.listingDenied();
        }

        @SuppressWarnings("deprecation")
        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            reader(session, transaction);
            throw scope.listingDenied();
        }

        @SuppressWarnings("deprecation")
        @Override
        public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            reader(session, transaction);
            throw scope.listingDenied();
        }

        @Override
        public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
                ConnectorSession session,
                ConnectorAccessControl accessControl,
                ConnectorTableHandle tableHandle,
                String procedureName,
                Map<String, Object> executeProperties,
                RetryMode retryMode)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableExecuteHandle> getTableHandleForMaterializedViewExecute(
                ConnectorSession session,
                ConnectorAccessControl accessControl,
                ConnectorTableHandle tableHandle,
                String procedureName,
                Map<String, Object> executeProperties,
                RetryMode retryMode)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Set<ColumnHandle> getColumnHandlesForTableExecute(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                ConnectorTableExecuteHandle connectorTableExecuteHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
                ConnectorSession session,
                ConnectorTableExecuteHandle tableExecuteHandle,
                ConnectorTableHandle updatedSourceTableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Map<String, Long> finishTableExecute(
                ConnectorSession session,
                ConnectorTableExecuteHandle tableExecuteHandle,
                Collection<Slice> fragments,
                List<Object> tableExecuteState)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishCreateTable(
                ConnectorSession session,
                ConnectorOutputTableHandle tableHandle,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(
                ConnectorSession session,
                ConnectorInsertTableHandle insertHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ConnectorInsertTableHandle beginRefreshMaterializedView(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                boolean hasForeignSourceTables,
                RetryMode retryMode,
                RefreshType refreshType)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                ConnectorInsertTableHandle insertHandle,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics,
                List<ConnectorTableHandle> sourceTableHandles,
                boolean hasForeignSourceTables,
                boolean hasSourceTableFunctions,
                boolean hasNonDeterministicFunctions)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public ConnectorMergeTableHandle beginMerge(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
                RetryMode retryMode)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishMerge(
                ConnectorSession session,
                ConnectorMergeTableHandle mergeTableHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishMerge(
                ConnectorSession session,
                ConnectorMergeTableHandle mergeTableHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics,
                MemoryContext memoryContext)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle handle, Map<ColumnHandle, Constant> assignments)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorWritableTableHandle tableHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }

        @Override
        public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableFunctionHandle tableFunctionHandle)
        {
            reader(session, transaction);
            throw writeDenied();
        }
    }

    private static final class DedicatedPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        private final HoglakeReadSessionVerifier verifier;
        private final ConnectorPageSourceProvider delegate;
        private final Scope scope;

        private DedicatedPageSourceProvider(HoglakeReadSessionVerifier verifier, ConnectorPageSourceProvider delegate, Scope scope)
        {
            this.verifier = requireNonNull(verifier, "verifier is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        public ConnectorPageSource createPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                MemoryContext memoryContext)
        {
            HoglakeReadSessionContext context = verifier.verify(session);
            HoglakeTableHandle handle = scope.validate(table, context);
            if (!(split instanceof HoglakeSplit hoglakeSplit)) {
                throw scope.tableDenied(handle.remote());
            }

            scope.requireLocation(context, hoglakeSplit.path(), handle.remote());
            if (hoglakeSplit.deleteFilePath().isPresent()) {
                scope.requireLocation(context, hoglakeSplit.deleteFilePath().orElseThrow(), handle.remote());
            }

            HoglakeSplit localSplit = stripMetadataCount(hoglakeSplit);
            HoglakeTableHandle localTable = stripMetadataCount(handle);
            try {
                ConnectorPageSource pageSource = delegate.createPageSource(
                        transaction,
                        session,
                        localSplit,
                        localTable,
                        tableCredentials,
                        columns,
                        dynamicFilter,
                        memoryContext);
                return new RedactingPageSource(pageSource);
            }
            catch (RuntimeException failure) {
                throw storageDenial(failure);
            }
        }
    }

    private static final class RedactingPageSource
            implements ConnectorPageSource
    {
        private final ConnectorPageSource delegate;

        private RedactingPageSource(ConnectorPageSource delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return delegate.getCompletedPositions();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            try {
                return delegate.getNextSourcePage();
            }
            catch (RuntimeException failure) {
                throw storageDenial(failure);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public Metrics getMetrics()
        {
            return delegate.getMetrics();
        }
    }

    /**
     * File system factory that injects only verified JWT AWS session credentials.
     * Each returned file system enforces this path's storage grants on every file open.
     */
    private static final class DedicatedSessionFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final HoglakeReadSessionVerifier verifier;
        private final TrinoFileSystemFactory delegate;
        private final Scope scope;

        private DedicatedSessionFileSystemFactory(HoglakeReadSessionVerifier verifier, TrinoFileSystemFactory delegate, Scope scope)
        {
            this.verifier = requireNonNull(verifier, "verifier is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            HoglakeReadSessionContext context = verifier.verify(identity);
            StorageSession storage = context.storage();
            TrinoFileSystem fileSystem = delegate.create(ConnectorIdentity.forUser("hoglake-dedicated-read")
                    .withExtraCredentials(ImmutableMap.of(
                            EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, storage.accessKeyId(),
                            EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, storage.secretAccessKey(),
                            EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, storage.sessionToken()))
                    .build());
            return new AuthorizingFileSystem(fileSystem, context, scope);
        }
    }

    /**
     * Read-only file system wrapper that enforces this path's signed grants on every open.
     */
    private static final class AuthorizingFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;
        private final HoglakeReadSessionContext context;
        private final Scope scope;

        private AuthorizingFileSystem(TrinoFileSystem delegate, HoglakeReadSessionContext context, Scope scope)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.context = requireNonNull(context, "context is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        private void authorize(Location location)
        {
            if (location == null) {
                throw targetDenied();
            }
            scope.requireAnyConfiguredLocation(context, location.toString());
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            authorize(location);
            return delegate.newInputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            authorize(location);
            return delegate.newInputFile(location, length);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            authorize(location);
            return delegate.newInputFile(location, length, lastModified);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw targetDenied();
        }

        @Override
        public void deleteFile(Location location)
        {
            throw targetDenied();
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw targetDenied();
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw targetDenied();
        }

        @Override
        public FileIterator listFiles(Location location)
        {
            throw targetDenied();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            throw targetDenied();
        }

        @Override
        public void createDirectory(Location location)
        {
            throw targetDenied();
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw targetDenied();
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            throw targetDenied();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw targetDenied();
        }

        @Override
        public Metrics getMetrics()
        {
            return delegate.getMetrics();
        }
    }
}
