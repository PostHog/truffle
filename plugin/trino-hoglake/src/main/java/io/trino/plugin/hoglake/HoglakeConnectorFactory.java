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
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.PageSorter;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import org.weakref.jmx.guice.MBeanModule;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HoglakeConnectorFactory
        implements ConnectorFactory
{
    private static final String MULTI_TENANT_PROPERTY = "hoglake.multi-tenant";
    private static final String SHARED_REMOTE_READ_PROPERTY = "hoglake.shared.remote-read";
    private static final Set<String> SESSION_ONLY_S3_PROPERTIES = ImmutableSet.of("s3.region", "s3.endpoint", "s3.path-style-access");

    @Override
    public String getName()
    {
        return "hoglake";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        ClassLoader classLoader = getClass().getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            if (booleanProperty(config, MULTI_TENANT_PROPERTY)) {
                if (booleanProperty(config, SHARED_REMOTE_READ_PROPERTY)) {
                    Map<String, String> properties = sessionOnlyS3Properties(config, SHARED_REMOTE_READ_PROPERTY);
                    Injector injector = new Bootstrap(
                            "io.trino.bootstrap.catalog." + catalogName,
                            new MBeanModule(),
                            new MBeanServerModule(),
                            new SharedModule(Optional.of(properties.get("s3.region")), context.getNodeManager().getCurrentNode().getVersion(), context.getPageSorter()),
                            new FileSystemModule(catalogName, context, false),
                            new ConnectorContextModule(catalogName, context))
                            .doNotInitializeLogging()
                            .disableSystemProperties()
                            .setRequiredConfigurationProperties(properties)
                            .initialize();
                    return injector.getInstance(HoglakeSharedConnector.class);
                }
                Injector injector = new Bootstrap(
                        "io.trino.bootstrap.catalog." + catalogName,
                        new SharedModule(Optional.empty(), context.getNodeManager().getCurrentNode().getVersion(), context.getPageSorter()),
                        new ConnectorContextModule(catalogName, context))
                        .doNotInitializeLogging()
                        .disableSystemProperties()
                        .setRequiredConfigurationProperties(config)
                        .initialize();
                return injector.getInstance(HoglakeSharedConnector.class);
            }
            Injector injector = new Bootstrap(
                    "io.trino.bootstrap.catalog." + catalogName,
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new HoglakeModule(),
                    new FileSystemModule(catalogName, context, false),
                    new ConnectorContextModule(catalogName, context))
                    .doNotInitializeLogging()
                    .disableSystemProperties()
                    .setOptionalConfigurationProperties(HoglakeFileSystemConfig.defaults(config))
                    .setRequiredConfigurationProperties(HoglakeFileSystemConfig.normalize(config))
                    .initialize();

            return new HoglakeConnector(
                    injector.getInstance(HoglakeMetadata.class),
                    new ClassLoaderSafeConnectorSplitManager(injector.getInstance(ConnectorSplitManager.class), classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(injector.getInstance(ConnectorPageSourceProvider.class), classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(injector.getInstance(ConnectorPageSinkProvider.class), classLoader),
                    injector.getInstance(LifeCycleManager.class),
                    injector.getInstance(new Key<Set<SessionPropertiesProvider>>() {}));
        }
    }

    private static boolean booleanProperty(Map<String, String> config, String property)
    {
        String value = config.get(property);
        if (value == null || value.equals("false")) {
            return false;
        }
        if (value.equals("true")) {
            return true;
        }
        throw new IllegalArgumentException("%s must be exactly true or false".formatted(property));
    }

    /**
     * The read path's file system is S3 with no credential of its own: a file is
     * read only with the storage session of the caller's verified token. So the
     * catalog may say where S3 is and nothing else about it: a key, a role, a
     * security mapping, a cache or another file system would each be a second
     * source of authority or a store shared between callers.
     */
    private static Map<String, String> sessionOnlyS3Properties(Map<String, String> config, String modeProperty)
    {
        for (String name : config.keySet()) {
            boolean storage = name.startsWith("s3.") || name.startsWith("fs.") || name.startsWith("hoglake.s3.") ||
                    name.startsWith("gcs.") || name.startsWith("azure.") || name.startsWith("hive.") || name.startsWith("local.");
            if (storage && !SESSION_ONLY_S3_PROPERTIES.contains(name)) {
                throw new IllegalArgumentException("Configuration property '%s' cannot be set with %s".formatted(name, modeProperty));
            }
        }
        String region = config.get("s3.region");
        if (region == null || region.isBlank()) {
            throw new IllegalArgumentException("s3.region is required with %s".formatted(modeProperty));
        }
        return ImmutableMap.<String, String>builder()
                .putAll(config)
                .put("fs.s3.enabled", "true")
                .buildOrThrow();
    }

    private static class SharedModule
            implements Module
    {
        private final Optional<String> storageRegion;
        private final String trinoVersion;
        private final PageSorter pageSorter;

        private SharedModule(Optional<String> storageRegion, String trinoVersion, PageSorter pageSorter)
        {
            this.storageRegion = storageRegion;
            this.trinoVersion = trinoVersion;
            this.pageSorter = pageSorter;
        }

        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(HoglakeSharedConfig.class);
        }

        @Provides
        @Singleton
        public HoglakeSharedSessionVerifier verifier(HoglakeSharedConfig config)
        {
            return new HoglakeSharedSessionVerifier(config, storageRegion);
        }

        @Provides
        @Singleton
        public HoglakeSharedHttpClient client(HoglakeSharedConfig config, HoglakeSharedSessionVerifier verifier)
        {
            return new HoglakeSharedHttpClient(config, verifier);
        }

        @Provides
        @Singleton
        public HoglakeSharedConnector connector(
                HoglakeSharedConfig config,
                HoglakeSharedHttpClient client,
                HoglakeSharedSessionVerifier verifier,
                LifeCycleManager lifeCycleManager,
                Injector injector)
        {
            Optional<HoglakeSharedReadPath> readPath = Optional.empty();
            Optional<HoglakeDedicatedReadPath> readSessions = Optional.empty();
            if (config.getReadSessionProfile().isPresent() != !config.getReadSessionRemoteTargets().isEmpty()
                    || (config.getReadSessionProfile().isPresent() && storageRegion.isEmpty())
                    || (config.isReadSessionLocalRead() && config.getReadSessionProfile().isEmpty())
                    || (config.isWriteSessionLocalWrite() && !config.isReadSessionLocalRead())) {
                throw new IllegalArgumentException("Invalid shared Hoglake read session configuration");
            }
            if (storageRegion.isPresent()) {
                // Only the read path's bootstrap binds a file system
                TrinoFileSystemFactory fileSystemFactory = injector.getInstance(TrinoFileSystemFactory.class);
                readPath = Optional.of(new HoglakeSharedReadPath(config, verifier, fileSystemFactory));
                if (config.getReadSessionProfile().isPresent()) {
                    // The same verifier, issuer, keys and limits as the dedicated catalog. Its grants
                    // bound every file; its remote targets are the only ones this catalog reads.
                    HoglakeReadSessionVerifier readSessionVerifier = new HoglakeReadSessionVerifier(
                            config.getIssuer(),
                            config.getJwksFile().toPath(),
                            config.getReadSessionProfile().orElseThrow(),
                            config.getReadSessionRemoteTargets(),
                            verifier.hogliteBackends(),
                            storageRegion,
                            config.getClockSkew().roundTo(SECONDS),
                            config.getScopedMaxLifetime().roundTo(SECONDS),
                            Clock.systemUTC(),
                            System::nanoTime);
                    readSessions = Optional.of(config.isReadSessionLocalRead()
                            ? HoglakeDedicatedReadPath.forSharedReadSession(
                            config.getUri(),
                            Duration.ofNanos(config.getRequestTimeout().roundTo(NANOSECONDS)),
                            readSessionVerifier,
                            fileSystemFactory,
                            config.getReadSessionRemoteTargets(),
                            config.isWriteSessionLocalWrite(),
                            trinoVersion,
                            pageSorter)
                            : HoglakeDedicatedReadPath.forRemoteTeamSchema(
                            config.getUri(),
                            Duration.ofNanos(config.getRequestTimeout().roundTo(NANOSECONDS)),
                            readSessionVerifier,
                            fileSystemFactory,
                            config.getReadSessionRemoteTargets()));
                }
            }
            return new HoglakeSharedConnector(client, verifier, lifeCycleManager, readPath, readSessions);
        }
    }
}
