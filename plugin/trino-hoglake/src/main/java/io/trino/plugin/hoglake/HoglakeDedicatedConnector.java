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

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Default-off read-only connector for dedicated local Hoglake table reads.
 * Every metadata request and storage open verifies the caller's read session token.
 */
public final class HoglakeDedicatedConnector
        implements Connector
{
    private final HoglakeDedicatedReadPath readPath;
    private final HoglakeReadSessionVerifier verifier;
    private final Runnable shutdownHook;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;

    public HoglakeDedicatedConnector(
            HoglakeDedicatedReadPath readPath,
            HoglakeReadSessionVerifier verifier,
            LifeCycleManager lifeCycleManager)
    {
        this(readPath, verifier, requireNonNull(lifeCycleManager, "lifeCycleManager is null")::stop);
    }

    HoglakeDedicatedConnector(
            HoglakeDedicatedReadPath readPath,
            HoglakeReadSessionVerifier verifier,
            Runnable shutdownHook)
    {
        this.readPath = requireNonNull(readPath, "readPath is null");
        this.verifier = requireNonNull(verifier, "verifier is null");
        this.shutdownHook = requireNonNull(shutdownHook, "shutdownHook is null");
        this.sessionProperties = new HoglakeSessionProperties(new HoglakeConfig()).getSessionProperties();
        ClassLoader classLoader = getClass().getClassLoader();
        this.splitManager = new ClassLoaderSafeConnectorSplitManager(readPath.splitManager(), classLoader);
        this.pageSourceProvider = new ClassLoaderSafeConnectorPageSourceProvider(readPath.pageSourceProvider(), classLoader);
        this.pageSinkProvider = new ClassLoaderSafeConnectorPageSinkProvider(new DenyingPageSinkProvider(), classLoader);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return new HoglakeTransactionHandle(UUID.randomUUID());
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return readPath.metadata(session, transactionHandle);
    }

    @Override
    public void commit(ConnectorTransactionHandle handle)
    {
        readPath.end(handle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle handle)
    {
        readPath.end(handle);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public void shutdown()
    {
        try {
            readPath.close();
        }
        finally {
            shutdownHook.run();
        }
    }

    private TrinoException denyWrite(ConnectorSession session)
    {
        verifier.verify(session);
        return new TrinoException(NOT_SUPPORTED, "Dedicated Hoglake local read is read-only");
    }

    private final class DenyingPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorOutputTableHandle outputTableHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw denyWrite(session);
        }

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorInsertTableHandle insertTableHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw denyWrite(session);
        }

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorTableExecuteHandle tableExecuteHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw denyWrite(session);
        }

        @Override
        public ConnectorMergeSink createMergeSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorMergeTableHandle mergeHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId,
                MemoryContext memoryContext)
        {
            throw denyWrite(session);
        }
    }
}
