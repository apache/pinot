/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.starter;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Authority;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Result;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Status;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.common.coordination.MetadataRpcClient;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.core.data.manager.LogicalTableContext;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Native OFFLINE table lifecycle using the normal Pinot data manager and conditional physical installer.
/// Metadata providers must be bound to the current table incarnation and server registration. This class has no
/// Helix connection and grants no query-serving authority; legacy unfenced mutations fail explicitly.
public final class NativeInstanceDataManager extends BaseInstanceDataManager {
  // Avoid opening a second channel to an already locked inode in this JVM: on some systems closing that channel
  // would release process-wide file locks held by another channel. OS locking still excludes other processes.
  private static final Set<Path> OWNED_DIRECTORIES = ConcurrentHashMap.newKeySet();
  private HelixInstanceDataManagerConfig _config;
  private Function<String, TableDataManagerMetadata> _metadata;
  private SegmentOperationsThrottlerSet _throttlers;
  private ServerReloadJobStatusCache _reloadJobStatusCache;
  private final ReentrantReadWriteLock _lifecycleAdmission = new ReentrantReadWriteLock();
  private final Phaser _actions = new Phaser(1);
  @Nullable
  private Path _lockedDirectory;
  @Nullable
  private FileChannel _lockChannel;
  @Nullable
  private FileLock _directoryLock;
  @Nullable
  private InstallationOutbox _installationOutbox;
  private boolean _started;
  private boolean _closed;

  /// Initializes ordinary OFFLINE support using the existing component configuration parser, without a Helix manager.
  public synchronized void initWithMetadata(PinotConfiguration config,
      Function<String, TableDataManagerMetadata> metadata, ServerMetrics serverMetrics,
      @Nullable SegmentOperationsThrottlerSet throttlers, ServerReloadJobStatusCache reloadJobStatusCache)
      throws Exception {
    checkState(_config == null && !_closed, "Native instance data manager is already initialized or closed");
    HelixInstanceDataManagerConfig parsed = new HelixInstanceDataManagerConfig(config);
    checkArgument(DefaultTableDataManagerProvider.class.getName().equals(parsed.getTableDataManagerProviderClass()),
        "Native table data manager plugins are not supported");
    checkArgument(SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME.equals(
        parsed.getSegmentDirectoryLoader()), "Native installation requires the default local segment loader");
    checkArgument(parsed.getSegmentPeerDownloadScheme() == null, "Native peer download is not supported");
    checkArgument(!parsed.isAsyncSegmentRefreshEnabled(), "Native actions do not use unfenced async refresh");
    _metadata = requireNonNull(metadata, "metadata");
    requireNonNull(serverMetrics, "serverMetrics");
    _reloadJobStatusCache = requireNonNull(reloadJobStatusCache, "reloadJobStatusCache");
    _throttlers = throttlers;
    createWritableDirectory(Path.of(parsed.getInstanceDataDir()));
    try {
      acquireDirectory(Path.of(parsed.getInstanceDataDir()));
      createWritableDirectory(Path.of(parsed.getInstanceSegmentTarDir()));
      initLifecycleResources(parsed.getMaxParallelRefreshThreads(), 0, parsed.getErrorCacheSize());
      _config = parsed;
    } catch (Exception e) {
      shutDownManagedTables();
      try {
        releaseDirectory();
      } catch (IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  private void acquireDirectory(Path directory) throws IOException {
    Path canonical = directory.toRealPath();
    if (!OWNED_DIRECTORIES.add(canonical)) {
      throw new IOException("Native instance data directory is already owned: " + canonical);
    }
    _lockedDirectory = canonical;
    _lockChannel = FileChannel.open(canonical.resolve(".native-instance.lock"),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    _directoryLock = _lockChannel.tryLock();
    if (_directoryLock == null) {
      throw new IOException("Native instance data directory is locked by another process: " + canonical);
    }
  }

  private void releaseDirectory() throws IOException {
    if (_lockChannel != null) {
      // Keep the JVM reservation and channel references if close fails: ownership is then uncertain and must not
      // be handed to another local manager by opening another channel to the same inode.
      _lockChannel.close();
    }
    _lockChannel = null;
    _directoryLock = null;
    if (_lockedDirectory != null) {
      OWNED_DIRECTORIES.remove(_lockedDirectory);
      _lockedDirectory = null;
    }
  }

  private static void createWritableDirectory(Path directory) throws IOException {
    Files.createDirectories(directory);
    Path probe = Files.createTempFile(directory, ".pinot-writability-check-", ".tmp");
    Files.delete(probe);
  }

  /// A bounded operation's ownership of the native data directory. Shutdown cannot release the process lock until
  /// all leases close. The path must not be used after closing the lease.
  public static final class DirectoryLease implements AutoCloseable {
    private final Path _directory;
    private final Phaser _actions;
    private final AtomicBoolean _released = new AtomicBoolean();

    private DirectoryLease(Path directory, Phaser actions) {
      _directory = directory;
      _actions = actions;
    }

    public Path directory() {
      checkState(!_released.get(), "Directory ownership lease is closed");
      return _directory;
    }

    @Override
    public void close() {
      if (_released.compareAndSet(false, true)) {
        _actions.arriveAndDeregister();
      }
    }
  }

  public synchronized DirectoryLease acquireDirectoryOwnership() throws IOException {
    if (_closed || _directoryLock == null || !_directoryLock.isValid() || _lockedDirectory == null) {
      throw new IOException("Native instance does not own its data directory");
    }
    _actions.register();
    return new DirectoryLease(_lockedDirectory, _actions);
  }

  public InstallationOutbox openInstallationOutbox(MetadataRpc.ParticipantToken server, MetadataRpcClient rpc,
      long timeoutMillis) throws IOException {
    requireNonNull(rpc, "rpc");
    return openInstallationOutbox(server, new InstallationOutbox.Rpc() {
      @Override
      public MetadataRpc.CommandResult execute(MetadataRpc.InstallationCommandRequest request, long timeout)
          throws Exception {
        return rpc.installationExecute(request, timeout);
      }

      @Override
      public MetadataRpc.CommandResult outcome(MetadataRpc.InstallationOutcomeRequest request, long timeout)
          throws Exception {
        return rpc.installationOutcome(request, timeout);
      }
    }, timeoutMillis);
  }

  /// Opens the sole report stream for this SERVER session. A newer registration retires old references and retains
  /// their files as evidence; it never replays old reports under the new authenticated session.
  public synchronized InstallationOutbox openInstallationOutbox(MetadataRpc.ParticipantToken server,
      InstallationOutbox.Rpc rpc, long timeoutMillis) throws IOException {
    if (_closed) {
      throw new IOException("Native instance data manager is closed");
    }
    InstallationOutbox.validateServer(requireNonNull(server, "server"));
    if (_installationOutbox != null) {
      MetadataRpc.ParticipantToken old = _installationOutbox.server();
      if (old.equals(server)) {
        return _installationOutbox;
      }
      checkArgument(old.getHistoryId().equals(server.getHistoryId())
          && old.getParticipantId().equals(server.getParticipantId())
          && server.getRegistrationEpoch() > old.getRegistrationEpoch()
          && !old.getIncarnation().equals(server.getIncarnation()), "Outbox SERVER session did not advance");
      _installationOutbox.retire();
    }
    _installationOutbox = new InstallationOutbox(this, server, rpc, timeoutMillis);
    return _installationOutbox;
  }

  @Override
  public void init(PinotConfiguration config, HelixManager manager, ServerMetrics metrics,
      @Nullable SegmentOperationsThrottlerSet throttlers, ServerReloadJobStatusCache reloadJobStatusCache) {
    throw unsupported("Helix initialization; use initWithMetadata");
  }

  @Override
  public synchronized void start() {
    checkState(_config != null && !_closed && _directoryLock != null && _directoryLock.isValid(),
        "Native instance data manager is uninitialized, closed or lacks directory ownership");
    _started = true;
  }

  @Override
  public void shutDown() {
    synchronized (this) {
      if (_closed) {
        return;
      }
      _closed = true;
      _started = false;
    }
    _lifecycleAdmission.writeLock().lock();
    try {
      shutDownManagedTables();
    } finally {
      _lifecycleAdmission.writeLock().unlock();
    }
    // Preparation and authority refresh run outside table locks. They must also drain before another process can
    // acquire the directory and act on the same durable installation pointers.
    _actions.arriveAndAwaitAdvance();
    try {
      releaseDirectory();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to release native instance directory ownership", e);
    }
  }

  /// Executes explicit expected identities and local authority; successful installation still requires a committed
  /// receipt before a controller can complete the operation.
  public Result execute(Request request, Authority authority) throws Exception {
    requireNonNull(request, "request");
    return executor(request.identity().tableName()).execute(request, authority);
  }

  /// Returns the conditional installer for one table. It never exposes legacy segment mutation APIs.
  public NativeOfflineSegmentAction.Executor executor(String tableName) {
    OfflineTableDataManager table;
    _lifecycleAdmission.readLock().lock();
    try {
      synchronized (this) {
        checkState(_started && !_closed, "Native instance data manager is not running");
      }
      checkArgument(TableNameBuilder.isOfflineTableResource(tableName), "Native tables must be OFFLINE");
      table = (OfflineTableDataManager) getOrCreateManagedTable(tableName, name -> {
        OfflineTableDataManager created = new OfflineTableDataManager();
        TableDataManagerMetadata source = requireNonNull(_metadata.apply(name), "table metadata");
        try {
          created.initWithMetadata(_config, source, _segmentLocks, name, _segmentReloadSemaphore,
              _segmentReloadRefreshExecutor, null, _errorCache, _throttlers, false, _reloadJobStatusCache);
          created.start();
        } catch (RuntimeException e) {
          try {
            created.shutDown();
          } catch (RuntimeException cleanup) {
            e.addSuppressed(cleanup);
          }
          throw e;
        }
        return created;
      });
    } finally {
      _lifecycleAdmission.readLock().unlock();
    }
    NativeOfflineSegmentAction.Executor delegate = table.nativeExecutor();
    return new NativeOfflineSegmentAction.Executor() {
      @Override
      public Result execute(Request request, Authority authority) throws Exception {
        checkArgument(request.identity().tableName().equals(tableName)
            && request.identity().server().participantId().equals(getInstanceId()),
            "Native request belongs to another table or server");
        synchronized (NativeInstanceDataManager.this) {
          if (_closed) {
            return new Result(Status.FENCED, null);
          }
          _actions.register();
        }
        try {
          return delegate.execute(request, authority);
        } finally {
          _actions.arriveAndDeregister();
        }
      }

      @Nullable
      @Override
      public Installation installed(String segmentName) throws IOException {
        return delegate.installed(segmentName);
      }
    };
  }

  @Override
  public Set<String> getAllTables() {
    return Set.copyOf(super.getAllTables());
  }

  @Override
  public String getInstanceId() {
    return _config.getInstanceId();
  }

  @Override
  public String getInstanceDataDir() {
    return _config.getInstanceDataDir();
  }

  @Override
  public String getSegmentFileDirectory() {
    return _config.getInstanceSegmentTarDir();
  }

  @Override
  public int getMaxParallelRefreshThreads() {
    return _config.getMaxParallelRefreshThreads();
  }

  @Override
  public boolean isAsyncSegmentRefreshEnabled() {
    return false;
  }

  @Override
  public File getSegmentDataDirectory(String tableName, String segmentName) {
    TableDataManager table = getTableDataManager(tableName);
    checkState(table != null, "Native table is not loaded: %s", tableName);
    SegmentDataManager segment = table.acquireSegment(segmentName);
    checkState(segment != null, "Native segment is not loaded: %s", segmentName);
    try {
      return segment.getSegment().getSegmentMetadata().getIndexDir();
    } finally {
      table.releaseSegment(segment);
    }
  }

  @Override
  public void deleteTable(String tableName, long deletionTimeMs) {
    throw unsupported("Unfenced table deletion");
  }

  @Override
  public void addOnlineSegment(String tableName, String segmentName) {
    throw unsupported("Unfenced segment load");
  }

  @Override
  public void addConsumingSegment(String tableName, String segmentName) {
    throw unsupported("Realtime ingestion");
  }

  @Override
  public void replaceSegment(String tableName, String segmentName) {
    throw unsupported("Unfenced segment replacement");
  }

  @Override
  public void offloadSegment(String tableName, String segmentName) {
    throw unsupported("Unfenced segment offload");
  }

  @Override
  public void deleteSegment(String tableName, String segmentName) {
    throw unsupported("Unfenced segment deletion");
  }

  @Override
  public void reloadSegment(String tableName, String segmentName, boolean forceDownload, boolean onlyIfNeeded,
      String reloadJobId) {
    throw unsupported("Unfenced segment reload");
  }

  @Override
  public void reloadAllSegments(String tableName, boolean forceDownload, boolean onlyIfNeeded, String reloadJobId) {
    throw unsupported("Unfenced table reload");
  }

  @Override
  public void reloadSegments(String tableName, List<String> segments, boolean forceDownload, boolean onlyIfNeeded,
      String reloadJobId) {
    throw unsupported("Unfenced segment reload");
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    throw unsupported("Helix property store");
  }

  @Override
  public SegmentUploader getSegmentUploader() {
    throw unsupported("Realtime segment upload");
  }

  @Override
  public void forceCommit(String tableName, Set<String> segments) {
    throw unsupported("Realtime force commit");
  }

  @Override
  public void setSupplierOfIsServerReadyToConsumeData(BooleanSupplier readiness) {
    throw unsupported("Realtime readiness");
  }

  @Override
  public void setSupplierOfIsServerReadyToServeQueries(BooleanSupplier readiness) {
    throw unsupported("Query readiness without permit and content fencing");
  }

  @Override
  public List<File> getConsumerDirPaths() {
    throw unsupported("Realtime consumer directories");
  }

  @Override
  public LogicalTableContext getLogicalTableContext(String tableName) {
    throw unsupported("Logical tables");
  }

  private static UnsupportedOperationException unsupported(String operation) {
    return new UnsupportedOperationException(operation + " is not supported by the native OFFLINE instance manager");
  }
}
