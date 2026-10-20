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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.tls.PinotInsecureMode;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.utils.ClusterConfigForTable;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.unsafe.MmapMemoryConfig;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.PinotMd5Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Owns the installation-only native branch of the normal server starter. Initialization validates local bootstrap
/// inputs; start and close serialize ownership of metadata channels, participant, storage factories, and metrics.
/// No Helix client, server admin endpoint, query listener, or query-serving capability is created.
public final class NativeServerStarterLifecycle implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeServerStarterLifecycle.class);
  private static final String PREFIX = "pinot.coordination.ratis.server.";
  private final PinotConfiguration _config;
  private final NativeMetadataConfig _metadataConfig;
  private final NativeServerParticipant.Settings _settings;
  @Nullable
  private NativeServerParticipant _participant;
  @Nullable
  private ServerMetrics _metrics;
  @Nullable
  private ServerInstance _instance;
  private boolean _started;
  private boolean _closed;
  private boolean _storageInitialized;
  @Nullable
  private ServiceStatus.ServiceStatusCallback _statusCallback;

  public NativeServerStarterLifecycle(PinotConfiguration config) {
    _config = config.clone();
    _metadataConfig = new NativeMetadataConfig(_config);
    String instanceId = _config.getProperty(Server.CONFIG_OF_INSTANCE_ID);
    checkArgument(_metadataConfig.participantId().equals(instanceId),
        "Native server requires an explicit instance ID matching pinot.coordination.ratis.participantId");
    requireDirectory(_config, Server.CONFIG_OF_INSTANCE_DATA_DIR);
    requireDirectory(_config, Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR);
    checkArgument(new ServerConf(_config).getInstanceDataManagerClassName()
            .equals(Server.DEFAULT_INSTANCE_DATA_MANAGER_CLASS),
        "Custom instance data manager classes are not supported by the native starter");
    _settings = new NativeServerParticipant.Settings(_metadataConfig.historyId(), instanceId, Map.of(),
        _metadataConfig.rpcTimeoutMillis(), _metadataConfig.heartbeatMillis(),
        _config.getProperty(PREFIX + "snapshotMillis", 1000L),
        _config.getProperty(PREFIX + "actionThreads", 1), _config.getProperty(PREFIX + "queuedActions", 128));
  }

  private static void requireDirectory(PinotConfiguration config, String key) {
    String value = config.getProperty(key);
    checkArgument(value != null && !value.isBlank(), "Native server requires an explicit durable directory: %s", key);
    checkArgument(Path.of(value).isAbsolute(), "Native server directory must be absolute: %s", key);
  }

  public String instanceId() {
    return _settings.participantId();
  }

  public synchronized void start(SegmentOperationsThrottlerSet throttlers) throws Exception {
    checkState(!_started && !_closed, "Native starter is already started or closed");
    _started = true;
    try {
      LOGGER.info("Starting installation-only Ratis server: {}; admin and query listeners are disabled", instanceId());
      ServerConf serverConf = new ServerConf(_config);
      _metrics = ServerMetricsInitUtils.initServerMetrics(serverConf);
      _metrics.setValueOfGlobalGauge(ServerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
      throttlers.initializeMetrics();
      PinotInsecureMode.setPinotInInsecureMode(
          _config.getProperty(CommonConstants.CONFIG_OF_PINOT_INSECURE_MODE, false));
      PinotMd5Mode.setPinotMd5Disabled(_config.getProperty(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED,
          PinotMd5Mode.isPinotMd5Disabled()));
      String codec = _config.getProperty(CommonConstants.CONFIG_OF_PINOT_TAR_COMPRESSION_CODEC_NAME);
      if (codec != null) {
        TarCompressionUtils.setDefaultCompressor(codec);
      }
      String mmapAdvice = _config.getProperty(Server.CONFIG_OF_MMAP_DEFAULT_ADVICE);
      if (mmapAdvice != null) {
        MmapMemoryConfig.setDefaultAdvice(mmapAdvice);
      }
      PinotDataBuffer.loadDefaultFactory(_config);
      LOGGER.info("Loaded {} index types", IndexService.getInstance().getAllIndexes().size());
      _storageInitialized = true;
      initializeSegmentFetching(_config);
      ServerReloadJobStatusCache jobs = new ServerReloadJobStatusCache(instanceId());
      NativeMetadataConnection membership = connection();
      NativeMetadataConnection work;
      try {
        work = connection();
      } catch (Exception e) {
        membership.close();
        throw e;
      }
      try {
        _participant = new NativeServerParticipant(_settings, membership, work);
      } catch (RuntimeException e) {
        work.close();
        membership.close();
        throw e;
      }
      _participant.clusterConfig().registerClusterConfigChangeListener(
          new TextIndexUtils.LuceneMaxClauseCountConfigChangeListener());
      _participant.clusterConfig().registerClusterConfigChangeListener(
          new ClusterConfigForTable.ConfigChangeListener());
      _participant.clusterConfig().registerClusterConfigChangeListener(jobs);
      _participant.clusterConfig().registerClusterConfigChangeListener(throttlers);
      _participant.start(serverConf.getInstanceDataManagerConfig(), _metrics, throttlers, jobs);
      _instance = new ServerInstance(serverConf, instanceId(), _participant.dataManager(), jobs);
      _statusCallback = new ServiceStatus.ServiceStatusCallback() {
        @Override
        public ServiceStatus.Status getServiceStatus() {
          return _participant.isReadyForInstallations() ? ServiceStatus.Status.GOOD : ServiceStatus.Status.BAD;
        }

        @Override
        public String getStatusDescription() {
          return "Native installations only; query listeners disabled"
              + (_participant.failure() == null ? "" : ": " + _participant.failure());
        }
      };
      ServiceStatus.setServiceStatusCallback(instanceId(), _statusCallback);
    } catch (Exception e) {
      try {
        close();
      } catch (RuntimeException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  private NativeMetadataConnection connection() throws IOException {
    return new NativeMetadataConnection(_metadataConfig.endpoints(), _metadataConfig.certificate(),
        _metadataConfig.privateKey(), _metadataConfig.trustCertificates(), _metadataConfig.rpcTimeoutMillis());
  }

  public synchronized ServerInstance serverInstance() {
    return requireNonNull(_instance, "Native server is not started");
  }

  /// Shared with the Helix starter so both paths initialize the existing filesystem, fetcher and crypter plugins.
  public static void initializeSegmentFetching(PinotConfiguration config) throws Exception {
    PinotFSFactory.init(config.subset(Server.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY));
    SegmentFetcherFactory.init(config.subset(Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY));
    PinotCrypterFactory.init(config.subset(Server.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER));
  }

  @Override
  public synchronized void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    if (_statusCallback != null) {
      ServiceStatus.removeServiceStatusCallback(instanceId(), _statusCallback);
      _statusCallback = null;
    }
    if (_participant != null) {
      // Drain actions before releasing shared storage resources. A failed drain must retain those resources.
      _participant.close();
    }
    try {
      if (_storageInitialized) {
        PinotFSFactory.shutdown();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to close native storage factories", e);
    } finally {
      if (_metrics != null) {
        _metrics.getMetricsRegistry().shutdown();
      }
    }
  }
}
