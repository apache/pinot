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
package org.apache.pinot.common.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.common.utils.tls.ZooKeeperSslContextSupplier;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServiceStartableUtils {
  private ServiceStartableUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceStartableUtils.class);
  private static final String CLUSTER_CONFIG_ZK_PATH_TEMPLATE = "/%s/CONFIGS/CLUSTER/%s";
  private static final String PINOT_ALL_CONFIG_KEY_PREFIX = "pinot.all.";
  private static final String PINOT_INSTANCE_CONFIG_KEY_PREFIX_TEMPLATE = "pinot.%s.";
  protected static String _timeZone;

  /// Applies the ZK cluster config to:
  /// - The given instance config if it does not already exist.
  /// - Set the timezone.
  /// - Initialize the default values in [ForwardIndexConfig].
  ///
  /// In the ZK cluster config:
  /// - pinot.all.\* will be replaced to role specific config, e.g. pinot.controller.\* for controllers
  public static void applyClusterConfig(PinotConfiguration instanceConfig, String zkAddress, String clusterName,
      ServiceRole serviceRole) {
    ZooKeeperSslContextSupplier.initialize();
    int zkClientSessionConfig =
        instanceConfig.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_SESSION_TIMEOUT_MS);
    int zkClientConnectionTimeoutMs =
        instanceConfig.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_CONNECT_TIMEOUT_MS);
    ZkClient zkClient = new ZkClient.Builder()
        .setZkSerializer(new ZNRecordSerializer())
        .setZkServer(zkAddress)
        .setConnectionTimeout(zkClientConnectionTimeoutMs)
        .setSessionTimeout(zkClientSessionConfig)
        .build();
    zkClient.waitUntilConnected(zkClientConnectionTimeoutMs, TimeUnit.MILLISECONDS);

    try {
      ZNRecord clusterConfigZNRecord =
          zkClient.readData(String.format(CLUSTER_CONFIG_ZK_PATH_TEMPLATE, clusterName, clusterName), true);
      if (clusterConfigZNRecord == null) {
        applyIngestionGroovyConfig(instanceConfig, serviceRole, Map.of());
        LOGGER.warn("Failed to find cluster config for cluster: {}, skipping applying cluster config", clusterName);
        setTimezone(instanceConfig);
        initForwardIndexConfig(instanceConfig);
        initFieldSpecConfig(instanceConfig);
        return;
      }

      Map<String, String> clusterConfigs = clusterConfigZNRecord.getSimpleFields();
      applyIngestionGroovyConfig(instanceConfig, serviceRole, clusterConfigs);
      String instanceConfigKeyPrefix =
          String.format(PINOT_INSTANCE_CONFIG_KEY_PREFIX_TEMPLATE, serviceRole.name().toLowerCase());
      for (Map.Entry<String, String> entry : clusterConfigs.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key.startsWith(PINOT_ALL_CONFIG_KEY_PREFIX)) {
          String instanceConfigKey = instanceConfigKeyPrefix + key.substring(PINOT_ALL_CONFIG_KEY_PREFIX.length());
          addConfigIfNotExists(instanceConfig, instanceConfigKey, value);
        } else {
          // TODO: Currently it puts all keys to the instance config. Consider standardizing instance config keys and
          //       only put keys with the instance config key prefix.
          addConfigIfNotExists(instanceConfig, key, value);
        }
      }
    } finally {
      ZkStarter.closeAsync(zkClient);
    }
    setTimezone(instanceConfig);
    initForwardIndexConfig(instanceConfig);
    initFieldSpecConfig(instanceConfig);
    initRequestUtilsConfig(instanceConfig);
  }

  private static void applyIngestionGroovyConfig(PinotConfiguration instanceConfig, ServiceRole serviceRole,
      Map<String, String> clusterConfigs) {
    if (serviceRole == ServiceRole.CONTROLLER) {
      instanceConfig.setProperty(CommonConstants.Groovy.DISABLE_INGESTION_GROOVY,
          CommonConstants.Groovy.isIngestionGroovyDisabled(
              clusterConfigs.get(CommonConstants.Groovy.DISABLE_INGESTION_GROOVY)));
    }
  }

  private static void addConfigIfNotExists(PinotConfiguration instanceConfig, String key, String value) {
    if (!instanceConfig.containsKey(key)) {
      instanceConfig.setProperty(key, value);
    }
  }

  private static void setTimezone(PinotConfiguration instanceConfig) {
    TimeZone localTimezone = TimeZone.getDefault();
    _timeZone = instanceConfig.getProperty(CommonConstants.CONFIG_OF_TIMEZONE, localTimezone.getID());
    System.setProperty("user.timezone", _timeZone);
    LOGGER.info("Timezone: {}", _timeZone);
  }

  @VisibleForTesting
  static void initForwardIndexConfig(PinotConfiguration instanceConfig) {
    String defaultRawIndexWriterVersion =
        instanceConfig.getProperty(CommonConstants.ForwardIndexConfigs.CONFIG_OF_DEFAULT_RAW_INDEX_WRITER_VERSION);
    if (defaultRawIndexWriterVersion != null) {
      LOGGER.info("Setting forward index default raw index writer version to: {}", defaultRawIndexWriterVersion);
      ForwardIndexConfig.setDefaultRawIndexWriterVersion(Integer.parseInt(defaultRawIndexWriterVersion));
    }
    String defaultTargetMaxChunkSize =
        instanceConfig.getProperty(CommonConstants.ForwardIndexConfigs.CONFIG_OF_DEFAULT_TARGET_MAX_CHUNK_SIZE);
    if (defaultTargetMaxChunkSize != null) {
      LOGGER.info("Setting forward index default target max chunk size to: {}", defaultTargetMaxChunkSize);
      ForwardIndexConfig.setDefaultTargetMaxChunkSize(defaultTargetMaxChunkSize);
    }
    String defaultTargetDocsPerChunk =
        instanceConfig.getProperty(CommonConstants.ForwardIndexConfigs.CONFIG_OF_DEFAULT_TARGET_DOCS_PER_CHUNK);
    if (defaultTargetDocsPerChunk != null) {
      LOGGER.info("Setting forward index default target docs per chunk to: {}", defaultTargetDocsPerChunk);
      ForwardIndexConfig.setDefaultTargetDocsPerChunk(Integer.parseInt(defaultTargetDocsPerChunk));
    }
    String defaultCompressionCodec =
        instanceConfig.getProperty(CommonConstants.ForwardIndexConfigs.CONFIG_OF_DEFAULT_COMPRESSION_CODEC);
    if (defaultCompressionCodec != null) {
      setDefaultCompressionCodec(defaultCompressionCodec);
    }
  }

  /// Applies the cluster-wide default compression codec for raw forward indexes.
  ///
  /// The value is a [CompressionCodec], the same spelling used by `compressionCodec` in a table config,
  /// so that an operator setting a cluster default and a table author setting a column override write the
  /// same word. [ForwardIndexConfig#setDefaultCompressionCodec] decides what is acceptable and this method
  /// only parses, so the rejected set stays with the class that owns the default: the CLP family and
  /// `MV_ENTRY_DICT`, which name whole-index or dictionary-id formats rather than chunk codecs;
  /// `DELTA` and `DELTADELTA`, which are chunk codecs but are rejected by table config validation on every
  /// column and whose compressors throw on chunks that are not a multiple of 4 bytes; and `GZIP`, whose
  /// decompressor is not thread-safe.
  ///
  /// Unlike the numeric defaults above, an unusable value is logged and ignored instead of failing
  /// startup: this one is routinely set as a *cluster* config, where throwing would take down every
  /// component that restarts after the bad value is saved. The warning names the codecs that are
  /// accepted, so a rejected value is self-correcting from the log.
  ///
  /// Scope, which this being a *cluster* config does not make as wide as it sounds:
  ///
  /// - It is read once per JVM at startup, so after changing it a rolling restart leaves restarted
  ///   instances writing the new codec while the rest keep the old one until they restart too. Each
  ///   segment records the codec it was written with, so this mixes formats without breaking reads.
  /// - It reaches only components that call [#applyClusterConfig]: the controller, broker, server and
  ///   minion. Segments built outside the cluster -- standalone, Spark or Hadoop batch ingestion, and
  ///   `pinot-admin CreateSegment` -- keep the compiled-in default. A reload ignores the default when
  ///   *deciding* whether to rewrite a forward index, so nothing reconciles the two on its own; but a
  ///   rewrite triggered for another reason -- a dictionary-to-raw conversion, codecSpec removal,
  ///   re-enabling a disabled forward index, a new derived or default column -- does write the reloading
  ///   component's current default.
  /// - It is a process-global default, so in a single-JVM multi-role deployment the last role to start
  ///   wins for the whole process.
  ///
  /// The first two hold for the three forward index defaults above as well; they are spelled out here
  /// because this is the first of them an operator is routinely expected to set. The third is worse for
  /// those: under a parallel bootstrap the roles race, and unlike this one their statics are not
  /// `volatile`.
  @VisibleForTesting
  static void setDefaultCompressionCodec(String defaultCompressionCodec) {
    CompressionCodec codec;
    try {
      codec = CompressionCodec.valueOf(defaultCompressionCodec.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unknown forward index default compression codec: {}, expected one of: {}, keeping: {}",
          defaultCompressionCodec, ForwardIndexConfig.getAcceptableDefaultCompressionCodecs(),
          ForwardIndexConfig.getDefaultCompressionType());
      return;
    }
    try {
      ForwardIndexConfig.setDefaultCompressionCodec(codec);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Rejected forward index default compression codec: {} ({}), expected one of: {}, keeping: {}",
          codec, e.getMessage(), ForwardIndexConfig.getAcceptableDefaultCompressionCodecs(),
          ForwardIndexConfig.getDefaultCompressionType());
      return;
    }
    LOGGER.info("Set forward index default compression codec to: {}", ForwardIndexConfig.getDefaultCompressionType());
  }

  public static void initFieldSpecConfig(PinotConfiguration instanceConfig) {
    String defaultJsonSanitizationStrategy =
        instanceConfig.getProperty(CommonConstants.FieldSpecConfigs.CONFIG_OF_DEFAULT_JSON_MAX_LENGTH_EXCEED_STRATEGY);
    if (defaultJsonSanitizationStrategy != null) {
      try {
        FieldSpec.MaxLengthExceedStrategy strategy =
            FieldSpec.MaxLengthExceedStrategy.valueOf(defaultJsonSanitizationStrategy);
        LOGGER.info("Setting default JSON sanitization strategy to: {}", defaultJsonSanitizationStrategy);
        FieldSpec.setDefaultJsonMaxLengthExceedStrategy(strategy);
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Invalid default JSON sanitization strategy: {}, using default: {}",
            defaultJsonSanitizationStrategy, FieldSpec.getDefaultJsonMaxLengthExceedStrategy());
      }
    }

    String defaultJsonMaxLength =
        instanceConfig.getProperty(CommonConstants.FieldSpecConfigs.CONFIG_OF_DEFAULT_JSON_MAX_LENGTH);
    if (defaultJsonMaxLength != null) {
      try {
        int maxLength = Integer.parseInt(defaultJsonMaxLength);
        LOGGER.info("Setting default JSON max length to: {}", defaultJsonMaxLength);
        FieldSpec.setDefaultJsonMaxLength(maxLength);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid default JSON max length: {}, using default: {}",
            defaultJsonMaxLength, FieldSpec.getDefaultJsonMaxLength());
      }
    }
  }

  public static void initRequestUtilsConfig(PinotConfiguration instanceConfig) {
    boolean useLegacyLiteralUnescaping =
        instanceConfig.getProperty(CommonConstants.Helix.CONFIG_OF_SSE_LEGACY_LITERAL_UNESCAPING,
            CommonConstants.Helix.DEFAULT_SSE_LEGACY_LITERAL_UNESCAPING);
    RequestUtils.setUseLegacyLiteralUnescaping(useLegacyLiteralUnescaping);
  }
}
