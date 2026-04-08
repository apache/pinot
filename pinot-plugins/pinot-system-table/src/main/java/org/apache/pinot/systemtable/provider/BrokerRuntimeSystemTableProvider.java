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
package org.apache.pinot.systemtable.provider;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.systemtable.SystemTable;
import org.apache.pinot.common.systemtable.SystemTableProvider;
import org.apache.pinot.common.systemtable.SystemTableProviderContext;
import org.apache.pinot.common.systemtable.datasource.InMemorySystemTableSegment;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * System table that exposes broker-local runtime/JVM information across all live brokers.
 */
@SystemTable
public final class BrokerRuntimeSystemTableProvider implements SystemTableProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRuntimeSystemTableProvider.class);

  public static final String TABLE_NAME = "system.broker_runtime";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension("brokerId", FieldSpec.DataType.STRING)
      .addSingleValueDimension("host", FieldSpec.DataType.STRING)
      .addSingleValueDimension("port", FieldSpec.DataType.INT)
      .addMetric("startTimeMs", FieldSpec.DataType.LONG)
      .addMetric("uptimeMs", FieldSpec.DataType.LONG)
      .addMetric("jvmHeapUsedBytes", FieldSpec.DataType.LONG)
      .addMetric("jvmHeapMaxBytes", FieldSpec.DataType.LONG)
      .addMetric("jvmNonHeapUsedBytes", FieldSpec.DataType.LONG)
      .addMetric("threadCount", FieldSpec.DataType.INT)
      .addMetric("peakThreadCount", FieldSpec.DataType.INT)
      .addMetric("availableProcessors", FieldSpec.DataType.INT)
      .addMetric("systemLoadAverage", FieldSpec.DataType.DOUBLE)
      .addMetric("timestampMs", FieldSpec.DataType.LONG)
      .addSingleValueDimension("pinotVersion", FieldSpec.DataType.STRING)
      .build();

  @Nullable
  private HelixAdmin _helixAdmin;
  @Nullable
  private String _clusterName;
  @Nullable
  private PinotConfiguration _brokerConfig;

  public BrokerRuntimeSystemTableProvider() {
  }

  @Override
  public void init(SystemTableProviderContext context) {
    _helixAdmin = context.getHelixAdmin();
    _clusterName = context.getClusterName();
    _brokerConfig = context.getConfig();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.BROKER_SCATTER_GATHER;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public TableConfig getTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
  }

  @Override
  public IndexSegment getDataSource() {
    String brokerId = getBrokerId();
    HostPort hostPort = getHostPort(brokerId);

    long timestampMs = System.currentTimeMillis();
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    long startTimeMs = runtimeMXBean.getStartTime();
    long uptimeMs = runtimeMXBean.getUptime();

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
    long heapUsedBytes = heap.getUsed();
    long heapMaxBytes = heap.getMax();
    MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
    long nonHeapUsedBytes = nonHeap.getUsed();

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    int threadCount = threadMXBean.getThreadCount();
    int peakThreadCount = threadMXBean.getPeakThreadCount();

    OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
    int availableProcessors = osMxBean.getAvailableProcessors();
    double systemLoadAverage = Math.max(osMxBean.getSystemLoadAverage(), 0.0);

    Map<String, IntFunction<Object>> valueProviders = new HashMap<>();
    valueProviders.put("brokerId", docId -> brokerId);
    valueProviders.put("host", docId -> hostPort._host);
    valueProviders.put("port", docId -> hostPort._port);
    valueProviders.put("startTimeMs", docId -> startTimeMs);
    valueProviders.put("uptimeMs", docId -> uptimeMs);
    valueProviders.put("jvmHeapUsedBytes", docId -> heapUsedBytes);
    valueProviders.put("jvmHeapMaxBytes", docId -> heapMaxBytes);
    valueProviders.put("jvmNonHeapUsedBytes", docId -> nonHeapUsedBytes);
    valueProviders.put("threadCount", docId -> threadCount);
    valueProviders.put("peakThreadCount", docId -> peakThreadCount);
    valueProviders.put("availableProcessors", docId -> availableProcessors);
    valueProviders.put("systemLoadAverage", docId -> systemLoadAverage);
    valueProviders.put("timestampMs", docId -> timestampMs);
    valueProviders.put("pinotVersion", docId -> PinotVersion.VERSION);

    return new InMemorySystemTableSegment(TABLE_NAME, SCHEMA, 1, valueProviders);
  }

  private String getBrokerId() {
    if (_brokerConfig == null) {
      return "";
    }
    return _brokerConfig.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID, "");
  }

  private HostPort getHostPort(String brokerId) {
    if (_helixAdmin == null || _clusterName == null || brokerId.isEmpty()) {
      return getHostPortFromConfigOrInstanceId(brokerId);
    }
    try {
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_clusterName, brokerId);
      if (instanceConfig == null) {
        return getHostPortFromConfigOrInstanceId(brokerId);
      }
      URI baseUri = URI.create(InstanceUtils.getInstanceBaseUri(instanceConfig));
      return new HostPort(baseUri.getHost(), baseUri.getPort());
    } catch (Exception e) {
      LOGGER.debug("{}: error fetching broker instance config for {}", TABLE_NAME, brokerId, e);
      return getHostPortFromConfigOrInstanceId(brokerId);
    }
  }

  private HostPort getHostPortFromConfigOrInstanceId(String brokerId) {
    if (_brokerConfig != null) {
      String host = _brokerConfig.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_HOSTNAME, "");
      int port = _brokerConfig.getProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, -1);
      if (!host.isEmpty() && port > 0) {
        return new HostPort(host, port);
      }
    }

    if (brokerId.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
      String suffix = brokerId.substring(CommonConstants.Helix.BROKER_INSTANCE_PREFIX_LENGTH);
      int lastUnderscore = suffix.lastIndexOf('_');
      if (lastUnderscore > 0 && lastUnderscore + 1 < suffix.length()) {
        String host = suffix.substring(0, lastUnderscore);
        String portString = suffix.substring(lastUnderscore + 1);
        try {
          int port = Integer.parseInt(portString);
          return new HostPort(host, port);
        } catch (NumberFormatException ignored) {
          // fall through
        }
      }
    }
    return new HostPort("", -1);
  }

  private static final class HostPort {
    final String _host;
    final int _port;

    private HostPort(String host, int port) {
      _host = host;
      _port = port;
    }
  }
}
