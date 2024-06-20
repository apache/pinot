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
package org.apache.pinot.common.config.provider;

import com.google.common.base.Preconditions;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuotaConfigProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuotaConfigProvider.class);
  private static final String CONFIG_PATH = "/CONFIGS/CLUSTER/quota";

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private TableCache _tableCache;
  private final ZkQuotaConfigChangeListener _configChangeListener = new ZkQuotaConfigChangeListener();
  private final Properties _metaConfigs = new Properties();

  public QuotaConfigProvider(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    synchronized (_configChangeListener) {
      _propertyStore.subscribeDataChanges(CONFIG_PATH, _configChangeListener);
      ZNRecord record = _propertyStore.get(CONFIG_PATH, null, AccessOption.PERSISTENT);
      if (record != null) {
        _metaConfigs.putAll(record.getSimpleFields());
      }
    }
  }

  public QuotaConfigProvider(ZkHelixPropertyStore<ZNRecord> propertyStore, TableCache tableCache) {
    this(propertyStore);
    _tableCache = tableCache;
  }

  private String quotasFromTableConfig(String namespacedConfigName) {
    String[] split = StringUtils.split(namespacedConfigName, '.');
    int length = split.length;
    if (length != 7) {
      return null;
    }
    // cluster.clusterName.database.databaseName.table.tableName.configName
    String tableName = split[3].equals(CommonConstants.DEFAULT_DATABASE) ? split[5] : split[3] + "." + split[5];
    String configName = split[7];
    TableConfig tableConfig = _tableCache.getTableConfig(tableName);
    if (tableConfig == null || tableConfig.getQuotaConfig() == null) {
      return null;
    }
    if (configName.equals("tableStorage")) {
      return tableConfig.getQuotaConfig().getStorage();
    }
    if (configName.equals("tableMaxQps")) {
      return tableConfig.getQuotaConfig().getMaxQueriesPerSecond();
    }
    return null;
  }

  public String getConfig(String namespacedConfigName) {
    String[] split = StringUtils.split(namespacedConfigName, '.');
    int length = split.length;
    Preconditions.checkArgument(length % 2 == 1,
        String.format("Wrong namesapce construction : %s", namespacedConfigName));
    String configName = split[length - 1];
    String namespace = "";
    String configValue = quotasFromTableConfig(namespacedConfigName);
    for (int i = 0; i < length - 1; i = i + 2) {
      namespace = String.format("%s%s.%s.", namespace, split[i], split[i + 1]);
      configValue = _metaConfigs.getProperty(namespace + configName, configValue);
    }
    return configValue;
  }

  public String getNamespacedConfigName(@Nullable String clusterName, @Nullable String databaseName,
      @Nullable String tableName, String configName) {
    StringBuilder builder = new StringBuilder();
    if (clusterName != null) {
      builder.append("cluster.");
      builder.append(clusterName);
      builder.append('.');
    }
    if (databaseName != null) {
      builder.append("database.");
      builder.append(databaseName);
      builder.append('.');
    }
    if (tableName != null) {
      String[] split = StringUtils.split(tableName, '.');
      if (split.length == 2) {
        if (databaseName == null) {
          builder.append("database.");
          builder.append(split[0]);
          builder.append('.');
        }
        builder.append("table.");
        builder.append(split[1]);
      } else {
        builder.append("table.");
        builder.append(tableName);
      }
      builder.append('.');
    }
    builder.append(configName);
    return builder.toString();
  }

  private class ZkQuotaConfigChangeListener implements IZkDataListener {

    @Override
    public void handleDataChange(String path, Object data)
        throws Exception {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        synchronized (_metaConfigs) {
          _metaConfigs.clear();
          _metaConfigs.putAll(znRecord.getSimpleFields());
          LOGGER.info("Updated quota configs with : {}", znRecord.getSimpleFields());
        }
      }
    }

    @Override
    public void handleDataDeleted(String path)
        throws Exception {
      _metaConfigs.clear();
      LOGGER.info("Deleted quota configs");
    }
  }
}
