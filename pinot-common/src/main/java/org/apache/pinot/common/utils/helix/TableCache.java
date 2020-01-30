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
package org.apache.pinot.common.utils.helix;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.zookeeper.api.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.api.zkclient.IZkDataListener;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Caches table config and schema of a table.
 *  At the start - loads all the table configs and schemas in map.
 *  sets up a zookeeper listener that watches for any change and updates the cache.
 *  TODO: optimize to load only changed table configs/schema on a callback.
 *  TODO: Table deletes are not handled as of now
 *  Goal is to eventually grow this into a PinotClusterDataAccessor
 */
public class TableCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableCache.class);

  private static final String PROPERTYSTORE_SCHEMAS_PREFIX = "/SCHEMAS";
  private static final String PROPERTYSTORE_TABLE_CONFIGS_PREFIX = "/CONFIGS/TABLE";

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  TableConfigChangeListener _tableConfigChangeListener;
  SchemaChangeListener _schemaChangeListener;

  public TableCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    _schemaChangeListener = new SchemaChangeListener();
    _schemaChangeListener.refresh();
    _tableConfigChangeListener = new TableConfigChangeListener();
    _tableConfigChangeListener.refresh();
  }

  public String getActualTableName(String tableName) {
    return _tableConfigChangeListener._tableNameMap.getOrDefault(tableName.toLowerCase(), tableName);
  }

  public String getActualColumnName(String tableName, String columnName) {
    String schemaName = _tableConfigChangeListener._table2SchemaConfigMap.get(tableName.toLowerCase());
    if (schemaName != null) {
      String actualColumnName = _schemaChangeListener.getColumnName(schemaName, columnName);
      // If actual column name doesn't exist in schema, then return the origin column name.
      if (actualColumnName == null) {
        return columnName;
      }
      return actualColumnName;
    }
    return columnName;
  }

  class TableConfigChangeListener implements IZkChildListener, IZkDataListener {

    Map<String, TableConfig> _tableConfigMap = new ConcurrentHashMap<>();
    Map<String, String> _tableNameMap = new ConcurrentHashMap<>();
    Map<String, String> _table2SchemaConfigMap = new ConcurrentHashMap<>();

    public synchronized void refresh() {
      try {
        //always subscribe first before reading, so that we dont miss any changes
        _propertyStore.subscribeChildChanges(PROPERTYSTORE_TABLE_CONFIGS_PREFIX, _tableConfigChangeListener);
        _propertyStore.subscribeDataChanges(PROPERTYSTORE_TABLE_CONFIGS_PREFIX, _tableConfigChangeListener);
        List<ZNRecord> children =
            _propertyStore.getChildren(PROPERTYSTORE_TABLE_CONFIGS_PREFIX, null, AccessOption.PERSISTENT);
        if (children != null) {
          for (ZNRecord znRecord : children) {
            try {
              TableConfig tableConfig = TableConfig.fromZnRecord(znRecord);
              String tableNameWithType = tableConfig.getTableName();
              _tableConfigMap.put(tableNameWithType, tableConfig);
              String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
              //create case insensitive mapping
              _tableNameMap.put(tableNameWithType.toLowerCase(), tableNameWithType);
              _tableNameMap.put(rawTableName.toLowerCase(), rawTableName);
              //create case insensitive mapping between table name and schemaName
              _table2SchemaConfigMap.put(tableNameWithType.toLowerCase(), rawTableName);
              _table2SchemaConfigMap.put(rawTableName.toLowerCase(), rawTableName);
            } catch (Exception e) {
              LOGGER.warn("Exception loading table config for: {}: {}", znRecord.getId(), e.getMessage());
              //ignore
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Exception subscribing/reading tableconfigs", e);
        //ignore
      }
    }

    @Override
    public void handleChildChange(String s, List<String> list)
        throws Exception {
      refresh();
    }

    @Override
    public void handleDataChange(String s, Object o)
        throws Exception {
      refresh();
    }

    @Override
    public void handleDataDeleted(String s)
        throws Exception {
      refresh();
    }
  }

  class SchemaChangeListener implements IZkChildListener, IZkDataListener {
    Map<String, Map<String, String>> _schemaColumnMap = new ConcurrentHashMap<>();

    public synchronized void refresh() {
      try {
        //always subscribe first before reading, so that we dont miss any changes between reading and setting the watcher again
        _propertyStore.subscribeChildChanges(PROPERTYSTORE_SCHEMAS_PREFIX, _schemaChangeListener);
        _propertyStore.subscribeDataChanges(PROPERTYSTORE_SCHEMAS_PREFIX, _schemaChangeListener);
        List<ZNRecord> children =
            _propertyStore.getChildren(PROPERTYSTORE_SCHEMAS_PREFIX, null, AccessOption.PERSISTENT);
        if (children != null) {
          for (ZNRecord znRecord : children) {
            try {
              Schema schema = SchemaUtils.fromZNRecord(znRecord);
              String schemaNameLowerCase = schema.getSchemaName().toLowerCase();
              Collection<FieldSpec> allFieldSpecs = schema.getAllFieldSpecs();
              ConcurrentHashMap<String, String> columnNameMap = new ConcurrentHashMap<>();
              _schemaColumnMap.put(schemaNameLowerCase, columnNameMap);
              for (FieldSpec fieldSpec : allFieldSpecs) {
                columnNameMap.put(fieldSpec.getName().toLowerCase(), fieldSpec.getName());
              }
            } catch (Exception e) {
              LOGGER.warn("Exception loading schema for: {}: {}", znRecord.getId(), e.getMessage());
              //ignore
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Exception subscribing/reading schemas", e);
        //ignore
      }
    }

    String getColumnName(String schemaName, String columnName) {
      Map<String, String> columnNameMap = _schemaColumnMap.get(schemaName.toLowerCase());
      if (columnNameMap != null) {
        return columnNameMap.get(columnName.toLowerCase());
      }
      return columnName;
    }

    @Override
    public void handleChildChange(String s, List<String> list)
        throws Exception {
      refresh();
    }

    @Override
    public void handleDataChange(String s, Object o)
        throws Exception {
      refresh();
    }

    @Override
    public void handleDataDeleted(String s)
        throws Exception {
      refresh();
    }
  }
}

