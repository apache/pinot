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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * This class caches the table config and schema for all tables in the server instance.
 * It fetches the latest table config and schema from ZK if not available in the cache.
 */
public class TableConfigAndSchemaCache {

    private static TableConfigAndSchemaCache _instance;

    private final ConcurrentHashMap<String, TableConfig> _tableConfigCache;
    private final ConcurrentHashMap<String, Schema> _tableSchemaCache;

    private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

    private TableConfigAndSchemaCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
        _propertyStore = propertyStore;
        _tableConfigCache = new ConcurrentHashMap<>();
        _tableSchemaCache = new ConcurrentHashMap<>();
    }

    public static void init(ZkHelixPropertyStore<ZNRecord> propertyStore) {
        _instance = new TableConfigAndSchemaCache(propertyStore);
    }

    public static TableConfigAndSchemaCache getInstance() {
        if (_instance == null) {
            // This is a fallback for cases where init() is not called used in tests
            _instance = new TableConfigAndSchemaCache(null);
        }
        return _instance;
    }

    /**
     * Get the cached table config if available, otherwise fetch the latest table config from ZK and cache it.
     *
     * @param tableNameWithType Table name with type suffix
     * @return TableConfig
     */
    public TableConfig getTableConfig(String tableNameWithType) {
        return _tableConfigCache.computeIfAbsent(tableNameWithType, this::getLatestTableConfig);
    }

    /**
     * Get the cached schema if available, otherwise fetch the latest schema from ZK and cache it.
     *
     * @param tableName - Table name without or without type suffix
     * @return Schema - Schema for the table
     */
    public Schema getSchema(String tableName) {
        String rawTableName = TableNameBuilder.extractRawTableName(tableName);
        return _tableSchemaCache.computeIfAbsent(rawTableName, this::getLatestSchema);
    }

    /**
     * Get the latest table config for the given table name.
     *
     * @param tableNameWithType Table name with type suffix
     * @return TableConfig - Latest table config
     */
    public TableConfig getLatestTableConfig(String tableNameWithType) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
        _tableConfigCache.put(tableNameWithType, tableConfig);
        return tableConfig;
    }

    /**
     * Get the latest schema for the given table name.
     *
     * @param tableName - Table name without or without type suffix
     * @return Schema - Latest schema
     */
    public Schema getLatestSchema(String tableName) {
        String rawTableName = TableNameBuilder.extractRawTableName(tableName);
        Schema schema = ZKMetadataProvider.getSchema(_propertyStore, rawTableName);
        Preconditions.checkState(schema != null, "Failed to find schema for table: %s", rawTableName);
        _tableSchemaCache.put(rawTableName, schema);
        return schema;
    }

    public void setTableConfig(TableConfig tableConfig) {
        _tableConfigCache.put(tableConfig.getTableName(), tableConfig);
    }

    public void setSchema(String tableName, Schema schema) {
        String rawTableName = TableNameBuilder.extractRawTableName(tableName);
        _tableSchemaCache.put(rawTableName, schema);
    }
}
