package org.apache.pinot.common.config.provider;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

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
        if (_instance != null) {
            throw new IllegalStateException("Instance already initialized.");
        }
        _instance = new TableConfigAndSchemaCache(propertyStore);
    }

    public static TableConfigAndSchemaCache getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("Instance not initialized.");
        }
        return INSTANCE;
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
}
