package org.apache.pinot.spi.config.provider;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * An interface for the provider of pinot configs
 */
public interface PinotConfigProvider {

  /**
   * Gets the tableConfig
   */
  TableConfig getTableConfig(String tableNameWithType);

  /**
   * Registers the {@link TableConfigChangeListener} and notifies it whenever any changes (addition, update, removal)
   * to any of the table configs are detected
   */
  void registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener);

  /**
   * Gets the schema
   */
  Schema getSchema(String rawTableName);

  /**
   * Registers the {@link SchemaChangeListener} and notifies it whenever any changes  (addition, update, removal) to
   * schemas are detected
   */
  void registerSchemaChangeListener(SchemaChangeListener schemaChangeListener);
}
