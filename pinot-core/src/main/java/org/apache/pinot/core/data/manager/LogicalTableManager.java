package org.apache.pinot.core.data.manager;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;


public class LogicalTableManager {
  private final LogicalTableConfig _logicalTableConfig;
  private final Schema _logicalTableSchema;
  private final TableConfig _offlineTableConfig;
  private final TableConfig _realtimeTableConfig;

  public LogicalTableManager(LogicalTableConfig logicalTableConfig, Schema logicalTableSchema,
      TableConfig offlineTableConfig, TableConfig realtimeTableConfig) {
    _logicalTableConfig = logicalTableConfig;
    _logicalTableSchema = logicalTableSchema;
    _offlineTableConfig = offlineTableConfig;
    _realtimeTableConfig = realtimeTableConfig;
  }

  public LogicalTableConfig getLogicalTableConfig() {
    return _logicalTableConfig;
  }
  public Schema getLogicalTableSchema() {
    return _logicalTableSchema;
  }
  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }
  public TableConfig getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }
}
