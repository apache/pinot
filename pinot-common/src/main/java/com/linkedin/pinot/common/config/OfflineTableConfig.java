package com.linkedin.pinot.common.config;

import java.util.Map;


public class OfflineTableConfig extends AbstractTableConfig {

  private final IndexingConfig indexConfig;

  protected OfflineTableConfig(String tableName, String tableType,
      SegmentsValidationAndRetentionConfig validationConfig, TenantConfig tenantConfig,
      TableCustomConfig customConfigs, Map<String, String> rawMap, IndexingConfig indexConfig) {
    super(tableName, tableType, validationConfig, tenantConfig, customConfigs, rawMap);
    this.indexConfig = indexConfig;
  }

  @Override
  public IndexingConfig getIndexingConfig() {
    return indexConfig;
  }

  @Override
  public String toString() {
    StringBuilder bld = new StringBuilder(super.toString());
    bld.append(indexConfig.toString());
    return bld.toString();
  }
}
