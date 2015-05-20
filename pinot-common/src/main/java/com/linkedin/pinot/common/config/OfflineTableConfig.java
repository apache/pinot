package com.linkedin.pinot.common.config;



public class OfflineTableConfig extends AbstractTableConfig {

  private final IndexingConfig indexConfig;

  protected OfflineTableConfig(String tableName, String tableType,
      SegmentsValidationAndRetentionConfig validationConfig, TenantConfig tenantConfig,
      TableCustomConfig customConfigs, String jsonString, IndexingConfig indexConfig) {
    super(tableName, tableType, validationConfig, tenantConfig, customConfigs, jsonString);
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
