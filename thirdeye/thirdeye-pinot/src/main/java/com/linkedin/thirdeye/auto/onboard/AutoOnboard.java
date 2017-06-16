package com.linkedin.thirdeye.auto.onboard;

import com.linkedin.thirdeye.datasource.DataSourceConfig;

/**
 * This is the abstract parent class for all auto onboard services for various datasources
 */
public abstract class AutoOnboard {
  private DataSourceConfig dataSourceConfig;

  public AutoOnboard(DataSourceConfig dataSourceConfig) {
    this.dataSourceConfig = dataSourceConfig;
  }

  public DataSourceConfig getDataSourceConfig() {
    return dataSourceConfig;
  }

  /**
   * Method which contains implementation of what needs to be done as part of onboarding for this data source
   */
  public abstract void run();

  /**
   * Method for triggering adhoc run of the onboard logic
   */
  public abstract void runAdhoc();
}
