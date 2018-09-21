package com.linkedin.thirdeye.auto.onboard;

import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutoOnboardDummyDataSource extends AutoOnboard {

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardDummyDataSource.class);

  public AutoOnboardDummyDataSource(MetadataSourceConfig metadataSourceConfig) {
    super(metadataSourceConfig);
  }

  @Override
  public void run() {
  }

  @Override
  public void runAdhoc() {

  }
}
