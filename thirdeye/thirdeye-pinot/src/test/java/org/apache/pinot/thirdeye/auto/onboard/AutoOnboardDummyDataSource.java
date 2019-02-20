package org.apache.pinot.thirdeye.auto.onboard;

import org.apache.pinot.thirdeye.datasource.DataSourceConfig;
import org.apache.pinot.thirdeye.datasource.MetadataSourceConfig;
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
