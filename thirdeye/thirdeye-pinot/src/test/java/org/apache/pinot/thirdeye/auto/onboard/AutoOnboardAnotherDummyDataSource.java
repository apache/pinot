package org.apache.pinot.thirdeye.auto.onboard;

import org.apache.pinot.thirdeye.datasource.DataSourceConfig;
import org.apache.pinot.thirdeye.datasource.MetadataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutoOnboardAnotherDummyDataSource extends AutoOnboard {

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardAnotherDummyDataSource.class);

  public AutoOnboardAnotherDummyDataSource(MetadataSourceConfig metadataSourceConfig) {
    super(metadataSourceConfig);
  }

  @Override
  public void run() {
  }

  @Override
  public void runAdhoc() {

  }
}
