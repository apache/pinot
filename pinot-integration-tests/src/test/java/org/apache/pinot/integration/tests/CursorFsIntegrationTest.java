package org.apache.pinot.integration.tests;

import java.io.File;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class CursorFsIntegrationTest extends CursorIntegrationTest {
  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_INSTANCE_TAGS,
        TagNameUtils.getBrokerTagForTenant(TENANT_NAME));
    configuration.setProperty(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".protocol", "file");
    File tmpPath = new File(_tempDir, "tmp");
    File dataPath = new File(_tempDir, "data");
    configuration.setProperty(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".temp.dir", tmpPath);
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".file.data.dir", "file://" + dataPath);
  }

  @Override
  protected Object[][] getPageSizes() {
    return new Object[][]{
        {1000}, {0} // 0 triggers default behaviour
    };
  }
}
