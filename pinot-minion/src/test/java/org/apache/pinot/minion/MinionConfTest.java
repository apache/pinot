/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.minion;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MinionConfTest {

  @Test
  public void testDeprecatedConfigs()
      throws ConfigurationException {
    // Check configs with old names that have no pinot.minion prefix.
    String[] cfgKeys = new String[]{
        CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY,
        CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY,
        CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER,
        CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_CRYPTER
    };
    PinotConfiguration rawCfg = new PinotConfiguration(new PropertiesConfiguration(
        PropertiesConfiguration.class.getClassLoader().getResource("pinot-configuration-old-minion.properties")
            .getFile()));
    final MinionConf oldConfig = new MinionConf(rawCfg.toMap());
    Assert.assertEquals(oldConfig.getMetricsPrefix(), "pinot.minion.old.custom.metrics.");
    for (String cfgKey : cfgKeys) {
      Assert.assertFalse(oldConfig.subset(cfgKey).isEmpty(), cfgKey);
      Assert.assertTrue(oldConfig.subset("pinot.minion." + cfgKey).isEmpty(), cfgKey);
    }

    // Check configs with new names that have the pinot.minion prefix.
    rawCfg = new PinotConfiguration(new PropertiesConfiguration(
        PropertiesConfiguration.class.getClassLoader().getResource("pinot-configuration-new-minion.properties")
            .getFile()));
    final MinionConf newConfig = new MinionConf(rawCfg.toMap());
    for (String cfgKey : cfgKeys) {
      Assert.assertTrue(newConfig.subset(cfgKey).isEmpty(), cfgKey);
      Assert.assertFalse(newConfig.subset("pinot.minion." + cfgKey).isEmpty(), cfgKey);
    }
    // Check the config values.
    Assert.assertEquals(newConfig.getMetricsPrefix(), "pinot.minion.new.custom.metrics.");
    PinotConfiguration subcfg = newConfig.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    Assert.assertEquals(subcfg.subset("class").getProperty("s3"), "org.apache.pinot.plugin.filesystem.S3PinotFS");
    subcfg = newConfig.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    Assert.assertEquals(subcfg.getProperty("protocols"), "file,http,s3");
    subcfg = newConfig.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER);
    Assert.assertEquals(subcfg.subset("https").getProperty("enabled"), "true");
    subcfg = newConfig.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    Assert.assertEquals(subcfg.subset("class").getProperty("nooppinotcrypter"),
        "org.apache.pinot.core.crypt.NoOpPinotCrypter");
  }
}
