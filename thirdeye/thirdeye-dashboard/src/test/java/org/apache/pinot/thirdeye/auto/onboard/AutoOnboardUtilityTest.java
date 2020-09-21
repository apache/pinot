/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.auto.onboard;

import org.apache.pinot.thirdeye.datasource.MetadataSourceConfig;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AutoOnboardUtilityTest {

  @Test
  public void testDataSourceToAutoOnboardMap() {
    URL url = AutoOnboardUtilityTest.class.getResource("/data-sources/data-sources-config-1.yml");

    Map<String, List<AutoOnboard>> dsToOnboardsMap = AutoOnboardUtility.getDataSourceToAutoOnboardMap(url);

    // Assert two data sources (PinotThirdEyeDataSource, CSVThirdEyeDataSource)
    Assert.assertEquals(dsToOnboardsMap.keySet().size(), 2);

    // PinotThirdEyeDataSource has 2 metadata loaders (AutoOnboardDummyDataSource, AutoOnboardAnotherDummyDataSource)
    Assert.assertEquals(dsToOnboardsMap.get("PinotThirdEyeDataSource").size(), 2);

    // CSVThirdEyeDataSource has 1 metadata loader (AutoOnboardAnotherRandomDataSource)
    Assert.assertEquals(dsToOnboardsMap.get("CSVThirdEyeDataSource").size(), 1);

    // Assertion on AutoOnboardDummyDataSource
    MetadataSourceConfig dummyMDSource = dsToOnboardsMap.get("PinotThirdEyeDataSource").get(0).getMetadataSourceConfig();
    Assert.assertEquals(dummyMDSource.getClassName(), "org.apache.pinot.thirdeye.auto.onboard.AutoOnboardDummyDataSource");
    Assert.assertEquals(dummyMDSource.getProperties().size(), 10);
    Assert.assertEquals(dummyMDSource.getProperties().get("username"), "username");
    Assert.assertEquals(dummyMDSource.getProperties().get("password"), "password");

    // Assertion on AutoOnboardAnotherDummyDataSource
    MetadataSourceConfig anotherDummyMDSource = dsToOnboardsMap.get("PinotThirdEyeDataSource").get(1).getMetadataSourceConfig();
    Assert.assertEquals(anotherDummyMDSource.getClassName(), "org.apache.pinot.thirdeye.auto.onboard.AutoOnboardAnotherDummyDataSource");
    Assert.assertEquals(anotherDummyMDSource.getProperties().size(), 10);
    Assert.assertEquals(anotherDummyMDSource.getProperties().get("host"), "host");
    Assert.assertEquals(anotherDummyMDSource.getProperties().get("port"), 9999);

    // Assertion on AutoOnboardAnotherRandomDataSource
    MetadataSourceConfig anotherRandomMDSource = dsToOnboardsMap.get("CSVThirdEyeDataSource").get(0).getMetadataSourceConfig();
    Assert.assertEquals(anotherRandomMDSource.getClassName(), "org.apache.pinot.thirdeye.auto.onboard.AutoOnboardAnotherRandomDataSource");
    Assert.assertEquals(anotherRandomMDSource.getProperties().size(), 1);
  }

  @Test
  public void testAutoOnboardClassNotFoundService() {
    URL url = AutoOnboardUtilityTest.class.getResource("/data-sources/data-sources-config-2.yml");

    Map<String, List<AutoOnboard>> dsToOnboardsMap = AutoOnboardUtility.getDataSourceToAutoOnboardMap(url);

    // Assert no metadata loaders
    Assert.assertEquals(dsToOnboardsMap.keySet().size(), 0);
  }
}
