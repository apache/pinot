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

package org.apache.pinot.thirdeye.common;

import java.net.URL;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThirdEyeConfigurationTest {
  ThirdEyeConfiguration config;

  @BeforeMethod
  public void beforeMethod() {
    this.config = new ThirdEyeConfiguration();
    this.config.setRootDir("/myRoot");
  }

  @Test
  public void testGetDataSourcesAsUrl() throws Exception {
    config.setDataSources("file:/myDir/myDataSources.yml");
    Assert.assertEquals(config.getDataSourcesAsUrl(), new URL("file:/myDir/myDataSources.yml"));
  }

  @Test
  public void testGetDataSourcesAsUrlAbsolute() throws Exception {
    config.setDataSources("/myDir/myDataSources.yml");
    Assert.assertEquals(config.getDataSourcesAsUrl(), new URL("file:/myDir/myDataSources.yml"));
  }

  @Test
  public void testGetDataSourcesAsUrlRelative() throws Exception {
    config.setDataSources("myDir/myDataSources.yml");
    Assert.assertEquals(config.getDataSourcesAsUrl(), new URL("file:/myRoot/myDir/myDataSources.yml"));
  }

  @Test
  public void testGetDataSourcesAsUrlDefault() throws Exception {
    config.getDataSourcesAsUrl();
    Assert.assertEquals(config.getDataSourcesAsUrl(), new URL("file:/myRoot/data-sources/data-sources-config.yml"));
  }
}
