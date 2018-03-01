package com.linkedin.thirdeye.common;

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
