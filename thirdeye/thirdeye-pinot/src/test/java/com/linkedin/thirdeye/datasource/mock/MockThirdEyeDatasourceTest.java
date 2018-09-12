package com.linkedin.thirdeye.datasource.mock;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class MockThirdEyeDatasourceTest {
  private MockThirdEyeDataSource dataSource;

  // TODO execution tests
  // TODO generation tests
  // TODO caching consistency tests

  @BeforeMethod
  public void beforeMethod() throws Exception {
    Yaml yaml = new Yaml();
    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("data-sources-config.yml"))) {
      // NOTE: Yes, static typing does this to you.
      Map<String, List<Map<String, Map<String, Object>>>> config = (Map<String, List<Map<String, Map<String, Object>>>>) yaml.load(dataReader);
      this.dataSource = new MockThirdEyeDataSource(config.get("dataSourceConfigs").get(0).get("properties"));
    }
  }

  @Test
  public void testGetName() {
    Assert.assertEquals(this.dataSource.getName(), "MockThirdEyeDataSource");
  }

  @Test
  public void testGetMaxTime() throws Exception {
    long time = System.currentTimeMillis();
    Assert.assertTrue(this.dataSource.getMaxDataTime("pageViews") >= time);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMaxTimeInvalidDataset() throws Exception {
    this.dataSource.getMaxDataTime("invalid");
  }

  @Test
  public void testGetDatasets() throws Exception {
    Assert.assertEquals(this.dataSource.getDatasets(), Arrays.asList("pageViews", "purchases"));
  }

  @Test
  public void testGetDimensionFiltersPageViews() throws Exception {
    Map<String, List<String>> filters = this.dataSource.getDimensionFilters("pageViews");
    Assert.assertEquals(filters.keySet(), new HashSet<>(Arrays.asList("country", "browser", "platform")));
    Assert.assertEquals(filters.get("country"), Arrays.asList("ca", "mx", "us"));
    Assert.assertEquals(filters.get("browser"), Arrays.asList("chrome", "edge", "firefox", "safari"));
    Assert.assertEquals(filters.get("platform"), Arrays.asList("desktop", "mobile"));
  }

  @Test
  public void testGetDimensionFiltersPurchases() throws Exception {
    Map<String, List<String>> filters = this.dataSource.getDimensionFilters("purchases");
    Assert.assertEquals(filters.keySet(), new HashSet<>(Arrays.asList("country", "browser")));
    Assert.assertEquals(filters.get("country"), Arrays.asList("ca", "mx", "us"));
    Assert.assertEquals(filters.get("browser"), Arrays.asList("chrome", "edge", "safari"));
  }
}
