package com.linkedin.thirdeye.datasource.mock;

import com.linkedin.thirdeye.dataframe.DataFrame;
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

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


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
    Assert.assertTrue(this.dataSource.getMaxDataTime("business") >= time);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMaxTimeInvalidDataset() throws Exception {
    this.dataSource.getMaxDataTime("invalid");
  }

  @Test
  public void testGetDatasets() throws Exception {
    Assert.assertEquals(this.dataSource.getDatasets(), Arrays.asList("business", "tracking"));
  }

  @Test
  public void testGetDimensionFiltersTracking() throws Exception {
    Map<String, List<String>> filters = this.dataSource.getDimensionFilters("tracking");
    Assert.assertEquals(filters.keySet(), new HashSet<>(Arrays.asList("country", "browser", "platform")));
    Assert.assertEquals(filters.get("country"), Arrays.asList("ca", "mx", "us"));
    Assert.assertEquals(filters.get("browser"), Arrays.asList("chrome", "edge", "firefox", "safari"));
    Assert.assertEquals(filters.get("platform"), Arrays.asList("desktop", "mobile"));
  }

  @Test
  public void testGetDimensionFiltersBusiness() throws Exception {
    Map<String, List<String>> filters = this.dataSource.getDimensionFilters("business");
    Assert.assertEquals(filters.keySet(), new HashSet<>(Arrays.asList("country", "browser")));
    Assert.assertEquals(filters.get("country"), Arrays.asList("ca", "mx", "us"));
    Assert.assertEquals(filters.get("browser"), Arrays.asList("chrome", "edge", "safari"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDimensionFiltersInvalidDataset() throws Exception {
    this.dataSource.getDimensionFilters("invalid");
  }

  @Test
  public void testDataGenerator() {
    Assert.assertEquals(this.dataSource.data.size(), 30);
  }

  @Test
  public void testDataGeneratorHourly() {
    MockThirdEyeDataSource.Tuple path = new MockThirdEyeDataSource.Tuple(new String[] { "tracking", "metrics", "pageViews", "mx", "chrome", "mobile" });
    DataFrame data = this.dataSource.data.get(path);

    // allow for DST
    Assert.assertTrue(data.getDoubles(COL_VALUE).count() >= 28 * 24 - 1);
    Assert.assertTrue(data.getDoubles(COL_VALUE).count() <= 28 * 24 + 1);

    Assert.assertTrue(data.getDoubles(COL_VALUE).sum().doubleValue() > 0);
  }

  @Test
  public void testDataGeneratorDaily() {
    MockThirdEyeDataSource.Tuple path = new MockThirdEyeDataSource.Tuple(new String[] { "business", "metrics", "purchases", "ca", "safari" });
    DataFrame data = this.dataSource.data.get(path);
    Assert.assertEquals(data.getDoubles(COL_VALUE).count(), 28);
    Assert.assertTrue(data.getDoubles(COL_VALUE).sum().doubleValue() > 0);
  }
}
