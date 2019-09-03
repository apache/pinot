package org.apache.pinot.thirdeye.datasource.mock;

import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class MockThirdEyeDataSourceTest {
  private static final String COL_PURCHASES = "purchases";
  private static final String COL_REVENUE = "revenue";
  private static final String COL_PAGE_VIEWS = "pageViews";
  private static final String COL_AD_IMPRESSIONS = "adImpressions";

  private MockThirdEyeDataSource dataSource;

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
    Assert.assertTrue(this.dataSource.getMaxDataTime("business") <= time);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMaxTimeInvalidDataset() throws Exception {
    this.dataSource.getMaxDataTime("invalid");
  }

  @Test
  public void testGetDatasets() throws Exception {
    Assert.assertEquals(this.dataSource.getDatasets(), new HashSet<>(Arrays.asList("business", "tracking")));
  }

  @Test
  public void testGetDimensionFiltersTracking() throws Exception {
    Map<String, List<String>> filters = this.dataSource.getDimensionFilters("tracking");
    Assert.assertEquals(filters.keySet(), new HashSet<>(Arrays.asList("country", "browser", "platform")));
    Assert.assertEquals(new HashSet<>(filters.get("country")), new HashSet<>(Arrays.asList("ca", "mx", "us")));
    Assert.assertEquals(new HashSet<>(filters.get("browser")), new HashSet<>(Arrays.asList("chrome", "edge", "firefox", "safari")));
    Assert.assertEquals(new HashSet<>(filters.get("platform")), new HashSet<>(Arrays.asList("desktop", "mobile")));
  }

  @Test
  public void testGetDimensionFiltersBusiness() throws Exception {
    Map<String, List<String>> filters = this.dataSource.getDimensionFilters("business");
    Assert.assertEquals(filters.keySet(), new HashSet<>(Arrays.asList("country", "browser")));
    Assert.assertEquals(new HashSet<>(filters.get("country")), new HashSet<>(Arrays.asList("ca", "mx", "us")));
    Assert.assertEquals(new HashSet<>(filters.get("browser")), new HashSet<>(Arrays.asList("chrome", "edge", "safari")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDimensionFiltersInvalidDataset() throws Exception {
    this.dataSource.getDimensionFilters("invalid");
  }

  @Test
  public void testDataGenerator() {
    Assert.assertEquals(this.dataSource.datasetData.size(), 2);
    Assert.assertEquals(this.dataSource.datasetData.get("business").size(), 28 * 9);
    Assert.assertTrue(this.dataSource.datasetData.get("tracking").size() > 27 * 21); // allow for DST
  }

  @Test
  public void testDataGeneratorHourly() {
    DataFrame data = this.dataSource.datasetData.get("tracking");

    System.out.println(data.getDoubles(COL_PAGE_VIEWS).count());

    // allow for DST
    Assert.assertTrue(data.getDoubles(COL_PAGE_VIEWS).count() >= (28 * 24 - 1) * 21); // allow for DST
    Assert.assertTrue(data.getDoubles(COL_PAGE_VIEWS).count() <= (28 * 24 + 1) * 21);
    Assert.assertTrue(data.getDoubles(COL_AD_IMPRESSIONS).count() >= (28 * 24 - 1) * 7); // allow for DST
    Assert.assertTrue(data.getDoubles(COL_AD_IMPRESSIONS).count() <= (28 * 24 + 1) * 7);

    Assert.assertTrue(data.getDoubles(COL_PAGE_VIEWS).sum().doubleValue() > 0);
    Assert.assertTrue(data.getDoubles(COL_AD_IMPRESSIONS).sum().doubleValue() > 0);
  }

  @Test
  public void testDataGeneratorDaily() {
    DataFrame data = this.dataSource.datasetData.get("business");
    Assert.assertEquals(data.getDoubles(COL_PURCHASES).count(), 28 * 9);
    Assert.assertEquals(data.getDoubles(COL_REVENUE).count(), 28 * 9);

    Assert.assertTrue(data.getDoubles(COL_PURCHASES).sum().doubleValue() > 0);
    Assert.assertTrue(data.getDoubles(COL_REVENUE).sum().doubleValue() > 0);
  }

  @Test
  public void testExecute() throws Exception {
    long time = System.currentTimeMillis();

    // reverse lookup hack for metric id
    Long metricId = null;
    for (Map.Entry<Long, String> entry : this.dataSource.metricNameMap.entrySet()) {
      if (COL_PAGE_VIEWS.equals(entry.getValue())) {
        metricId = entry.getKey();
      }
    }

    MetricFunction metricFunction = new MetricFunction(MetricAggFunction.SUM, COL_PAGE_VIEWS, metricId, "tracking", null, null);

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(time - TimeUnit.DAYS.toMillis(1))
        .setEndTimeExclusive(time)
        .addMetricFunction(metricFunction)
        .setGroupBy("browser")
        .build("ref");

    ThirdEyeResponse response = this.dataSource.execute(request);

    Assert.assertEquals(response.getNumRows(), 4);

    Set<String> resultDimensions = new HashSet<>();
    for (int i = 0; i < response.getNumRows(); i++) {
      Assert.assertTrue(response.getRow(i).getMetrics().get(0) > 0);
      resultDimensions.add(response.getRow(i).getDimensions().get(0));
    }

    Assert.assertEquals(resultDimensions, new HashSet<>(Arrays.asList("chrome", "edge", "firefox", "safari")));
  }

  @Test
  public void testDeterministicMetricOrder() {
    Assert.assertEquals(this.dataSource.metricNameMap.get(1L), "purchases");
    Assert.assertEquals(this.dataSource.metricNameMap.get(2L), "revenue");
    Assert.assertEquals(this.dataSource.metricNameMap.get(3L), "adImpressions");
    Assert.assertEquals(this.dataSource.metricNameMap.get(4L), "pageViews");
  }
}
