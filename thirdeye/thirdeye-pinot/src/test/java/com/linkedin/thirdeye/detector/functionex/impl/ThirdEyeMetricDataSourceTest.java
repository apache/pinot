package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MockManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThirdEyeMetricDataSourceTest {

  static class MockDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
    String query;
    int outputLen = 0;

    @Override
    public DataFrame query(String query, AnomalyFunctionExContext context) throws Exception {
      this.query = query;
      DataFrame df = new DataFrame(outputLen);
      df.addSeries("time", new long[outputLen]);
      df.addSeries("function_metric", new long[outputLen]);
      return df;
    }
  }

  static class MockDatasetManager extends MockManager<DatasetConfigDTO> implements DatasetConfigManager {
    @Override
    public DatasetConfigDTO findByDataset(String dataset) {
      DatasetConfigDTO dto = new DatasetConfigDTO();
      dto.setTimeColumn("time");
      dto.setTimeDuration(10);
      dto.setTimeUnit(TimeUnit.SECONDS);
      return dto;
    }

    @Override
    public List<DatasetConfigDTO> findActive() {
      Assert.fail();
      return null;
    }

    @Override
    public List<DatasetConfigDTO> findActiveRequiresCompletenessCheck() {
      Assert.fail();
      return null;
    }
  }

  MockDataSource pinot;
  MockDatasetManager manager;
  ThirdEyeMetricDataSource ds;
  AnomalyFunctionExContext context;

  @BeforeMethod
  public void before() {
    this.pinot = new MockDataSource();
    this.manager = new MockDatasetManager();
    this.ds = new ThirdEyeMetricDataSource(pinot, manager);

    this.context = new AnomalyFunctionExContext();
    this.context.setMonitoringWindowStart(1000000);
    this.context.setMonitoringWindowEnd(2000000);
  }

  @Test
  public void testParseFilters() {
    String filterString = "a=bb,bb>=ccc,c<=aa,asdf<4,d>asdf,pew!=pew!";
    List<ThirdEyeMetricDataSource.Filter> filters = ThirdEyeMetricDataSource.parseFilters(filterString);
    Assert.assertEquals(filters.size(), 6);
    assertFilter(filters.get(0), "a", "=", "bb");
    assertFilter(filters.get(1), "bb", ">=", "ccc");
    assertFilter(filters.get(2), "c", "<=", "aa");
    assertFilter(filters.get(3), "asdf", "<", "4");
    assertFilter(filters.get(4), "d", ">", "asdf");
    assertFilter(filters.get(5), "pew", "!=", "pew!");
  }

  @Test
  public void testAlternativesClause() {
    String s = ThirdEyeMetricDataSource.makeFilters(Arrays.asList(
        new ThirdEyeMetricDataSource.Filter("a", "=", "b"),
        new ThirdEyeMetricDataSource.Filter("a", "=", "c")));
    Assert.assertEquals(s, "(((a='b') OR (a='c')))");
  }

  @Test
  public void testAlternativesClauseOneEquals() {
    String s = ThirdEyeMetricDataSource.makeFilters(Arrays.asList(
        new ThirdEyeMetricDataSource.Filter("a", "=", "b"),
        new ThirdEyeMetricDataSource.Filter("a", "<", "c")));
    Assert.assertEquals(s, "(((a='b') AND (a<'c')))");
  }

  @Test
  public void testDefaultClause() {
    String s = ThirdEyeMetricDataSource.makeFilters(Arrays.asList(
        new ThirdEyeMetricDataSource.Filter("a", ">=", "b"),
        new ThirdEyeMetricDataSource.Filter("a", "<", "c")));
    Assert.assertEquals(s, "(((a>='b') AND (a<'c')))");
  }

  @Test
  public void testQueryWithoutFilter() throws Exception {
    String query = "dataset/metric/function";
    this.ds.query(query, context);
    Assert.assertEquals(this.pinot.query, "SELECT function(metric) FROM dataset WHERE (((time>'100') AND (time<='200'))) GROUP BY time TOP 10000");
  }

  @Test
  public void testQueryWithFilter() throws Exception {
    String query = "dataset/metric/function/k1=v1,k1=v2,k2=v2";
    this.ds.query(query, context);
    Assert.assertEquals(this.pinot.query, "SELECT function(metric) FROM dataset WHERE (((k1='v1') OR (k1='v2')) AND ((k2='v2')) AND ((time>'100') AND (time<='200'))) GROUP BY k1, k2, time TOP 10000");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testQueryResultTruncationFail() throws Exception {
    this.pinot.outputLen = ThirdEyeMetricDataSource.TOP_K;
    this.ds.query("a/b/c", context);
  }

  static void assertFilter(ThirdEyeMetricDataSource.Filter f, String key, String operator, String value) {
    Assert.assertEquals(f.key, key);
    Assert.assertEquals(f.operator, operator);
    Assert.assertEquals(f.value, value);
  }

}
