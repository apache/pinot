package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceTest {
  ThirdEyeDataSource dataSource;

  @BeforeMethod
  public void beforeMethod(){

    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("source", new DataFrame()
        .addSeries("timestamp", 100)
        .addSeries("views", 1000)
        .addSeries("country", "us"));
    sources.put("other", new DataFrame());

    dataSource = CSVThirdEyeDataSource.fromDataFrame(sources);
  }

  @Test
  public void testFromDataFrame() throws Exception {
    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("a", new DataFrame());
    sources.put("b", new DataFrame());
    sources.put("c", new DataFrame());

    CSVThirdEyeDataSource dataSource = CSVThirdEyeDataSource.fromDataFrame(sources);
    Assert.assertEquals(new HashSet<>(dataSource.getDatasets()), sources.keySet());
    Assert.assertNotSame(new HashSet<>(dataSource.getDatasets()), sources.keySet());
  }

  @Test
  public void testFromUrl() throws Exception {
    URL business = this.getClass().getResource("business.csv");

    Map<String, URL> sources = new HashMap<>();
    sources.put("business", business);

    CSVThirdEyeDataSource dataSource = CSVThirdEyeDataSource.fromUrl(sources);

    Map<String, List<String>> dimensions = new HashMap<>();
    dimensions.put("country", Arrays.asList("us", "cn"));
    dimensions.put("browser", Arrays.asList("chrome", "firefox", "safari"));

    Assert.assertEquals(dataSource.getDatasets(), Collections.singletonList("business"));
    Assert.assertEquals(dataSource.getMaxDataTime("business"), 7200000);
    Assert.assertEquals(new HashSet<>(dataSource.getDimensionFilters("business").get("country")), new HashSet<>(dimensions.get("country")));
    Assert.assertEquals(new HashSet<>(dataSource.getDimensionFilters("business").get("browser")), new HashSet<>(dimensions.get("browser")));
  }

  @Test
  public void testGetName() {
    Assert.assertEquals(dataSource.getName(), CSVThirdEyeDataSource.class.getSimpleName());
  }

  @Test
  public void testGetDatasets() throws Exception{
    Assert.assertEquals(new HashSet(dataSource.getDatasets()), new HashSet(Arrays.asList("source", "other")));
  }

  @Test
  public void testGetMaxDataTime() throws Exception{
    Assert.assertEquals(dataSource.getMaxDataTime("source"), 100);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMaxDataTimeFail() throws Exception {
    dataSource.getMaxDataTime("invalid");
  }

  @Test
  public void testGetDimensionFilters() throws Exception{
    Assert.assertEquals(dataSource.getDimensionFilters("source"),
        Collections.singletonMap("country", Arrays.asList("us")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDimensionFiltersFail() throws Exception {
    dataSource.getDimensionFilters("invalid");
  }

  @Test
  public void testExecuteSingleRequest() throws Exception {
    DAOTestBase testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();


    MetricConfigDTO configDTO = new MetricConfigDTO();
    configDTO.setName("views");
    configDTO.setDataset("source");
    configDTO.setAlias("source::views");

    daoRegistry.getMetricConfigDAO().save(configDTO);
    Assert.assertNotNull(configDTO.getId());

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", configDTO.getId(), "source", null, null))
        .setDataSource("source")
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 1000)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response.getRequest(), expectedResponse.getRequest());
    Assert.assertEquals(response.getNumRows(), expectedResponse.getNumRows());
  }
}
