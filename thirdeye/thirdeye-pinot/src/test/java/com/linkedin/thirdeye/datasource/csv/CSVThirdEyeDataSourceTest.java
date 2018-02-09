package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ExpectedExceptions;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceTest {
  ThirdEyeDataSource dataSource;

  @BeforeMethod
  public void beforeMethod(){
    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("source", new DataFrame()
        .addSeries("timestamp", 100)
        .addSeries("country", "us"));
    sources.put("other", new DataFrame());

    dataSource = new CSVThirdEyeDataSource(sources);
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


}
