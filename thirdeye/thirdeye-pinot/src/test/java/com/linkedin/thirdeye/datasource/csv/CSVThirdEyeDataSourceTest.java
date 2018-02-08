package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceTest {
  ThirdEyeDataSource dataSource;

  @BeforeMethod
  public void beforeMethod(){
    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("source", new DataFrame());
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


}
