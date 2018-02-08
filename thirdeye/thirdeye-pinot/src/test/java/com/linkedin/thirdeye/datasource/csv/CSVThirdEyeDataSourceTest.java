package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceTest {
  ThirdEyeDataSource dataSource;

  @BeforeMethod
  public void beforeMethod(){
    dataSource = new CSVThirdEyeDataSource();
  }

  @Test
  public void testGetName() throws Exception{
    Assert.assertEquals(dataSource.getName(), CSVThirdEyeDataSource.class.getSimpleName());
  }
  

}
