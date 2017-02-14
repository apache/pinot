package com.linkedin.thirdeye.detector.functionex.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThirdEyePinotDataSourceTest {
  static final Logger LOG = LoggerFactory.getLogger(ThirdEyePinotDataSourceTest.class);

  @Test
  public void testExtractTableName() {
    String table = ThirdEyePinotDataSource.extractTableName("SELECT * FROM my_pinot_table");
    Assert.assertEquals(table, "my_pinot_table");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testExtractTableFail() {
    String table = ThirdEyePinotDataSource.extractTableName("SELECT * LIMIT 10");
    Assert.assertEquals(table, "my_pinot_table");
  }

  @Test
  public void testExtractTableNameLowerCase() {
    String table = ThirdEyePinotDataSource.extractTableName("select * from my_pinot_table");
    Assert.assertEquals(table, "my_pinot_table");
  }

  @Test
  public void testExtractTableNameEvil() {
    String table = ThirdEyePinotDataSource.extractTableName(
        "select evil_field_from from my_pinot_table group by evil_field_from limit 10");
    Assert.assertEquals(table, "my_pinot_table");
  }
}
