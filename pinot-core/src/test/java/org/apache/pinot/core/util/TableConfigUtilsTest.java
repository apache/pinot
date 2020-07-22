/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.util;

import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IngestionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;


public class TableConfigUtilsTest {

  @Test
  public void validateIngestionConfig() {
    // null ingestion config
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(null).build();
    TableConfigUtils.validate(tableConfig);

    // null filter config, transform config
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(null, null)).build();
    TableConfigUtils.validate(tableConfig);

    // null filter function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(new FilterConfig(null), null)).build();
    TableConfigUtils.validate(tableConfig);

    // valid filterFunction
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(new FilterConfig("startsWith(columnX, \"myPrefix\")"), null)).build();
    TableConfigUtils.validate(tableConfig);

    // valid filterFunction
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(new FilterConfig("Groovy({x == 10}, x)"), null)).build();
    TableConfigUtils.validate(tableConfig);

    // invalid filter function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(new FilterConfig("Groovy(badExpr)"), null)).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail on invalid filter function string");
    } catch (IllegalStateException e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(new FilterConfig("fakeFunction(xx)"), null)).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail for invalid filter function");
    } catch (IllegalStateException e) {
      // expected
    }

    // empty transform configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(null, Collections.emptyList())).build();
    TableConfigUtils.validate(tableConfig);

    // valid transform configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null, Lists.newArrayList(new TransformConfig("myCol", "reverse(anotherCol)")))).build();
    TableConfigUtils.validate(tableConfig);

    // valid transform configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null, Lists.newArrayList(new TransformConfig("myCol", "reverse(anotherCol)"),
            new TransformConfig("transformedCol", "Groovy({x+y}, x, y)")))).build();
    TableConfigUtils.validate(tableConfig);

    // null transform column name
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null, Lists.newArrayList(new TransformConfig(null, "reverse(anotherCol)")))).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail for null column name in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // null transform function string
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(new IngestionConfig(null, Lists.newArrayList(new TransformConfig("myCol", null)))).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail for null transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null, Lists.newArrayList(new TransformConfig("myCol", "fakeFunction(col)")))).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null, Lists.newArrayList(new TransformConfig("myCol", "Groovy(badExpr)")))).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null, Lists.newArrayList(new TransformConfig("myCol", "reverse(myCol)")))).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null,
            Lists.newArrayList(new TransformConfig("myCol", "Groovy({x + y + myCol}, x, myCol, y)")))).build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // duplicate transform config
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
        new IngestionConfig(null,
            Lists.newArrayList(new TransformConfig("myCol", "reverse(x)"), new TransformConfig("myCol", "lower(y)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail due to duplicate transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // chained transform functions
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(
            new IngestionConfig(null,
                    Lists.newArrayList(new TransformConfig("a", "reverse(x)"), new TransformConfig("b", "lower(a)"))))
            .build();
    try {
      TableConfigUtils.validate(tableConfig);
      Assert.fail("Should fail due to using transformed column 'a' as argument for transform function of column 'b'");
    } catch (IllegalStateException e) {
      // expected
    }
  }
}
