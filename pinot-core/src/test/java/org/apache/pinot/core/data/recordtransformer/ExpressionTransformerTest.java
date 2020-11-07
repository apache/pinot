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
package org.apache.pinot.core.data.recordtransformer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.IngestionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the evaluation of transform expressions by the ExpressionTransformer
 */
public class ExpressionTransformerTest {

  @Test
  public void testTransformConfigsFromTableConfig() {
    Schema pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("userId", FieldSpec.DataType.LONG)
        .addSingleValueDimension("fullName", FieldSpec.DataType.STRING)
        .addMultiValueDimension("bids", FieldSpec.DataType.INT)
        .addSingleValueDimension("maxBid", FieldSpec.DataType.INT)
        .addMultiValueDimension("map2_keys", FieldSpec.DataType.STRING)
        .addMultiValueDimension("map2_values", FieldSpec.DataType.INT).addMetric("cost", FieldSpec.DataType.DOUBLE)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();

    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("userId", "Groovy({user_id}, user_id)"));
    transformConfigs.add(new TransformConfig("fullName", "Groovy({firstName+' '+lastName}, firstName, lastName)"));
    transformConfigs.add(new TransformConfig("maxBid", "Groovy({bids.max{ it.toBigDecimal() }}, bids)"));
    transformConfigs.add(new TransformConfig("map2_keys", "Groovy({map2.sort()*.key}, map2)"));
    transformConfigs.add(new TransformConfig("map2_values", "Groovy({map2.sort()*.value}, map2)"));
    transformConfigs.add(new TransformConfig("hoursSinceEpoch", "Groovy({timestamp/(1000*60*60)}, timestamp)"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTransformFunctions")
        .setIngestionConfig(new IngestionConfig(null, transformConfigs)).build();

    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    DataTypeTransformer dataTypeTransformer = new DataTypeTransformer(pinotSchema);

    // test functions from schema
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("user_id", 1L);
    genericRow.putValue("firstName", "John");
    genericRow.putValue("lastName", "Denver");
    genericRow.putValue("bids", Arrays.asList(10, 20));
    HashMap<String, String> map1 = new HashMap<>(); // keys in Map from avro are always in STRING
    map1.put("30", "foo");
    map1.put("200", "bar");
    genericRow.putValue("map1", map1);
    HashMap<String, Integer> map2 = new HashMap<>();
    map2.put("k1", 10);
    map2.put("k2", 20);
    genericRow.putValue("map2", map2);
    genericRow.putValue("cost", 1000.0);
    genericRow.putValue("timestamp", 1574000000000L);
    genericRow.putValue("lon", 1.0);
    genericRow.putValue("lat", 2.0);

    // expression transformer
    expressionTransformer.transform(genericRow);

    // extract userId
    Assert.assertEquals(genericRow.getValue("userId"), 1L);
    // concat fullName
    Assert.assertEquals(genericRow.getValue("fullName"), "John Denver");
    Assert.assertTrue(((List) genericRow.getValue("bids")).containsAll(Arrays.asList(10, 20)));
    // find max bid from bids
    Assert.assertEquals(genericRow.getValue("maxBid"), 20);
    // handle Map through transform functions
    ArrayList map2__keys = (ArrayList) genericRow.getValue("map2_keys");
    Assert.assertEquals(map2__keys.get(0), "k1");
    Assert.assertEquals(map2__keys.get(1), "k2");
    ArrayList map2__values = (ArrayList) genericRow.getValue("map2_values");
    Assert.assertEquals(map2__values.get(0), 10);
    Assert.assertEquals(map2__values.get(1), 20);
    Assert.assertEquals(genericRow.getValue("cost"), 1000.0);
    // calculate hoursSinceEpoch
    Assert.assertEquals(genericRow.getValue("hoursSinceEpoch").toString(), "437222.2222222222");

    // data type transformer
    dataTypeTransformer.transform(genericRow);

    Assert.assertEquals(genericRow.getValue("userId"), 1L);
    Assert.assertEquals(genericRow.getValue("fullName"), "John Denver");
    Assert.assertEquals(((Object[]) genericRow.getValue("bids")), new Integer[]{10, 20});
    Assert.assertEquals(genericRow.getValue("maxBid"), 20);
    // handle Map through transform functions
    Object[] map2Keys = (Object[]) genericRow.getValue("map2_keys");
    Assert.assertEquals(map2Keys[0], "k1");
    Assert.assertEquals(map2Keys[1], "k2");
    Object[] map2Values = (Object[]) genericRow.getValue("map2_values");
    Assert.assertEquals(map2Values[0], 10);
    Assert.assertEquals(map2Values[1], 20);
    Assert.assertEquals(genericRow.getValue("cost"), 1000.0);
    // convert to LONG
    Assert.assertEquals(genericRow.getValue("hoursSinceEpoch"), 437222L);
  }

  /**
   * TODO: transform functions have moved to tableConfig#ingestionConfig. However, these tests remain to test backward compatibility/
   *  Remove these when we totally stop honoring transform functions in schema
   */
  @Test
  public void testTransformConfigsFromSchema() {
    Schema pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("userId", FieldSpec.DataType.LONG)
        .addSingleValueDimension("fullName", FieldSpec.DataType.STRING)
        .addMultiValueDimension("bids", FieldSpec.DataType.INT)
        .addSingleValueDimension("maxBid", FieldSpec.DataType.INT)
        .addMultiValueDimension("map1__KEYS", FieldSpec.DataType.INT)
        .addMultiValueDimension("map1__VALUES", FieldSpec.DataType.STRING).addMetric("cost", FieldSpec.DataType.DOUBLE)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();

    // only specified in schema
    pinotSchema.getFieldSpecFor("maxBid").setTransformFunction("Groovy({bids.max{ it.toBigDecimal() }}, bids)");
    // also specified in table config, ignore the schema setting
    pinotSchema.getFieldSpecFor("hoursSinceEpoch").setTransformFunction("Groovy({timestamp/(1000)}, timestamp)");

    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("userId", "Groovy({user_id}, user_id)"));
    transformConfigs.add(new TransformConfig("fullName", "Groovy({firstName+' '+lastName}, firstName, lastName)"));
    transformConfigs.add(new TransformConfig("hoursSinceEpoch", "Groovy({timestamp/(1000*60*60)}, timestamp)"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTransformFunctions")
        .setIngestionConfig(new IngestionConfig(null, transformConfigs)).build();

    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);

    GenericRow genericRow = new GenericRow();
    genericRow.putValue("user_id", 1L);
    genericRow.putValue("firstName", "John");
    genericRow.putValue("lastName", "Denver");
    genericRow.putValue("bids", Arrays.asList(10, 20));
    HashMap<String, String> map1 = new HashMap<>(); // keys in Map from avro are always in STRING
    map1.put("30", "foo");
    map1.put("200", "bar");
    genericRow.putValue("map1", map1);
    HashMap<String, Integer> map2 = new HashMap<>();
    map2.put("k1", 10);
    map2.put("k2", 20);
    genericRow.putValue("map2", map2);
    genericRow.putValue("cost", 1000.0);
    genericRow.putValue("timestamp", 1574000000000L);

    // expression transformer
    expressionTransformer.transform(genericRow);

    // extract userId
    Assert.assertEquals(genericRow.getValue("userId"), 1L);
    // concat fullName
    Assert.assertEquals(genericRow.getValue("fullName"), "John Denver");
    Assert.assertTrue(((List) genericRow.getValue("bids")).containsAll(Arrays.asList(10, 20)));
    // find max bid from bids
    Assert.assertEquals(genericRow.getValue("maxBid"), 20);
    // Backward compatible way to support MAP - __KEYS indicates keys of map1
    ArrayList map1__keys = (ArrayList) genericRow.getValue("map1__KEYS");
    Assert.assertEquals(map1__keys.get(0), "200");
    Assert.assertEquals(map1__keys.get(1), "30");
    // Backward compatible way to support MAP - __VALUES indicates values of map1
    ArrayList map1__values = (ArrayList) genericRow.getValue("map1__VALUES");
    Assert.assertEquals(map1__values.get(0), "bar");
    Assert.assertEquals(map1__values.get(1), "foo");
    Assert.assertEquals(genericRow.getValue("cost"), 1000.0);
    // calculate hoursSinceEpoch
    Assert.assertEquals(genericRow.getValue("hoursSinceEpoch").toString(), "437222.2222222222");
  }

  /**
   * If destination field already exists in the row, do not execute transform function
   */
  @Test
  public void testValueAlreadyExists() {
    Schema pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("fullName", FieldSpec.DataType.STRING, true);
    pinotSchema.addField(dimensionFieldSpec);
    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("fullName", "Groovy({firstName + ' ' + lastName}, firstName, lastName)"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testValueExists")
        .setIngestionConfig(new IngestionConfig(null, transformConfigs)).build();
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);

    GenericRow genericRow = new GenericRow();
    genericRow.putValue("firstName", "John");
    genericRow.putValue("lastName", "Denver");
    genericRow.putValue("fullName", "John N Denver");

    // no transformation
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("fullName"), "John N Denver");

    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "outgoing")).build();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testValueExists")
        .setIngestionConfig(new IngestionConfig(null, null)).build();
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);

    genericRow = new GenericRow();
    genericRow.putValue("incoming", "123456789");
    genericRow.putValue("outgoing", "123");

    // no transformation
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), "123");
  }

  /**
   * Reorder transformations so that all dependencies can be pre-evaluated.
   * https://github.com/apache/incubator-pinot/issues/5351
   */
  @Test
  public void testReorderBasedOnDependencies() {
    Schema pinotSchema = new Schema.SchemaBuilder()
        .addMultiValueDimension("expenses", FieldSpec.DataType.INT)
        .addMultiValueDimension("headCounts", FieldSpec.DataType.INT)
        .build();


    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("averageExpense", "Groovy({totalExpense / totalHeadCount}, totalExpense, totalHeadCount)"));
    transformConfigs.add(new TransformConfig("totalExpense", "Groovy({expenses.sum()}, expenses)"));
    transformConfigs.add(new TransformConfig("totalHeadCount", "Groovy({headCounts.sum()}, headCounts)"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTransformFunctions")
        .setIngestionConfig(new IngestionConfig(null, transformConfigs)).build();

    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);

    GenericRow genericRow = new GenericRow();
    genericRow.putValue("expenses", Arrays.asList(1, 1, 1));
    genericRow.putValue("headCounts", Arrays.asList(2, 2, 2));

    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("totalExpense"), 3);
    Assert.assertEquals(genericRow.getValue("averageExpense"), BigDecimal.valueOf(.5));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCyclicDependencies() {
    Schema pinotSchema = new Schema.SchemaBuilder()
        .addMultiValueDimension("a", FieldSpec.DataType.INT)
        .build();

    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("b", "Groovy({a + d}, a, d)"));
    transformConfigs.add(new TransformConfig("c", "Groovy({b}, b)"));
    transformConfigs.add(new TransformConfig("d", "Groovy({c}, c)"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTransformFunctions")
        .setIngestionConfig(new IngestionConfig(null, transformConfigs)).build();

    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);

    GenericRow genericRow = new GenericRow();
    genericRow.putValue("a", 1);

    expressionTransformer.transform(genericRow);
  }
}
