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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
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

    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("userId", "Groovy({user_id}, user_id)"),
        new TransformConfig("fullName", "Groovy({firstName+' '+lastName}, firstName, lastName)"),
        new TransformConfig("maxBid", "Groovy({bids.max{ it.toBigDecimal() }}, bids)"),
        new TransformConfig("map2_keys", "Groovy({map2.sort()*.key}, map2)"),
        new TransformConfig("map2_values", "Groovy({map2.sort()*.value}, map2)"),
        new TransformConfig("hoursSinceEpoch", "Groovy({timestamp/(1000*60*60)}, timestamp)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTransformFunctions")
        .setIngestionConfig(ingestionConfig).build();

    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    DataTypeTransformer dataTypeTransformer = new DataTypeTransformer(tableConfig, pinotSchema);

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
    ArrayList map2Keys = (ArrayList) genericRow.getValue("map2_keys");
    Assert.assertEquals(map2Keys.get(0), "k1");
    Assert.assertEquals(map2Keys.get(1), "k2");
    ArrayList map2Values = (ArrayList) genericRow.getValue("map2_values");
    Assert.assertEquals(map2Values.get(0), 10);
    Assert.assertEquals(map2Values.get(1), 20);
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
    Object[] map2KeysObject = (Object[]) genericRow.getValue("map2_keys");
    Assert.assertEquals(map2KeysObject[0], "k1");
    Assert.assertEquals(map2KeysObject[1], "k2");
    Object[] map2ValuesObject = (Object[]) genericRow.getValue("map2_values");
    Assert.assertEquals(map2ValuesObject[0], 10);
    Assert.assertEquals(map2ValuesObject[1], 20);
    Assert.assertEquals(genericRow.getValue("cost"), 1000.0);
    // convert to LONG
    Assert.assertEquals(genericRow.getValue("hoursSinceEpoch"), 437222L);
  }

  /**
   * TODO: transform functions have moved to tableConfig#ingestionConfig. However, these tests remain to test
   * backward compatibility/
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

    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("userId", "Groovy({user_id}, user_id)"),
        new TransformConfig("fullName", "Groovy({firstName+' '+lastName}, firstName, lastName)"),
        new TransformConfig("hoursSinceEpoch", "Groovy({timestamp/(1000*60*60)}, timestamp)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTransformFunctions")
        .setIngestionConfig(ingestionConfig).build();

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
    ArrayList map1Keys = (ArrayList) genericRow.getValue("map1__KEYS");
    Assert.assertEquals(map1Keys.get(0), "200");
    Assert.assertEquals(map1Keys.get(1), "30");
    // Backward compatible way to support MAP - __VALUES indicates values of map1
    ArrayList map1Values = (ArrayList) genericRow.getValue("map1__VALUES");
    Assert.assertEquals(map1Values.get(0), "bar");
    Assert.assertEquals(map1Values.get(1), "foo");
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
    List<TransformConfig> transformConfigs = Collections.singletonList(
        new TransformConfig("fullName", "Groovy({firstName + ' ' + lastName}, firstName, lastName)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testValueExists").setIngestionConfig(ingestionConfig)
            .build();
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
        .setIngestionConfig(new IngestionConfig()).build();
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);

    genericRow = new GenericRow();
    genericRow.putValue("incoming", "123456789");
    genericRow.putValue("outgoing", "123");

    // no transformation
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), "123");
  }

  @Test
  public void testTransformFunctionSortOrder() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("a", FieldSpec.DataType.STRING)
        .addSingleValueDimension("b", FieldSpec.DataType.STRING).addSingleValueDimension("c", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d", FieldSpec.DataType.STRING).addSingleValueDimension("e", FieldSpec.DataType.STRING)
        .addSingleValueDimension("f", FieldSpec.DataType.STRING).build();
    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("d", "plus(x, 10)"),
        new TransformConfig("b", "plus(d, 10)"),
        new TransformConfig("a", "plus(b, 10)"),
        new TransformConfig("c", "plus(a, d)"),
        new TransformConfig("f", "plus(e, 10)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testDerivedFunctions")
        .setIngestionConfig(ingestionConfig).build();
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, schema);
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("x", 100);
    genericRow.putValue("e", 200);
    GenericRow transform = expressionTransformer.transform(genericRow);
    Assert.assertEquals(transform.getValue("a"), 130.0);
    Assert.assertEquals(transform.getValue("b"), 120.0);
    Assert.assertEquals(transform.getValue("c"), 240.0);
    Assert.assertEquals(transform.getValue("d"), 110.0);
    Assert.assertEquals(transform.getValue("e"), 200);
    Assert.assertEquals(transform.getValue("f"), 210.0);
  }

  /** Check if there is more than one transform function definition for the same column. */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Cannot set more than one"
      + " ingestion transform function on column: a.")
  public void testMultipleTransformFunctionSortOrder() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("a", FieldSpec.DataType.INT)
        .addSingleValueDimension("b", FieldSpec.DataType.INT).addSingleValueDimension("c", FieldSpec.DataType.INT)
        .build();

    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("a", "plus(b,10)"),
        new TransformConfig("a", "plus(c,10)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testMultipleTransformFunctionSortOrder")
            .setIngestionConfig(ingestionConfig).build();

    // should throw runtime exception indicating that there are multiple transform config on column a
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, schema);
  }

  /* Check if Ingestion Transform Functions are parsed successfully when there is no cycle. */
  @Test
  public void testNonCyclicTransformFunctionSortOrder() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("a", FieldSpec.DataType.INT)
        .addSingleValueDimension("b", FieldSpec.DataType.INT).addSingleValueDimension("c", FieldSpec.DataType.INT)
        .build();

    // Define transform function dependencies: a -> (b,c), b -> d, d -> e, c -> (d,e)
    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("a", "plus(b,c)"),
        new TransformConfig("b", "plus(d,10)"),
        new TransformConfig("d", "plus(e,10)"),
        new TransformConfig("c", "plus(d,e)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testNonCyclicTransformFunctionSortOrder")
            .setIngestionConfig(ingestionConfig).build();
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, schema);

    // Check topological sort order
    Iterator<String> sortedColumns = expressionTransformer._expressionEvaluators.keySet().iterator();
    Assert.assertEquals(sortedColumns.next(), "d");
    Assert.assertEquals(sortedColumns.next(), "b");
    Assert.assertEquals(sortedColumns.next(), "c");
    Assert.assertEquals(sortedColumns.next(), "a");
  }

  /* Check if we throw exception when Ingestion Transform Functions have a cycle. */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Expression "
      + "cycle found for column 'a' in Ingestion Transform Function definitions.")
  public void testCyclicTransformFunctionSortOrder() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("a", FieldSpec.DataType.INT)
        .addSingleValueDimension("b", FieldSpec.DataType.INT).addSingleValueDimension("c", FieldSpec.DataType.INT)
        .build();

    // Define transform function dependencies: a -> b, b -> c, c -> a
    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("a", "plus(b,10)"),
        new TransformConfig("b", "plus(c,10)"),
        new TransformConfig("c", "plus(a,10)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testRecrusiveTransformFunctionSortOrder")
            .setIngestionConfig(ingestionConfig).build();
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, schema);
  }

  @Test
  public void testTransformFunctionWithWrongInput() {
    Schema pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("x", FieldSpec.DataType.INT, true);
    pinotSchema.addField(dimensionFieldSpec);
    List<TransformConfig> transformConfigs = Collections.singletonList(
        new TransformConfig("y", "plus(x, 10)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTransformFunctionWithWrongInput")
            .setIngestionConfig(ingestionConfig)
            .build();
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    // Valid case: x is int, y is int
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("x", 10);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("y"), 20.0);
    // Invalid case: x is string, y is int
    genericRow = new GenericRow();
    genericRow.putValue("x", "abcd");
    try {
      expressionTransformer.transform(genericRow);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(e.getCause().getMessage(), "Caught exception while executing function: plus(x,'10')");
    }
  }

  @Test
  public void testTransformFunctionContinueOnError() {
    Schema pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("x", FieldSpec.DataType.INT, true);
    pinotSchema.addField(dimensionFieldSpec);
    List<TransformConfig> transformConfigs = Collections.singletonList(
        new TransformConfig("y", "plus(x, 10)"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTransformFunctionWithWrongInput")
            .setIngestionConfig(ingestionConfig)
            .build();
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    // Valid case: x is int, y is int
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("x", 10);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("y"), 20.0);
    // Invalid case: x is string, y is int
    genericRow = new GenericRow();
    genericRow.putValue("x", "abcd");
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("y"), null);
    // Invalid case: x is null, y is int
    genericRow = new GenericRow();
    genericRow.putValue("x", null);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("y"), null);
  }
}
