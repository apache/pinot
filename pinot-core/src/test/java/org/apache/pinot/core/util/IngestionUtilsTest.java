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
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.IngestionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests fields extraction from Schema dna TableConfig for ingestion
 */
public class IngestionUtilsTest {

  @Test
  public void testExtractFieldsSchema() {

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema;

    // from groovy function
    schema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, argument2)");
    schema.addField(dimensionFieldSpec);
    List<String> extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // Map implementation for Avro - map__KEYS indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__KEYS", FieldSpec.DataType.INT, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__KEYS")));

    // Map implementation for Avro - map__VALUES indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__VALUES", FieldSpec.DataType.LONG, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__VALUES")));

    // Time field spec
    // only incoming
    schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("time"));

    // incoming and outgoing different column name
    schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "in"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "out")).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("in", "out")));

    // inbuilt functions
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("hoursSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochHours(timestamp)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestamp", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("tenMinutesSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochMinutesBucket(timestamp, 10)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestamp")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("date", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS");
    dateTimeFieldSpec.setTransformFunction("toDateTime(timestamp, 'yyyy-MM-dd')");
    schema.addField(dateTimeFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("date", "timestamp")));
  }

  @Test
  public void testExtractFieldsIngestionConfig() {
    Schema schema = new Schema();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    tableConfig.setIngestionConfig(new IngestionConfig(new FilterConfig("Groovy({x > 100}, x)")));

    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x")));

    schema.addField(new DimensionFieldSpec("y", FieldSpec.DataType.STRING, true));
    fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 2);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x", "y")));
  }
}
