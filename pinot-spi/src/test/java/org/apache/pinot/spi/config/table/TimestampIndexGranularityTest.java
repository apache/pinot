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
package org.apache.pinot.spi.config.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TimestampIndexGranularityTest {

  @Test
  public void testTimestampIndexGranularity() {
    String timeColumn = "testTs";
    TimestampIndexGranularity granularity = TimestampIndexGranularity.DAY;
    String timeColumnWithGranularity = "$testTs$DAY";
    Assert.assertEquals(TimestampIndexGranularity.getColumnNameWithGranularity(timeColumn, granularity),
        timeColumnWithGranularity);
    Assert.assertTrue(TimestampIndexGranularity.isValidTimeColumnWithGranularityName(timeColumnWithGranularity));
    Assert.assertFalse(TimestampIndexGranularity.isValidTimeColumnWithGranularityName(timeColumn));
    Assert.assertFalse(TimestampIndexGranularity.isValidTimeColumnWithGranularityName("$docId"));
    Assert.assertFalse(TimestampIndexGranularity.isValidTimeColumnWithGranularityName("$ts$"));
    Assert.assertFalse(TimestampIndexGranularity.isValidTimeGranularity("day"));
    Assert.assertTrue(TimestampIndexGranularity.isValidTimeGranularity("DAY"));
  }

  @Test
  public void testGetFieldSpecForTimestampColumnWithGranularity() {
    FieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("t1", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:SECONDS");
    FieldSpec dateTimeFieldSpecForTimestampColumnWithGranularity =
        TimestampIndexGranularity.getFieldSpecForTimestampColumnWithGranularity(dateTimeFieldSpec,
            TimestampIndexGranularity.DAY);
    Assert.assertEquals("$t1$DAY", dateTimeFieldSpecForTimestampColumnWithGranularity.getName());
  }

  @Test
  public void testExtractTimestampIndexGranularityColumnNames() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t1").setFieldConfigList(
        ImmutableList.of(new FieldConfig("f1", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.ZSTANDARD,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.DAY)), ImmutableMap.of()),
            new FieldConfig("f2", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.LZ4,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.WEEK)), ImmutableMap.of()),
            new FieldConfig("f3", FieldConfig.EncodingType.DICTIONARY, null,
                ImmutableList.of(FieldConfig.IndexType.TIMESTAMP), FieldConfig.CompressionCodec.PASS_THROUGH,
                new TimestampConfig(ImmutableList.of(TimestampIndexGranularity.MONTH)), ImmutableMap.of()))).build();
    Set<String> timestampIndexGranularityColumnNames =
        TimestampIndexGranularity.extractTimestampIndexGranularityColumnNames(tableConfig);
    Assert.assertEquals(ImmutableSet.of("$f1$DAY", "$f2$WEEK", "$f3$MONTH"), timestampIndexGranularityColumnNames);
  }

  @Test
  public void testGetTransformExpression() {
    Assert.assertEquals(TimestampIndexGranularity.getTransformExpression("ts", TimestampIndexGranularity.DAY),
        "dateTrunc('DAY', ts)");
    Assert.assertEquals(TimestampIndexGranularity.getTransformExpression("ts", TimestampIndexGranularity.WEEK),
        "dateTrunc('WEEK', ts)");
    Assert.assertEquals(TimestampIndexGranularity.getTransformExpression("ts", TimestampIndexGranularity.MONTH),
        "dateTrunc('MONTH', ts)");
  }
}
