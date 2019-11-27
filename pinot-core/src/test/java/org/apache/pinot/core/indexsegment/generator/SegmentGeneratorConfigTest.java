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
package org.apache.pinot.core.indexsegment.generator;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


// TODO: add more tests here.
public class SegmentGeneratorConfigTest {

  @Test
  public void testEpochTime() {
    Schema schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch")).build();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    assertEquals(segmentGeneratorConfig.getTimeColumnName(), "daysSinceEpoch");
    assertEquals(segmentGeneratorConfig.getTimeColumnType(), SegmentGeneratorConfig.TimeColumnType.EPOCH);
    assertEquals(segmentGeneratorConfig.getSegmentTimeUnit(), TimeUnit.DAYS);
    assertNull(segmentGeneratorConfig.getSimpleDateFormat());
  }

  @Test
  public void testSimpleDateFormat() {
    Schema schema = new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(FieldSpec.DataType.STRING, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT + ":yyyyMMdd", "Date")).build();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    assertEquals(segmentGeneratorConfig.getTimeColumnName(), "Date");
    assertEquals(segmentGeneratorConfig.getTimeColumnType(), SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE);
    assertNull(segmentGeneratorConfig.getSegmentTimeUnit());
    assertEquals(segmentGeneratorConfig.getSimpleDateFormat(), "yyyyMMdd");
  }
}
