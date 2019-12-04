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
package org.apache.pinot.spi.data.readers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class RecordReaderUtilsTest {

  @Test
  public void testExtractFieldSpecs() {
    TimeGranularitySpec timeGranularitySpec1 = new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "time");
    TimeGranularitySpec timeGranularitySpec2 = new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "time");
    TimeGranularitySpec timeGranularitySpec3 = new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "days");

    // Only have incoming time column
    Schema schema = new Schema.SchemaBuilder().addTime(timeGranularitySpec1).build();
    List<FieldSpec> fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);
    assertEquals(fieldSpecs.size(), 1);
    assertEquals(fieldSpecs.get(0), new TimeFieldSpec(timeGranularitySpec1));

    // Have the same incoming and outgoing time column name
    schema = new Schema.SchemaBuilder().addTime(timeGranularitySpec1, timeGranularitySpec2).build();
    fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);
    assertEquals(fieldSpecs.size(), 1);
    assertEquals(fieldSpecs.get(0), new TimeFieldSpec(timeGranularitySpec1));

    // Have different incoming and outgoing time column name
    schema = new Schema.SchemaBuilder().addTime(timeGranularitySpec1, timeGranularitySpec3).build();
    fieldSpecs = RecordReaderUtils.extractFieldSpecs(schema);
    assertEquals(fieldSpecs.size(), 2);
    assertEquals(fieldSpecs.get(0), new TimeFieldSpec(timeGranularitySpec1));
    assertEquals(fieldSpecs.get(1), new TimeFieldSpec(timeGranularitySpec3));
  }

  @Test
  public void testConvertMultiValue() {
    FieldSpec fieldSpec = new DimensionFieldSpec("intMV", DataType.INT, false);

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, (Collection) null));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, (String[]) null));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Collections.emptyList()));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[0]));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Collections.singletonList(null)));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{null}));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Collections.singletonList("")));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{""}));

    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, Arrays.asList(null, "")));
    assertNull(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{null, ""}));

    assertEquals(RecordReaderUtils.convertMultiValue(fieldSpec, Arrays.asList(null, "", 123)), new Object[]{123});
    assertEquals(RecordReaderUtils.convertMultiValue(fieldSpec, new String[]{null, "", "123"}), new Object[]{123});
  }
}
