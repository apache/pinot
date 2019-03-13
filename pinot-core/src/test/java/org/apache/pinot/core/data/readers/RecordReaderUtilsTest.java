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
package org.apache.pinot.core.data.readers;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class RecordReaderUtilsTest {

  @Test
  public void testExtractFieldSpecs() {
    TimeGranularitySpec timeGranularitySpec1 = new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time");
    TimeGranularitySpec timeGranularitySpec2 =
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time");
    TimeGranularitySpec timeGranularitySpec3 = new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "days");

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
}
