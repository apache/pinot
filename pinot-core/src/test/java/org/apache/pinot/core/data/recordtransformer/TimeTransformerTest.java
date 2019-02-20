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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.core.data.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class TimeTransformerTest {
  private static final long VALID_TIME = new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
  private static final long INVALID_TIME = new DateTime(2200, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();

  @Test
  public void testTimeFormat() {
    // When incoming and outgoing spec are the same, any time format should work
    Schema schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "time")));
    TimeTransformer transformer = new TimeTransformer(schema);
    GenericRow record = new GenericRow();
    record.putField("time", 20180101);
    record = transformer.transform(record);
    assertNotNull(record);
    assertEquals(record.getValue("time"), 20180101);

    // When incoming and outgoing spec are not the same, simple date format is not allowed
    schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "outgoing")));
    try {
      new TimeTransformer(schema);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testSkipConversion() {
    // When incoming time does not exist or is invalid, outgoing time exists and is valid, skip conversion
    Schema schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing")));
    TimeTransformer transformer = new TimeTransformer(schema);
    GenericRow record = new GenericRow();
    record.putField("outgoing", VALID_TIME);
    record = transformer.transform(record);
    assertNotNull(record);
    assertEquals(record.getValue("outgoing"), VALID_TIME);

    transformer = new TimeTransformer(schema);
    record = new GenericRow();
    record.putField("incoming", INVALID_TIME);
    record.putField("outgoing", VALID_TIME);
    record = transformer.transform(record);
    assertNotNull(record);
    assertEquals(record.getValue("outgoing"), VALID_TIME);

    // When incoming and outgoing time column is the same, and the value is already converted, skip conversion
    schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time")));
    transformer = new TimeTransformer(schema);
    record = new GenericRow();
    record.putField("time", VALID_TIME);
    record = transformer.transform(record);
    assertNotNull(record);
    assertEquals(record.getValue("time"), VALID_TIME);

    // When both incoming and outgoing time do not exist or are invalid, throw exception
    schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing")));
    transformer = new TimeTransformer(schema);
    record = new GenericRow();
    record.putField("incoming", INVALID_TIME);
    record.putField("outgoing", INVALID_TIME);
    try {
      transformer.transform(record);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time")));
    transformer = new TimeTransformer(schema);
    record = new GenericRow();
    record.putField("time", INVALID_TIME);
    try {
      transformer.transform(record);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testTimeConversion() {
    // When incoming time exists and is valid, do the conversion
    Schema schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing")));
    TimeTransformer transformer = new TimeTransformer(schema);
    GenericRow record = new GenericRow();
    record.putField("incoming", TimeUnit.MILLISECONDS.toDays(VALID_TIME));
    record = transformer.transform(record);
    assertNotNull(record);
    assertEquals(record.getValue("outgoing"), VALID_TIME);

    // When incoming and outgoing time column is the same, and the value is not yet converted, do the conversion
    schema = new Schema();
    schema.addField(new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time")));
    transformer = new TimeTransformer(schema);
    record = new GenericRow();
    record.putField("time", TimeUnit.MILLISECONDS.toDays(VALID_TIME));
    record = transformer.transform(record);
    assertNotNull(record);
    assertEquals(record.getValue("time"), VALID_TIME);
  }
}
