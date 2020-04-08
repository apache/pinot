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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.function.evaluators.DefaultTimeSpecEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class DefaultTimeSpecEvaluatorTest {
  private static final long VALID_TIME = new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
  private static final long INVALID_TIME = new DateTime(2200, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();

  @Test
  public void testTimeFormat() {
    // When incoming and outgoing spec are the same, any time format should work
    TimeFieldSpec timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "time"));
    DefaultTimeSpecEvaluator transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    GenericRow record = new GenericRow();
    record.putField("time", 20180101);
    Object transformedValue = transformer.evaluate(record);
    assertNotNull(transformedValue);
    assertEquals(transformedValue, 20180101);

    // When incoming and outgoing spec are not the same, simple date format is not allowed
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "outgoing"));
    try {
      new DefaultTimeSpecEvaluator(timeFieldSpec);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testSkipConversion() {
    // When incoming time does not exist or is invalid, outgoing time exists and is valid, skip conversion
    TimeFieldSpec timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    DefaultTimeSpecEvaluator transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    GenericRow record = new GenericRow();
    record.putField("outgoing", VALID_TIME);
    Object transformedValue = transformer.evaluate(record);
    assertNotNull(transformedValue);
    assertEquals(transformedValue, VALID_TIME);

    transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    record = new GenericRow();
    record.putField("incoming", INVALID_TIME);
    record.putField("outgoing", VALID_TIME);
    transformedValue = transformer.evaluate(record);
    assertNotNull(transformedValue);
    assertEquals(transformedValue, VALID_TIME);

    // When incoming and outgoing time column is the same, and the value is already converted, skip conversion
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"));
    transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    record = new GenericRow();
    record.putField("time", VALID_TIME);
    transformedValue = transformer.evaluate(record);
    assertNotNull(transformedValue);
    assertEquals(transformedValue, VALID_TIME);

    // When both incoming and outgoing time do not exist or are invalid, throw exception
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    record = new GenericRow();
    record.putField("incoming", INVALID_TIME);
    record.putField("outgoing", INVALID_TIME);
    try {
      transformer.evaluate(record);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"));
    transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    record = new GenericRow();
    record.putField("time", INVALID_TIME);
    try {
      transformer.evaluate(record);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testTimeConversion() {
    // When incoming time exists and is valid, do the conversion
    TimeFieldSpec timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    DefaultTimeSpecEvaluator transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    GenericRow record = new GenericRow();
    record.putField("incoming", TimeUnit.MILLISECONDS.toDays(VALID_TIME));
    Object transformedValue = transformer.evaluate(record);
    assertNotNull(transformedValue);
    assertEquals(transformedValue, VALID_TIME);

    // When incoming and outgoing time column is the same, and the value is not yet converted, do the conversion
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"));
    transformer = new DefaultTimeSpecEvaluator(timeFieldSpec);
    record = new GenericRow();
    record.putField("time", TimeUnit.MILLISECONDS.toDays(VALID_TIME));
    transformedValue = transformer.evaluate(record);
    assertNotNull(transformedValue);
    assertEquals(transformedValue, VALID_TIME);
  }
}
