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
package org.apache.pinot.spi.utils;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TimeConverterTest {

  @Test
  public void testIntTimeColumn() {
    TimeConverter converter = new TimeConverter(new TimeGranularitySpec(FieldSpec.DataType.INT, 5, TimeUnit.HOURS, "time"));
    // Should support conversion from all data types
    assertEquals(converter.toMillisSinceEpoch(123), 123 * TimeUnit.HOURS.toMillis(5));
    assertEquals(converter.toMillisSinceEpoch(123L), 123 * TimeUnit.HOURS.toMillis(5));
    assertEquals(converter.toMillisSinceEpoch(123f), 123 * TimeUnit.HOURS.toMillis(5));
    assertEquals(converter.toMillisSinceEpoch(123d), 123 * TimeUnit.HOURS.toMillis(5));
    assertEquals(converter.toMillisSinceEpoch("123"), 123 * TimeUnit.HOURS.toMillis(5));

    assertEquals(converter.fromMillisSinceEpoch(123 * TimeUnit.HOURS.toMillis(5)), 123);
  }

  @Test
  public void testLongTimeColumn() {
    TimeConverter converter = new TimeConverter(new TimeGranularitySpec(FieldSpec.DataType.LONG, 10, TimeUnit.DAYS, "time"));
    // Should support conversion from all data types
    assertEquals(converter.toMillisSinceEpoch(123), 123 * TimeUnit.DAYS.toMillis(10));
    assertEquals(converter.toMillisSinceEpoch(123L), 123 * TimeUnit.DAYS.toMillis(10));
    assertEquals(converter.toMillisSinceEpoch(123f), 123 * TimeUnit.DAYS.toMillis(10));
    assertEquals(converter.toMillisSinceEpoch(123d), 123 * TimeUnit.DAYS.toMillis(10));
    assertEquals(converter.toMillisSinceEpoch("123"), 123 * TimeUnit.DAYS.toMillis(10));

    assertEquals(converter.fromMillisSinceEpoch(123 * TimeUnit.DAYS.toMillis(10)), 123L);
  }

  @Test
  public void testFloatTimeColumn() {
    TimeConverter converter = new TimeConverter(new TimeGranularitySpec(FieldSpec.DataType.FLOAT, 1, TimeUnit.SECONDS, "time"));
    // Should support conversion from all data types
    assertEquals(converter.toMillisSinceEpoch(123), 123 * TimeUnit.SECONDS.toMillis(1));
    assertEquals(converter.toMillisSinceEpoch(123L), 123 * TimeUnit.SECONDS.toMillis(1));
    assertEquals(converter.toMillisSinceEpoch(123f), 123 * TimeUnit.SECONDS.toMillis(1));
    assertEquals(converter.toMillisSinceEpoch(123d), 123 * TimeUnit.SECONDS.toMillis(1));
    assertEquals(converter.toMillisSinceEpoch("123"), 123 * TimeUnit.SECONDS.toMillis(1));

    assertEquals(converter.fromMillisSinceEpoch(123 * TimeUnit.SECONDS.toMillis(1)), 123f);
  }

  @Test
  public void testDoubleTimeColumn() {
    TimeConverter converter = new TimeConverter(new TimeGranularitySpec(FieldSpec.DataType.DOUBLE, 3, TimeUnit.MINUTES, "time"));
    // Should support conversion from all data types
    assertEquals(converter.toMillisSinceEpoch(123), 123 * TimeUnit.MINUTES.toMillis(3));
    assertEquals(converter.toMillisSinceEpoch(123L), 123 * TimeUnit.MINUTES.toMillis(3));
    assertEquals(converter.toMillisSinceEpoch(123f), 123 * TimeUnit.MINUTES.toMillis(3));
    assertEquals(converter.toMillisSinceEpoch(123d), 123 * TimeUnit.MINUTES.toMillis(3));
    assertEquals(converter.toMillisSinceEpoch("123"), 123 * TimeUnit.MINUTES.toMillis(3));

    assertEquals(converter.fromMillisSinceEpoch(123 * TimeUnit.MINUTES.toMillis(3)), 123d);
  }

  @Test
  public void testStringTimeColumn() {
    TimeConverter converter = new TimeConverter(new TimeGranularitySpec(FieldSpec.DataType.STRING, 100, TimeUnit.MILLISECONDS, "time"));
    // Should support conversion from all data types
    assertEquals(converter.toMillisSinceEpoch(123), 123 * TimeUnit.MILLISECONDS.toMillis(100));
    assertEquals(converter.toMillisSinceEpoch(123L), 123 * TimeUnit.MILLISECONDS.toMillis(100));
    assertEquals(converter.toMillisSinceEpoch(123f), 123 * TimeUnit.MILLISECONDS.toMillis(100));
    assertEquals(converter.toMillisSinceEpoch(123d), 123 * TimeUnit.MILLISECONDS.toMillis(100));
    assertEquals(converter.toMillisSinceEpoch("123"), 123 * TimeUnit.MILLISECONDS.toMillis(100));

    assertEquals(converter.fromMillisSinceEpoch(123 * TimeUnit.MILLISECONDS.toMillis(100)), "123");
  }
}
