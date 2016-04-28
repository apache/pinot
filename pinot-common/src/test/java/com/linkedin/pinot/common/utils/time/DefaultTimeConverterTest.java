/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.TimeGranularitySpec;

public class DefaultTimeConverterTest {

  @Test
  public void testWithSameTimeSpec() {
    TimeGranularitySpec spec = new TimeGranularitySpec(DataType.LONG, 1, TimeUnit.DAYS, "1day");
    DefaultTimeConverter timeConverter = new DefaultTimeConverter();
    timeConverter.init(spec, spec);
    for (int i = 0; i < 1000; ++i) {
      Object convertedValue = timeConverter.convert(i);
      Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
      Assert.assertEquals(((Long) convertedValue).intValue(), i);
    }
    for (long i = 0; i < 1000; ++i) {
      Object convertedValue = timeConverter.convert(i);
      Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
      Assert.assertEquals(((Long) convertedValue).longValue(), i);
    }
  }

  @Test
  public void testWithDifferentTimeSpecButSameValue() {
    TimeGranularitySpec incoming = new TimeGranularitySpec(DataType.LONG, 2, TimeUnit.DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(DataType.LONG, 48, TimeUnit.HOURS, "48hours");
    DefaultTimeConverter timeConverter = new DefaultTimeConverter();
    timeConverter.init(incoming, outgoing);
    for (int i = 0; i < 1000; ++i) {
      Object convertedValue = timeConverter.convert(i);
      Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
      Assert.assertEquals(((Long) convertedValue).intValue(), i);
    }
    for (long i = 0; i < 1000; ++i) {
      Object convertedValue = timeConverter.convert(i);
      Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
      Assert.assertEquals(((Long) convertedValue).longValue(), i);
    }
  }

  @Test
  public void testWithDifferentTimeSpecs() {
    TimeGranularitySpec incoming = new TimeGranularitySpec(DataType.LONG, 2, TimeUnit.DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(DataType.LONG, 24, TimeUnit.HOURS, "24hours");
    DefaultTimeConverter timeConverter = new DefaultTimeConverter();
    timeConverter.init(incoming, outgoing);
    for (int i = 0; i < 1000; ++i) {
      Object convertedValue = timeConverter.convert(i);
      Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
      Assert.assertEquals(((Long) convertedValue).intValue(), i * 2);
    }
    for (long i = 0; i < 1000; ++i) {
      Object convertedValue = timeConverter.convert(i);
      Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
      Assert.assertEquals(((Long) convertedValue).longValue(), i * 2);
    }
  }

  @Test
  public void testWithDifferentIncomingValueTypes() {
    TimeGranularitySpec incoming = new TimeGranularitySpec(DataType.LONG, 2, TimeUnit.DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(DataType.LONG, 24, TimeUnit.HOURS, "24hours");
    DefaultTimeConverter timeConverter = new DefaultTimeConverter();
    timeConverter.init(incoming, outgoing);
    Object convertedValue = timeConverter.convert("1");
    Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
    Assert.assertEquals(((Long) convertedValue).intValue(), 2);
    convertedValue = timeConverter.convert(1);
    Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
    Assert.assertEquals(((Long) convertedValue).intValue(), 2);
    convertedValue = timeConverter.convert((long) 1);
    Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
    Assert.assertEquals(((Long) convertedValue).intValue(), 2);
    convertedValue = timeConverter.convert((short) 1);
    Assert.assertTrue(convertedValue instanceof Long, "Converted value data type should be Long");
    Assert.assertEquals(((Long) convertedValue).intValue(), 2);
  }

  @Test
  public void testWithOutgoingValueTypesString() {
    TimeGranularitySpec incoming = new TimeGranularitySpec(DataType.LONG, 2, TimeUnit.DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(DataType.STRING, 24, TimeUnit.HOURS, "24hours");
    DefaultTimeConverter timeConverter = new DefaultTimeConverter();
    timeConverter.init(incoming, outgoing);
    Object convertedValue = timeConverter.convert("1");
    Assert.assertTrue(convertedValue instanceof String, "Converted value data type should be STRING");
    Assert.assertEquals(Integer.parseInt(convertedValue.toString()), 2);
    Assert.assertEquals(convertedValue, "2");
    convertedValue = timeConverter.convert(1);
    Assert.assertTrue(convertedValue instanceof String, "Converted value data type should be STRING");
    Assert.assertEquals(Integer.parseInt(convertedValue.toString()), 2);
    Assert.assertEquals(convertedValue, "2");
    convertedValue = timeConverter.convert((long) 1);
    Assert.assertTrue(convertedValue instanceof String, "Converted value data type should be STRING");
    Assert.assertEquals(Integer.parseInt(convertedValue.toString()), 2);
    Assert.assertEquals(convertedValue, "2");
    convertedValue = timeConverter.convert((short) 1);
    Assert.assertTrue(convertedValue instanceof String, "Converted value data type should be STRING");
    Assert.assertEquals(Integer.parseInt(convertedValue.toString()), 2);
    Assert.assertEquals(convertedValue, "2");
  }
}
