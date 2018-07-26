/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import static java.util.concurrent.TimeUnit.*;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.pinot.common.data.FieldSpec.DataType.*;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
public class DefaultTimeConverterTest {

  @Test
  public void testWithSameTimeSpec() {
    TimeGranularitySpec spec = new TimeGranularitySpec(LONG, 1, DAYS, "1day");
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
    TimeGranularitySpec incoming = new TimeGranularitySpec(LONG, 2, DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(LONG, 48, HOURS, "48hours");
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
    TimeGranularitySpec incoming = new TimeGranularitySpec(LONG, 2, DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(LONG, 24, HOURS, "24hours");
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
    TimeGranularitySpec incoming = new TimeGranularitySpec(LONG, 2, DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(LONG, 24, HOURS, "24hours");
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
    TimeGranularitySpec incoming = new TimeGranularitySpec(LONG, 2, DAYS, "2days");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(STRING, 24, HOURS, "24hours");
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

  @Test
  public void testSimpleDateFormat() {
    TimeGranularitySpec incoming;
    TimeGranularitySpec outgoing;
    DefaultTimeConverter timeConverter;
    String SDF_PREFIX = TimeFormat.SIMPLE_DATE_FORMAT.toString();

    //this should not throw exception, since incoming == outgoing
    try {
      incoming = new TimeGranularitySpec(STRING, 1, HOURS, SDF_PREFIX + ":yyyyMMdd", "1hour");
      outgoing = new TimeGranularitySpec(STRING, 1, HOURS, SDF_PREFIX + ":yyyyMMdd", "1hour");
      timeConverter = new DefaultTimeConverter();
      timeConverter.init(incoming, outgoing);
    } catch (Exception e) {
      Assert.fail("sdf to sdf must be supported as long as incoming sdf = outgoing sdf");
    }
    //we don't support epoch to sdf conversion
    try {
      incoming = new TimeGranularitySpec(STRING, 1, HOURS, SDF_PREFIX + ":yyyyMMdd", "1hour");
      outgoing = new TimeGranularitySpec(LONG, 1, HOURS, "1hour");
      timeConverter = new DefaultTimeConverter();
      timeConverter.init(incoming, outgoing);
      Assert.fail("We don't support converting epoch to sdf currently");
    } catch (Exception e) {
      //expected
    }

    //we don't support sdf to epoch conversion
    try {
      incoming = new TimeGranularitySpec(STRING, 1, HOURS, SDF_PREFIX + ":yyyyMMdd", "1hour");
      outgoing = new TimeGranularitySpec(LONG, 1, HOURS, "1hour");
      timeConverter = new DefaultTimeConverter();
      timeConverter.init(incoming, outgoing);
      Assert.fail("We don't support converting sdf to epoch currently");
    } catch (Exception e) {
      //expected
    }
    
    //we don't support sdf to sdf conversion where incoming sdf != outoging sdf
    try {
      incoming = new TimeGranularitySpec(STRING, 1, HOURS, SDF_PREFIX + ":yyyyMMdd", "1hour");
      outgoing = new TimeGranularitySpec(STRING, 1, HOURS, SDF_PREFIX + ":yyyyMMddHH", "1hour");
      timeConverter = new DefaultTimeConverter();
      timeConverter.init(incoming, outgoing);
      Assert.fail("We don't support converting sdf to sdf where incoming sdf != outgoing sdf");
    } catch (Exception e) {
      //expected
    }
  }
}
