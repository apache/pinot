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

package org.apache.pinot.thirdeye.detection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConfigUtilsTest {
  @Test
  public void testGetListNull() {
    Assert.assertTrue(ConfigUtils.getList(null).isEmpty());
  }

  @Test
  public void testGetListPartialNull() {
    Assert.assertEquals(ConfigUtils.getList(Arrays.asList("a", null)).size(), 2);
  }

  @Test
  public void testGetMapNull() {
    Assert.assertTrue(ConfigUtils.getMap(null).isEmpty());
  }

  @Test
  public void testGetMapPartialNull() {
    Assert.assertEquals(ConfigUtils.getMap(Collections.singletonMap("a", null)).size(), 1);
  }

  @Test
  public void testGetLongsNull() {
    Assert.assertTrue(ConfigUtils.getLongs(null).isEmpty());
  }

  @Test
  public void testGetLongsPartialNull() {
    Assert.assertEquals(ConfigUtils.getLongs(Arrays.asList(1L, null, 2L)).size(), 2);
  }

  @Test
  public void testGetMultimapNull() {
    Assert.assertTrue(ConfigUtils.getMultimap(null).isEmpty());
  }

  @Test
  public void testGetMultimapPartialNull() {
    Assert.assertEquals(ConfigUtils.getMultimap(Collections.singletonMap("a", Arrays.asList("A", null))).size(), 2);
  }

  @Test
  public void testGetListModification() {
    List<String> defaultList = new ArrayList<>();
    List<String> list = ConfigUtils.getList(null, defaultList);
    list.add("value");
    Assert.assertNotEquals(list, defaultList);
  }

  @Test
  public void testGetMapModification() {
    Map<String, String> defaultMap = new HashMap<>();
    Map<String, String> map = ConfigUtils.getMap(null, defaultMap);
    map.put("key", "value");
    Assert.assertNotEquals(map, defaultMap);
  }

  @Test
  public void testPeriodParser() {
    Assert.assertEquals(ConfigUtils.parsePeriod("3600"), new Period().withField(DurationFieldType.millis(), 3600));
    Assert.assertEquals(ConfigUtils.parsePeriod("1d"), new Period().withField(DurationFieldType.days(), 1));
    Assert.assertEquals(ConfigUtils.parsePeriod("2hours"), new Period().withField(DurationFieldType.hours(), 2));
    Assert.assertEquals(ConfigUtils.parsePeriod("24 hrs"), new Period().withField(DurationFieldType.hours(), 24));
    Assert.assertEquals(ConfigUtils.parsePeriod("1 year"), new Period().withField(DurationFieldType.years(), 1));
    Assert.assertEquals(ConfigUtils.parsePeriod("  3   w  "), new Period().withField(DurationFieldType.weeks(), 3));
  }

  @Test
  public void testPeriodTypeParser() {
    Assert.assertEquals(ConfigUtils.parsePeriodType("ms"), PeriodType.millis());
    Assert.assertEquals(ConfigUtils.parsePeriodType("millis"), PeriodType.millis());
    Assert.assertEquals(ConfigUtils.parsePeriodType("s"), PeriodType.seconds());
    Assert.assertEquals(ConfigUtils.parsePeriodType("sec"), PeriodType.seconds());
    Assert.assertEquals(ConfigUtils.parsePeriodType("secs"), PeriodType.seconds());
    Assert.assertEquals(ConfigUtils.parsePeriodType("seconds"), PeriodType.seconds());
    Assert.assertEquals(ConfigUtils.parsePeriodType("m"), PeriodType.minutes());
    Assert.assertEquals(ConfigUtils.parsePeriodType("min"), PeriodType.minutes());
    Assert.assertEquals(ConfigUtils.parsePeriodType("mins"), PeriodType.minutes());
    Assert.assertEquals(ConfigUtils.parsePeriodType("minutes"), PeriodType.minutes());
    Assert.assertEquals(ConfigUtils.parsePeriodType("h"), PeriodType.hours());
    Assert.assertEquals(ConfigUtils.parsePeriodType("hour"), PeriodType.hours());
    Assert.assertEquals(ConfigUtils.parsePeriodType("hours"), PeriodType.hours());
    Assert.assertEquals(ConfigUtils.parsePeriodType("d"), PeriodType.days());
    Assert.assertEquals(ConfigUtils.parsePeriodType("day"), PeriodType.days());
    Assert.assertEquals(ConfigUtils.parsePeriodType("days"), PeriodType.days());
    Assert.assertEquals(ConfigUtils.parsePeriodType("w"), PeriodType.weeks());
    Assert.assertEquals(ConfigUtils.parsePeriodType("week"), PeriodType.weeks());
    Assert.assertEquals(ConfigUtils.parsePeriodType("weeks"), PeriodType.weeks());
    Assert.assertEquals(ConfigUtils.parsePeriodType("mon"), PeriodType.months());
    Assert.assertEquals(ConfigUtils.parsePeriodType("mons"), PeriodType.months());
    Assert.assertEquals(ConfigUtils.parsePeriodType("month"), PeriodType.months());
    Assert.assertEquals(ConfigUtils.parsePeriodType("months"), PeriodType.months());
    Assert.assertEquals(ConfigUtils.parsePeriodType("y"), PeriodType.years());
    Assert.assertEquals(ConfigUtils.parsePeriodType("year"), PeriodType.years());
    Assert.assertEquals(ConfigUtils.parsePeriodType("years"), PeriodType.years());
    Assert.assertEquals(ConfigUtils.parsePeriodType("a"), PeriodType.years());
    Assert.assertEquals(ConfigUtils.parsePeriodType("ans"), PeriodType.years());
  }
}
