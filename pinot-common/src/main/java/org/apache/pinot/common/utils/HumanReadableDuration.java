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
package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HumanReadableDuration {
  private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+)\\s*(\\S+)");
  private static final Map<String, TimeUnit> SUFFIXES = createSuffixes();

  private HumanReadableDuration() {
  }

  private static Map<String, TimeUnit> createSuffixes() {
    Map<String, TimeUnit> suffixes = new HashMap<>();
    suffixes.put("ns", TimeUnit.NANOSECONDS);
    suffixes.put("nanosecond", TimeUnit.NANOSECONDS);
    suffixes.put("nanoseconds", TimeUnit.NANOSECONDS);
    suffixes.put("us", TimeUnit.MICROSECONDS);
    suffixes.put("microsecond", TimeUnit.MICROSECONDS);
    suffixes.put("microseconds", TimeUnit.MICROSECONDS);
    suffixes.put("ms", TimeUnit.MILLISECONDS);
    suffixes.put("millisecond", TimeUnit.MILLISECONDS);
    suffixes.put("milliseconds", TimeUnit.MILLISECONDS);
    suffixes.put("s", TimeUnit.SECONDS);
    suffixes.put("second", TimeUnit.SECONDS);
    suffixes.put("seconds", TimeUnit.SECONDS);
    suffixes.put("m", TimeUnit.MINUTES);
    suffixes.put("minute", TimeUnit.MINUTES);
    suffixes.put("minutes", TimeUnit.MINUTES);
    suffixes.put("h", TimeUnit.HOURS);
    suffixes.put("hour", TimeUnit.HOURS);
    suffixes.put("hours", TimeUnit.HOURS);
    suffixes.put("d", TimeUnit.DAYS);
    suffixes.put("day", TimeUnit.DAYS);
    suffixes.put("days", TimeUnit.DAYS);
    return suffixes;
  }

  public static Duration from(String durationStr) {
    final Matcher matcher = DURATION_PATTERN.matcher(durationStr);
    Preconditions.checkArgument(matcher.matches(), "Invalid duration string: %s", durationStr);

    final long count = Long.parseLong(matcher.group(1));
    final TimeUnit unit = SUFFIXES.get(matcher.group(2));
    if (unit == null) {
      throw new IllegalArgumentException(String.format("Unknown time-unit: %s", matcher.group(2)));
    }
    return Duration.of(count, unit.toChronoUnit());
  }
}
