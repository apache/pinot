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

package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.Period;
import org.joda.time.PeriodType;


/**
 * Utility class for (semi-)safely extracting complex config values.
 */
public class ConfigUtils {
  private static final Pattern PATTERN_PERIOD = Pattern.compile("([0-9]+)\\s*(\\S*)");

  private ConfigUtils() {
    // left blank
  }

  /**
   * Returns {@code value} casted as typed List (ArrayList). If {@code value} is null,
   * returns an empty collection.
   * <br/><b>NOTE:</b> this method does not guarantee type safety of the contained values
   *
   * @param value value to cast
   * @param <T> list item type
   * @throws IllegalArgumentException if value cannot be casted to List
   * @return value casted as List
   */
  public static <T> List<T> getList(Object value) {
    return getList(value, new ArrayList<T>());
  }

  /**
   * Returns {@code value} casted as typed List (ArrayList). If {@code value} is null,
   * returns {@code defaultValue}.
   * <br/><b>NOTE:</b> this method does not guarantee type safety of the contained values
   *
   * @param value value to cast
   * @param defaultValue default value to return on {@code null} value
   * @param <T> list item type
   * @throws IllegalArgumentException if value cannot be casted to List
   * @return value casted as List
   */
  public static <T> List<T> getList(Object value, List<T> defaultValue) {
    if (value == null) {
      return new ArrayList<>(defaultValue);
    }

    if (!(value instanceof Collection)) {
      throw new IllegalArgumentException(String.format("Could not parse as collection: %s", value));
    }

    return new ArrayList<>((Collection<T>) value);
  }

  /**
   * Returns {@code value} casted as typed Map (HashMap). If {@code value} is null,
   * returns an empty map.
   * <br/><b>NOTE:</b> this method does not guarantee type safety of the contained keys and values
   *
   * @param value value to cast
   * @param <K> map key type
   * @param <V> map value type
   * @throws IllegalArgumentException if value cannot be casted to Map
   * @return value casted as map
   */
  public static <K, V> Map<K, V> getMap(Object value) {
    return getMap(value, new HashMap<K, V>());
  }

  /**
   * Returns {@code value} casted as typed Map (HashMap). If {@code value} is null,
   * returns {@code defaultValue}.
   * <br/><b>NOTE:</b> this method does not guarantee type safety of the contained keys and values
   *
   * @param value value to cast
   * @param defaultValue default value to return on {@code null} value
   * @param <K> map key type
   * @param <V> map value type
   * @throws IllegalArgumentException if value cannot be casted to Map
   * @return value casted as map
   */
  public static <K, V> Map<K, V> getMap(Object value, Map<K, V> defaultValue) {
    if (value == null) {
      return new HashMap<>(defaultValue);
    }

    if (!(value instanceof Map)) {
      throw new IllegalArgumentException(String.format("Could not parse as map: %s", value));
    }

    return new HashMap<>((Map<K, V>) value);
  }

  /**
   * Returns {@code value} casted as List via {@code getList(Object value)} and parsed via {@code getLongs(Collection&lt;Number&gt;)}.
   *
   * @param value value casted as list and parsed
   * @return equivalent collection of Long without nulls
   */
  public static List<Long> getLongs(Object value) {
    return getLongs(ConfigUtils.<Number>getList(value));
  }

  /**
   * Returns a Collection of {@code Number} as a Collection of {@code Long}, skipping {@code null}
   * values. If {@code numbers} is {@code null}, returns an empty collection.
   *
   * @param numbers collection of Number
   * @return equivalent collection of Long without nulls
   */
  public static List<Long> getLongs(Collection<Number> numbers) {
    if (numbers == null) {
      return new ArrayList<>();
    }

    List<Long> output = new ArrayList<>();
    for (Number n : numbers) {
      if (n == null) {
        continue;
      }
      output.add(n.longValue());
    }
    return output;
  }

  /**
   * Returns {@code value} casted as Map via {@code getMap(Object value)} and parsed via {@code getMultimap(Map&lt;K, Collection&lt;V&gt;&gt;)}.
   *
   * @param value value casted as map and parsed
   * @return equivalent multimap
   */
  public static <K, V> Multimap<K, V> getMultimap(Object value) {
    return getMultimap(ConfigUtils.<K, Collection<V>>getMap(value));
  }

  /**
   * Returns a map with nested collection as a {@code Multimap} with matching types. if {@code nestedMap}
   * is {@code null} returns an empty Multimap.
   *
   * @param nestedMap map with nested collection values
   * @param <K> map key type
   * @param <V> map value type
   * @return equivalent {@code Multimap}
   */
  public static <K, V> Multimap<K, V> getMultimap(Map<K, Collection<V>> nestedMap) {
    if (nestedMap == null) {
      return ArrayListMultimap.create();
    }

    Multimap<K, V> output = ArrayListMultimap.create();
    for (Map.Entry<K, Collection<V>> entry : nestedMap.entrySet()) {
      for (V value : entry.getValue()) {
        output.put(entry.getKey(), value);
      }
    }
    return output;
  }

  /**
   * Helper for parsing a period string from config (e.g. '3 days', '1min', '3600000')
   *
   * @param period
   * @return
   */
  public static Period parsePeriod(String period) {
    Matcher m = PATTERN_PERIOD.matcher(period);
    if (!m.find()) {
      throw new IllegalArgumentException(String.format("Could not parse period expression '%s'", period));
    }

    int size = Integer.valueOf(m.group(1).trim());

    PeriodType t = PeriodType.millis();
    if (m.group(2).length() > 0) {
      t = parsePeriodType(m.group(2).trim());
    }

    return new Period().withFieldAdded(t.getFieldType(0), size);
  }

  /**
   * Helper for heuristically parsing period unit from config (e.g. 'millis', 'hour', 'd')
   *
   * @param type period type string
   * @return PeriodType
   */
  static PeriodType parsePeriodType(String type) {
    type = type.toLowerCase();

    if (type.startsWith("y") || type.startsWith("a")) {
      return PeriodType.years();
    }
    if (type.startsWith("mo")) {
      return PeriodType.months();
    }
    if (type.startsWith("w")) {
      return PeriodType.weeks();
    }
    if (type.startsWith("d")) {
      return PeriodType.days();
    }
    if (type.startsWith("h")) {
      return PeriodType.hours();
    }
    if (type.startsWith("s")) {
      return PeriodType.seconds();
    }
    if (type.startsWith("mill") || type.startsWith("ms")) {
      return PeriodType.millis();
    }
    if (type.startsWith("m")) {
      return PeriodType.minutes();
    }

    throw new IllegalArgumentException(String.format("Invalid period type '%s'", type));
  }
}
