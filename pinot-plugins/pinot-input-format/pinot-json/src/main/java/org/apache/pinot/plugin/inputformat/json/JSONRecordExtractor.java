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
package org.apache.pinot.plugin.inputformat.json;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;


/// Extracts Pinot [GenericRow] from a parsed JSON `Map<String, Object>` (Jackson representation). JSON has
/// no native bytes / float type.
///
/// **JSON source type → Java input → Java output type:**
/// - `true` / `false` → `Boolean` → `Boolean`
/// - int that fits in 32 bits → `Integer` → `Integer`
/// - int that overflows 32 bits but fits in 64 → `Long` → `Long`
/// - int that overflows 64 bits → `BigInteger` → `BigDecimal` (Pinot has no `BigInteger` data type)
/// - decimal → `BigDecimal` → `BigDecimal` when parsed with Pinot's BigDecimal-aware JSON readers
/// - decimal → `Double` → `Double` for callers that still provide default Jackson-decoded maps
/// - string → `String` → `String`
/// - `null` → `null` → `null`
/// - array → `List` → `Object[]` (each element recursively converted)
/// - object → `Map` → `Map<String, Object>` (each value recursively converted)
public class JSONRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    if (_extractAll) {
      for (Map.Entry<String, Object> entry : from.entrySet()) {
        Object value = entry.getValue();
        to.putValue(entry.getKey(), value != null ? convert(value) : null);
      }
    } else {
      for (String fieldName : _fields) {
        Object value = from.get(fieldName);
        to.putValue(fieldName, value != null ? convert(value) : null);
      }
    }
    return to;
  }

  /// Walks a non-null Jackson-parsed value and produces the contract shape: `BigDecimal` for `BigInteger`
  /// (oversized ints), `Object[]` for JSON arrays, `Map<String, Object>` for JSON objects, pass-through for
  /// the other Jackson scalar types (`Boolean`, `Integer`, `Long`, `Double`, `BigDecimal`, `String`).
  private static Object convert(Object value) {
    // BigInteger widens (Pinot has no BigInteger type)
    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }
    // List
    if (value instanceof List) {
      //noinspection unchecked
      return convertList((List<Object>) value);
    }
    // Map
    if (value instanceof Map) {
      //noinspection unchecked
      return convertMap((Map<String, Object>) value);
    }
    // Single value pass-through (Boolean / Integer / Long / Double / BigDecimal / String)
    return value;
  }

  private static Object[] convertList(List<Object> list) {
    int n = list.size();
    Object[] result = new Object[n];
    for (int i = 0; i < n; i++) {
      Object v = list.get(i);
      result[i] = v != null ? convert(v) : null;
    }
    return result;
  }

  private static Map<String, Object> convertMap(Map<String, Object> map) {
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object v = entry.getValue();
      result.put(entry.getKey(), v != null ? convert(v) : null);
    }
    return result;
  }
}
