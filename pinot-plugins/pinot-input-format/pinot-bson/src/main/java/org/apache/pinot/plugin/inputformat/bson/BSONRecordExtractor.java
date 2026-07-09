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
package org.apache.pinot.plugin.inputformat.bson;

import com.google.common.collect.Maps;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;


/// Extracts a Pinot [GenericRow] from a decoded BSON document (`org.bson.Document`, which is a
/// `Map<String, Object>`). Values are the Java objects produced by the standard MongoDB
/// [org.bson.codecs.DocumentCodec].
///
/// **BSON type → Java output type:**
/// - `Double` / `Int32` / `Int64` / `Boolean` / `String` → same boxed type (pass-through)
/// - `Document` (embedded) → `Map<String, Object>` (values recursively converted)
/// - `Array` → `Object[]` (elements recursively converted)
/// - `ObjectId` → `String` (24-char hex)
/// - `DateTime` → `java.sql.Timestamp`
/// - `Decimal128` → `BigDecimal` (`NaN` / `Infinity` → `null`, as `BigDecimal` cannot represent them)
/// - `Binary` → `byte[]`
/// - `null` → `null`
///
/// Any other (rare, deprecated, or internal) BSON type falls back to `value.toString()`. The converted values
/// follow the shared `RecordExtractor` contract, so the downstream data-type transformer coerces them to the
/// declared column type.
public class BSONRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {

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

  @SuppressWarnings("unchecked")
  private static Object convert(Object value) {
    if (value instanceof Map) {
      return convertMap((Map<String, Object>) value);
    }
    if (value instanceof List) {
      return convertList((List<Object>) value);
    }
    if (value instanceof ObjectId) {
      return ((ObjectId) value).toHexString();
    }
    if (value instanceof Date) {
      return new Timestamp(((Date) value).getTime());
    }
    if (value instanceof Decimal128) {
      Decimal128 decimal128 = (Decimal128) value;
      // NaN / Infinity are legal Decimal128 values with no BigDecimal representation; surface them as null
      // instead of letting bigDecimalValue() throw.
      return decimal128.isNaN() || decimal128.isInfinite() ? null : decimal128.bigDecimalValue();
    }
    if (value instanceof Binary) {
      return ((Binary) value).getData();
    }
    // Double / Integer / Long / String / Boolean pass through; any other BSON type uses its string form.
    if (value instanceof Number || value instanceof String || value instanceof Boolean) {
      return value;
    }
    return value.toString();
  }

  private static Object[] convertList(List<Object> list) {
    int numValues = list.size();
    Object[] result = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      Object value = list.get(i);
      result[i] = value != null ? convert(value) : null;
    }
    return result;
  }

  private static Map<String, Object> convertMap(Map<String, Object> map) {
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      result.put(entry.getKey(), value != null ? convert(value) : null);
    }
    return result;
  }
}
