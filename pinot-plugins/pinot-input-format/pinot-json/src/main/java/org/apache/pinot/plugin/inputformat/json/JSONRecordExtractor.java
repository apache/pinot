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

import java.util.Map;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;


/// Extracts Pinot [GenericRow] from a parsed JSON `Map<String, Object>` (Jackson representation). JSON has
/// no native bytes / float / big-decimal type.
///
/// **JSON source type → Java input → Java output type:**
/// - JSON `true` / `false` → `Boolean` → `Boolean`
/// - JSON int that fits in 32 bits → `Integer` → `Integer`
/// - JSON int that overflows 32 bits but fits in 64 → `Long` → `Long`
/// - JSON int that overflows 64 bits → `BigInteger` → `BigDecimal` (widened by [BaseRecordExtractor])
/// - JSON decimal → `Double` → `Double` (never `Float` or `BigDecimal` with default Jackson config)
/// - JSON string → `String` → `String`
/// - JSON `null` → `null` → `null`
/// - JSON array → `List` → `Object[]` (each element recursively converted)
/// - JSON object → `Map` → `Map<String, Object>` (each value recursively converted)
public class JSONRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    if (_extractAll) {
      for (Map.Entry<String, Object> entry : from.entrySet()) {
        Object value = entry.getValue();
        if (value != null) {
          value = convert(value);
        }
        to.putValue(entry.getKey(), value);
      }
    } else {
      for (String fieldName : _fields) {
        Object value = from.get(fieldName);
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }
    }
    return to;
  }
}
