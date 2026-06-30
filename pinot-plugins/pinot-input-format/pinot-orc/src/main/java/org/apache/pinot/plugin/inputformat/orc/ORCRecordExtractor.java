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
package org.apache.pinot.plugin.inputformat.orc;

import com.google.common.collect.Maps;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;

import static java.nio.charset.StandardCharsets.UTF_8;


/// Extracts a single ORC row into a [GenericRow]. Input is an [ORCRecordExtractor.Record] handle wrapping
/// a [VectorizedRowBatch] + schema + row index.
///
/// **ORC schema category → Java output type:**
/// - `BOOLEAN` → `Boolean`
/// - `BYTE` / `SHORT` / `INT` → `Integer` (small ints widen to `Integer`)
/// - `LONG` → `Long`
/// - `FLOAT` → `Float`
/// - `DOUBLE` → `Double`
/// - `DECIMAL` → `BigDecimal`
/// - `STRING` / `VARCHAR` / `CHAR` → `String`
/// - `BINARY` → `byte[]`
/// - `DATE` → [LocalDate] via [LocalDate#ofEpochDay] (TZ-independent, calendar-date semantics)
/// - `TIMESTAMP` / `TIMESTAMP_INSTANT` → [Timestamp] preserving full sub-second nanos from [TimestampColumnVector]
/// - `LIST<X>` → `Object[]` (null elements preserved; empty list surfaces as empty `Object[]`)
/// - `MAP<K, V>` → `Map<String, Object>` (keys stringified via [BaseRecordExtractor#stringifyMapKey])
/// - `STRUCT<...>` → `Map<String, Object>`
/// - any nullable column with `isNull[rowId]` set → `null`
public class ORCRecordExtractor extends BaseRecordExtractor<ORCRecordExtractor.Record> {

  /// One ORC row's worth of state. The reader allocates a single instance and calls [#set] on each row
  /// advance — no per-row allocation.
  public static final class Record {
    VectorizedRowBatch _batch;
    TypeDescription _schema;
    int _rowId;

    public void set(VectorizedRowBatch batch, TypeDescription schema, int rowId) {
      _batch = batch;
      _schema = schema;
      _rowId = rowId;
    }
  }

  // Cached `_fields ∩ source-schema` → child-index map for the include-list path. The ORC source schema is
  // fixed for the lifetime of the reader (and therefore the extractor instance), so we build this lazily on
  // the first row and reuse it from then on — no per-row schema-identity check needed. Iterating this map
  // directly drives the extraction loop, so no per-row HashMap lookup against the source schema either.
  private Map<String, Integer> _fieldToIndexMap;

  @Override
  public GenericRow extract(Record from, GenericRow to) {
    TypeDescription schema = from._schema;
    List<TypeDescription> fieldTypes = schema.getChildren();
    ColumnVector[] cols = from._batch.cols;
    int rowId = from._rowId;
    if (_extractAll) {
      List<String> fieldNames = schema.getFieldNames();
      int numFields = fieldNames.size();
      for (int i = 0; i < numFields; i++) {
        String fieldName = fieldNames.get(i);
        to.putValue(fieldName, extractValue(fieldName, cols[i], fieldTypes.get(i), rowId));
      }
    } else {
      if (_fieldToIndexMap == null) {
        _fieldToIndexMap = buildFieldToIndexMap(schema, _fields);
      }
      for (Map.Entry<String, Integer> entry : _fieldToIndexMap.entrySet()) {
        int index = entry.getValue();
        to.putValue(entry.getKey(), extractValue(entry.getKey(), cols[index], fieldTypes.get(index), rowId));
      }
    }
    return to;
  }

  private static Map<String, Integer> buildFieldToIndexMap(TypeDescription schema, Set<String> fields) {
    Map<String, Integer> map = Maps.newHashMapWithExpectedSize(fields.size());
    List<String> fieldNames = schema.getFieldNames();
    int numFieldsInSchema = fieldNames.size();
    for (int i = 0; i < numFieldsInSchema; i++) {
      String name = fieldNames.get(i);
      if (fields.contains(name)) {
        map.put(name, i);
      }
    }
    return map;
  }

  /// Extracts the value at `rowId` from `columnVector`, dispatching by `fieldType.getCategory()`. Recurses
  /// into [#extractValue] for nested complex types and falls through to [#extractSingleValue] for primitives.
  @Nullable
  private static Object extractValue(String field, ColumnVector columnVector, TypeDescription fieldType, int rowId) {
    if (columnVector.isRepeating) {
      rowId = 0;
    }
    if (!columnVector.noNulls && columnVector.isNull[rowId]) {
      return null;
    }
    TypeDescription.Category category = fieldType.getCategory();
    switch (category) {
      case LIST: {
        TypeDescription childType = fieldType.getChildren().get(0);
        ListColumnVector listColumnVector = (ListColumnVector) columnVector;
        int offset = (int) listColumnVector.offsets[rowId];
        int length = (int) listColumnVector.lengths[rowId];
        Object[] values = new Object[length];
        for (int j = 0; j < length; j++) {
          values[j] = extractValue(field, listColumnVector.child, childType, offset + j);
        }
        return values;
      }
      case MAP: {
        // Map keys go straight to `extractSingleValue` instead of `extractValue` — we deliberately skip the
        // `isRepeating` / null guards that `extractValue` performs. ORC's format invariants guarantee map
        // keys are non-null, and the Apache ORC reader populates each entry individually instead of
        // collapsing the child key vector to `isRepeating = true`. Adding the guards here would be defensive
        // against API states no real reader produces. Keys are stringified via [#stringifyMapKey] per the
        // `Map<String, Object>` contract — TIMESTAMP / BINARY / TINYINT etc. ORC key types all serialize
        // through the same shared helper as every other format.
        List<TypeDescription> children = fieldType.getChildren();
        TypeDescription.Category keyCategory = children.get(0).getCategory();
        TypeDescription valueType = children.get(1);
        MapColumnVector mapColumnVector = (MapColumnVector) columnVector;
        int offset = (int) mapColumnVector.offsets[rowId];
        int length = (int) mapColumnVector.lengths[rowId];
        Map<String, Object> map = new HashMap<>();
        for (int j = 0; j < length; j++) {
          int childRowId = offset + j;
          Object key = extractSingleValue(field, mapColumnVector.keys, childRowId, keyCategory);
          Object value = extractValue(field, mapColumnVector.values, valueType, childRowId);
          map.put(stringifyMapKey(key), value);
        }
        return map;
      }
      case STRUCT: {
        StructColumnVector structColumnVector = (StructColumnVector) columnVector;
        List<String> childrenFieldNames = fieldType.getFieldNames();
        List<TypeDescription> childrenFieldTypes = fieldType.getChildren();
        Map<String, Object> convertedMap = new HashMap<>();
        for (int i = 0; i < childrenFieldNames.size(); i++) {
          convertedMap.put(childrenFieldNames.get(i),
              extractValue(childrenFieldNames.get(i), structColumnVector.fields[i], childrenFieldTypes.get(i), rowId));
        }
        return convertedMap;
      }
      default:
        return extractSingleValue(field, columnVector, rowId, category);
    }
  }

  /// Extracts a primitive value at `rowId` from `columnVector`. The MAP-key call site relies on ORC's
  /// format invariant that keys are non-null, and on the Apache ORC reader not setting `isRepeating` on
  /// child key vectors of a MAP — so the `isRepeating` / null guards in [#extractValue] are intentionally
  /// not duplicated here.
  private static Object extractSingleValue(String field, ColumnVector columnVector, int rowId,
      TypeDescription.Category category) {
    switch (category) {
      case BOOLEAN:
        return ((LongColumnVector) columnVector).vector[rowId] == 1;
      case BYTE:
      case SHORT:
      case INT:
        return (int) ((LongColumnVector) columnVector).vector[rowId];
      case LONG:
        return ((LongColumnVector) columnVector).vector[rowId];
      case DATE: {
        // ORC `DATE` is days-since-epoch in a `LongColumnVector`. Surface as [LocalDate] (TZ-independent,
        // calendar-date semantics).
        long days = ((LongColumnVector) columnVector).vector[rowId];
        return LocalDate.ofEpochDay(days);
      }
      case TIMESTAMP:
      case TIMESTAMP_INSTANT: {
        // `time[rowId]` is epoch millis truncated to second precision; sub-second nanos (0..999_999_999)
        // live in `nanos[rowId]`. `TIMESTAMP_INSTANT` (UTC instant) shares the same vector layout as
        // `TIMESTAMP` (local). Construct a `Timestamp` and set the full nanos field to preserve precision.
        TimestampColumnVector tsv = (TimestampColumnVector) columnVector;
        Timestamp ts = new Timestamp(tsv.time[rowId]);
        ts.setNanos(tsv.nanos[rowId]);
        return ts;
      }
      case FLOAT:
        return (float) ((DoubleColumnVector) columnVector).vector[rowId];
      case DOUBLE:
        return ((DoubleColumnVector) columnVector).vector[rowId];
      case STRING:
      case VARCHAR:
      case CHAR: {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
        return new String(bytesColumnVector.vector[rowId], bytesColumnVector.start[rowId],
            bytesColumnVector.length[rowId], UTF_8);
      }
      case BINARY: {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
        int length = bytesColumnVector.length[rowId];
        byte[] bytes = new byte[length];
        System.arraycopy(bytesColumnVector.vector[rowId], bytesColumnVector.start[rowId], bytes, 0, length);
        return bytes;
      }
      case DECIMAL:
        return ((DecimalColumnVector) columnVector).vector[rowId].getHiveDecimal().bigDecimalValue();
      default:
        throw new IllegalStateException("Unsupported field type: " + category + " for field: " + field);
    }
  }
}
