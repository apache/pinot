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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.utils.StringUtils;


/**
 * Extractor for ORC records
 */
public class ORCRecordExtractor implements RecordExtractor<ORCRecordReader.ORCValue[]> {

  private Set<String> _fields;

  @Override
  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _fields = fields;
  }

  @Override
  public GenericRow extract(ORCRecordReader.ORCValue[] from, GenericRow to) {
    for (ORCRecordReader.ORCValue orcValue : from) {
      String field = orcValue.getField();
      if (_fields.contains(field)) {
        TypeDescription fieldType = orcValue.getFieldType();
        TypeDescription.Category category = fieldType.getCategory();
        ColumnVector columnVector = orcValue.getColumnVector();
        int rowId = orcValue.getRowId();

        if (category == TypeDescription.Category.LIST) {
          // Multi-value field, extract to Object[]
          TypeDescription.Category childCategory = fieldType.getChildren().get(0).getCategory();
          ListColumnVector listColumnVector = (ListColumnVector) columnVector;
          if ((listColumnVector.noNulls || !listColumnVector.isNull[rowId])) {
            int offset = (int) listColumnVector.offsets[rowId];
            int length = (int) listColumnVector.lengths[rowId];
            List<Object> values = new ArrayList<>(length);
            for (int j = 0; j < length; j++) {
              Object value = extractSingleValue(listColumnVector.child, offset + j, childCategory);
              // NOTE: Only keep non-null values
              // TODO: Revisit
              if (value != null) {
                values.add(value);
              }
            }
            if (!values.isEmpty()) {
              to.putValue(field, values.toArray());
            } else {
              // NOTE: Treat empty list as null
              // TODO: Revisit
              to.putValue(field, null);
            }
          } else {
            to.putValue(field, null);
          }
        } else if (category == TypeDescription.Category.MAP) {
          // Map field
          List<TypeDescription> children = fieldType.getChildren();
          TypeDescription.Category keyCategory = children.get(0).getCategory();
          TypeDescription.Category valueCategory = children.get(1).getCategory();
          MapColumnVector mapColumnVector = (MapColumnVector) columnVector;
          if ((mapColumnVector.noNulls || !mapColumnVector.isNull[rowId])) {
            int offset = (int) mapColumnVector.offsets[rowId];
            int length = (int) mapColumnVector.lengths[rowId];
            Map<Object, Object> map = new HashMap<>();
            for (int j = 0; j < length; j++) {
              int childRowId = offset + j;
              Object key = extractSingleValue(mapColumnVector.keys, childRowId, keyCategory);
              Object value = extractSingleValue(mapColumnVector.values, childRowId, valueCategory);
              map.put(key, value);
            }
            to.putValue(field, map);
          } else {
            to.putValue(field, null);
          }
        } else {
          // Single-value field
          to.putValue(field, extractSingleValue(columnVector, rowId, category));
        }
      }
    }
    return to;
  }

  @Nullable
  private static Object extractSingleValue(ColumnVector columnVector, int rowId, TypeDescription.
      Category category) {

    switch (category) {
      case BOOLEAN:
        // Extract to String
        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[rowId]) {
          return Boolean.toString(longColumnVector.vector[rowId] == 1);
        } else {
          return null;
        }
      case BYTE:
      case SHORT:
      case INT:
        // Extract to Integer
        longColumnVector = (LongColumnVector) columnVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[rowId]) {
          return (int) longColumnVector.vector[rowId];
        } else {
          return null;
        }
      case LONG:
      case DATE:
        // Extract to Long
        longColumnVector = (LongColumnVector) columnVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[rowId]) {
          return longColumnVector.vector[rowId];
        } else {
          return null;
        }
      case TIMESTAMP:
        // Extract to Long
        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
        if (timestampColumnVector.noNulls || !timestampColumnVector.isNull[rowId]) {
          return timestampColumnVector.time[rowId];
        } else {
          return null;
        }
      case FLOAT:
        // Extract to Float
        DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
        if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[rowId]) {
          return (float) doubleColumnVector.vector[rowId];
        } else {
          return null;
        }
      case DOUBLE:
        // Extract to Double
        doubleColumnVector = (DoubleColumnVector) columnVector;
        if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[rowId]) {
          return doubleColumnVector.vector[rowId];
        } else {
          return null;
        }
      case STRING:
      case VARCHAR:
      case CHAR:
        // Extract to String
        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
        if (bytesColumnVector.noNulls || !bytesColumnVector.isNull[rowId]) {
          int length = bytesColumnVector.length[rowId];
          if (length != 0) {
            return StringUtils.decodeUtf8(bytesColumnVector.vector[rowId], bytesColumnVector.start[rowId], length);
          } else {
            // NOTE: Treat empty String as null
            // TODO: Revisit
            return null;
          }
        } else {
          return null;
        }
      case BINARY:
        // Extract to byte[]
        bytesColumnVector = (BytesColumnVector) columnVector;
        if (bytesColumnVector.noNulls || !bytesColumnVector.isNull[rowId]) {
          int length = bytesColumnVector.length[rowId];
          byte[] bytes = new byte[length];
          System.arraycopy(bytesColumnVector.vector[rowId], bytesColumnVector.start[rowId], bytes, 0, length);
          return bytes;
        } else {
          return null;
        }
      default:
        // Unsupported types
        throw new IllegalStateException("Unsupported field type: " + category);
    }
  }
}
