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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.StringUtils;


/**
 * Record Reader for ORC files.
 * <p>The following ORC types are supported:
 * <ul>
 *   <li>BOOLEAN -> String</li> TODO: -> Boolean?
 *   <li>BYTE, SHORT, INT -> Integer</li>
 *   <li>LONG, DATE, TIMESTAMP -> Long</li>
 *   <li>FLOAT -> Float</li>
 *   <li>DOUBLE -> Double</li>
 *   <li>STRING, VARCHAR, CHAR -> String</li>
 *   <li>BINARY -> byte[]</li>
 *   <li>LIST -> Object[] of the supported types</li> TODO: -> List?
 *   <li>MAP -> Map of the supported types</li>
 * </ul>
 */
public class ORCRecordReader implements RecordReader {
  private List<String> _orcFields;
  private List<TypeDescription> _orcFieldTypes;
  private boolean[] _includeOrcFields;
  private org.apache.orc.RecordReader _orcRecordReader;
  private VectorizedRowBatch _rowBatch;
  private boolean _hasNext;
  private int _nextRowId;

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    Configuration configuration = new Configuration();
    Reader orcReader = OrcFile.createReader(new Path(dataFile.getAbsolutePath()),
        OrcFile.readerOptions(configuration).filesystem(FileSystem.getLocal(configuration)));
    TypeDescription orcSchema = orcReader.getSchema();
    Preconditions
        .checkState(orcSchema.getCategory() == TypeDescription.Category.STRUCT, "ORC schema must be of type: STRUCT");
    _orcFields = orcSchema.getFieldNames();
    _orcFieldTypes = orcSchema.getChildren();

    // Only read the required fields
    int numOrcFields = _orcFields.size();
    _includeOrcFields = new boolean[numOrcFields];
    // NOTE: Include for ORC reader uses field id as the index
    boolean[] orcReaderInclude = new boolean[orcSchema.getMaximumId() + 1];
    orcReaderInclude[orcSchema.getId()] = true;
    for (int i = 0; i < numOrcFields; i++) {
      String field = _orcFields.get(i);
      if (fieldsToRead.contains(field)) {
        TypeDescription fieldType = _orcFieldTypes.get(i);
        TypeDescription.Category category = fieldType.getCategory();
        if (category == TypeDescription.Category.LIST) {
          // Multi-value field
          TypeDescription.Category childCategory = fieldType.getChildren().get(0).getCategory();
          Preconditions.checkState(isSupportedSingleValueType(childCategory), "Illegal multi-value field type: %s (field %s)",
              childCategory, field);
          // NOTE: LIST is stored as 2 vectors
          int fieldId = fieldType.getId();
          orcReaderInclude[fieldId] = true;
          orcReaderInclude[fieldId + 1] = true;
        } else if (category == TypeDescription.Category.MAP) {
          // Map field
          List<TypeDescription> children = fieldType.getChildren();
          TypeDescription.Category keyCategory = children.get(0).getCategory();
          Preconditions
              .checkState(isSupportedSingleValueType(keyCategory), "Illegal map key field type: %s (field %s)", keyCategory, field);
          TypeDescription.Category valueCategory = children.get(1).getCategory();
          Preconditions
              .checkState(isSupportedSingleValueType(valueCategory), "Illegal map value field type: %s (field %s)", valueCategory, field);
          // NOTE: MAP is stored as 3 vectors
          int fieldId = fieldType.getId();
          orcReaderInclude[fieldId] = true;
          orcReaderInclude[fieldId + 1] = true;
          orcReaderInclude[fieldId + 2] = true;
        } else {
          // Single-value field
          Preconditions
              .checkState(isSupportedSingleValueType(category), "Illegal single-value field type: %s (field %s)", category, field);
          orcReaderInclude[fieldType.getId()] = true;
        }
        _includeOrcFields[i] = true;
      }
    }

    _orcRecordReader = orcReader.rows(new Reader.Options().include(orcReaderInclude));
    _rowBatch = orcSchema.createRowBatch();
    _hasNext = _orcRecordReader.nextBatch(_rowBatch);
    _nextRowId = 0;
  }

  private static boolean isSupportedSingleValueType(TypeDescription.Category category) {
    switch (category) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DATE:
      case TIMESTAMP:
      case BINARY:
      case VARCHAR:
      case CHAR:
        return true;
      default:
        return false;
    }
  }

  @Override
  public boolean hasNext() {
    return _hasNext;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    int numFields = _orcFields.size();
    for (int i = 0; i < numFields; i++) {
      if (!_includeOrcFields[i]) {
        continue;
      }
      String field = _orcFields.get(i);
      TypeDescription fieldType = _orcFieldTypes.get(i);
      TypeDescription.Category category = fieldType.getCategory();
      if (category == TypeDescription.Category.LIST) {
        // Multi-value field, extract to Object[]
        TypeDescription.Category childCategory = fieldType.getChildren().get(0).getCategory();
        ListColumnVector listColumnVector = (ListColumnVector) _rowBatch.cols[i];
        int rowId = listColumnVector.isRepeating ? 0 : _nextRowId;
        if ((listColumnVector.noNulls || !listColumnVector.isNull[rowId])) {
          int offset = (int) listColumnVector.offsets[rowId];
          int length = (int) listColumnVector.lengths[rowId];
          List<Object> values = new ArrayList<>(length);
          for (int j = 0; j < length; j++) {
            Object value = extractSingleValue(field, listColumnVector.child, offset + j, childCategory);
            // NOTE: Only keep non-null values
            // TODO: Revisit
            if (value != null) {
              values.add(value);
            }
          }
          if (!values.isEmpty()) {
            reuse.putValue(field, values.toArray());
          } else {
            // NOTE: Treat empty list as null
            // TODO: Revisit
            reuse.putValue(field, null);
          }
        } else {
          reuse.putValue(field, null);
        }
      } else if (category == TypeDescription.Category.MAP) {
        // Map field
        List<TypeDescription> children = fieldType.getChildren();
        TypeDescription.Category keyCategory = children.get(0).getCategory();
        TypeDescription.Category valueCategory = children.get(1).getCategory();
        MapColumnVector mapColumnVector = (MapColumnVector) _rowBatch.cols[i];
        int rowId = mapColumnVector.isRepeating ? 0 : _nextRowId;
        if ((mapColumnVector.noNulls || !mapColumnVector.isNull[rowId])) {
          int offset = (int) mapColumnVector.offsets[rowId];
          int length = (int) mapColumnVector.lengths[rowId];
          Map<Object, Object> map = new HashMap<>();
          for (int j = 0; j < length; j++) {
            int childRowId = offset + j;
            Object key = extractSingleValue(field, mapColumnVector.keys, childRowId, keyCategory);
            Object value = extractSingleValue(field, mapColumnVector.values, childRowId, valueCategory);
            map.put(key, value);
          }
          reuse.putValue(field, map);
        } else {
          reuse.putValue(field, null);
        }
      } else {
        // Single-value field
        reuse.putValue(field, extractSingleValue(field, _rowBatch.cols[i], _nextRowId, category));
      }
    }

    if (++_nextRowId == _rowBatch.size) {
      _hasNext = _orcRecordReader.nextBatch(_rowBatch);
      _nextRowId = 0;
    }
    return reuse;
  }

  @Nullable
  private static Object extractSingleValue(String field, ColumnVector columnVector, int rowId, TypeDescription.Category category) {
    if (columnVector.isRepeating) {
      rowId = 0;
    }
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
        throw new IllegalStateException("Unsupported field type: " + category + " for field: " + field);
    }
  }

  @Override
  public void rewind()
      throws IOException {
    _orcRecordReader.seekToRow(0);
    _hasNext = _orcRecordReader.nextBatch(_rowBatch);
    _nextRowId = 0;
  }

  @Override
  public void close()
      throws IOException {
    _orcRecordReader.close();
  }
}
