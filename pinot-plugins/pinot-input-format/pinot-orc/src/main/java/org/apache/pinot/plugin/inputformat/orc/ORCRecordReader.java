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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


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
  private static final String EXTENSION = "orc";

  private List<String> _orcFields;
  private List<TypeDescription> _orcFieldTypes;
  private boolean[] _includeOrcFields;
  private org.apache.orc.RecordReader _orcRecordReader;
  private VectorizedRowBatch _rowBatch;
  private boolean _hasNext;
  private int _nextRowId;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    Configuration configuration = new Configuration();
    File orcFile = RecordReaderUtils.unpackIfRequired(dataFile, EXTENSION);
    Reader orcReader = OrcFile.createReader(new Path(orcFile.getAbsolutePath()),
        OrcFile.readerOptions(configuration).filesystem(FileSystem.getLocal(configuration)));
    TypeDescription orcSchema = orcReader.getSchema();
    Preconditions.checkState(orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "ORC schema must be of type: STRUCT");
    _orcFields = orcSchema.getFieldNames();
    _orcFieldTypes = orcSchema.getChildren();

    // Only read the required fields
    int numOrcFields = _orcFields.size();
    _includeOrcFields = new boolean[numOrcFields];
    // NOTE: Include for ORC reader uses field id as the index
    boolean[] orcReaderInclude = new boolean[orcSchema.getMaximumId() + 1];
    orcReaderInclude[orcSchema.getId()] = true;
    boolean extractAllFields = fieldsToRead == null || fieldsToRead.isEmpty();
    for (int i = 0; i < numOrcFields; i++) {
      String field = _orcFields.get(i);
      if (extractAllFields || fieldsToRead.contains(field)) {
        initFieldsToRead(orcReaderInclude, _orcFieldTypes.get(i), field);
        _includeOrcFields[i] = true;
      }
    }

    _orcRecordReader = orcReader.rows(new Reader.Options().include(orcReaderInclude));
    _rowBatch = orcSchema.createRowBatch();
    _hasNext = _orcRecordReader.nextBatch(_rowBatch);
    _nextRowId = 0;
  }

  /**
   * Initializes the fields to be read using the field ID. Traverses children fields in the case
   * of struct, list, or map types.
   *
   * @param orcReaderInclude IDs of the fields to be read
   * @param fieldType contains the field ID and its children TypeDescriptions (if not a single value field)
   * @param field name of the field being read
   */
  private void initFieldsToRead(boolean[] orcReaderInclude, TypeDescription fieldType, String field) {
    int fieldId = fieldType.getId();
    orcReaderInclude[fieldId] = true; // Include ID for top level field
    TypeDescription.Category category = fieldType.getCategory();
    if (category == TypeDescription.Category.LIST) {
      // Lists always have a single child column for its elements
      TypeDescription childFieldType = fieldType.getChildren().get(0);
      initFieldsToRead(orcReaderInclude, childFieldType, field);
    } else if (category == TypeDescription.Category.MAP) {
      orcReaderInclude[fieldId + 1] = true; //include ID for the map's keys

      // Maps always have two child columns for its keys and values
      List<TypeDescription> children = fieldType.getChildren();
      TypeDescription.Category keyCategory = children.get(0).getCategory();
      Preconditions.checkState(isSupportedSingleValueType(keyCategory), "Illegal map key field type: %s (field %s)",
          keyCategory, field);
      initFieldsToRead(orcReaderInclude, children.get(1), field);
    } else if (category == TypeDescription.Category.STRUCT) {
      List<String> childrenFieldNames = fieldType.getFieldNames();
      List<TypeDescription> childrenFieldTypes = fieldType.getChildren();
      // Struct columns have one child column for each field of the struct
      for (int i = 0; i < childrenFieldNames.size(); i++) {
        initFieldsToRead(orcReaderInclude, childrenFieldTypes.get(i), childrenFieldNames.get(i));
      }
    } else {
      // Single-value field
      Preconditions.checkState(isSupportedSingleValueType(category), "Illegal single-value field type: %s (field %s)",
          category, field);
    }
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
      case DECIMAL:
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
      reuse.putValue(field, extractValue(field, _rowBatch.cols[i], fieldType, _nextRowId));
    }

    if (_nextRowId == _rowBatch.size - 1) {
      _hasNext = _orcRecordReader.nextBatch(_rowBatch);
      _nextRowId = 0;
    } else {
      _nextRowId++;
    }
    return reuse;
  }

  /**
   * Extracts the values for a given column vector.
   *
   * @param field name of the field being extracted
   * @param columnVector contains values of the field and its sub-types
   * @param fieldType information about the field such as the category (STRUCT, LIST, MAP, INT, etc)
   * @param rowId the ID of the row value being extracted
   * @return extracted row value from the column
   */
  @Nullable
  private Object extractValue(String field, ColumnVector columnVector, TypeDescription fieldType, int rowId) {
    TypeDescription.Category category = fieldType.getCategory();

    if (category == TypeDescription.Category.LIST) {
      TypeDescription childType = fieldType.getChildren().get(0);
      ListColumnVector listColumnVector = (ListColumnVector) columnVector;
      if (columnVector.isRepeating) {
        rowId = 0;
      }
      if ((listColumnVector.noNulls || !listColumnVector.isNull[rowId])) {
        int offset = (int) listColumnVector.offsets[rowId];
        int length = (int) listColumnVector.lengths[rowId];
        List<Object> values = new ArrayList<>(length);
        for (int j = 0; j < length; j++) {
          Object value = extractValue(field, listColumnVector.child, childType, offset + j);
          // NOTE: Only keep non-null values
          if (value != null) {
            values.add(value);
          }
        }
        if (!values.isEmpty()) {
          return values.toArray();
        } else {
          // NOTE: Treat empty list as null
          return null;
        }
      } else {
        return null;
      }
    } else if (category == TypeDescription.Category.MAP) {
      List<TypeDescription> children = fieldType.getChildren();
      TypeDescription.Category keyCategory = children.get(0).getCategory();
      TypeDescription valueType = children.get(1);
      MapColumnVector mapColumnVector = (MapColumnVector) columnVector;

      if (columnVector.isRepeating) {
        rowId = 0;
      }

      if ((mapColumnVector.noNulls || !mapColumnVector.isNull[rowId])) {
        int offset = (int) mapColumnVector.offsets[rowId];
        int length = (int) mapColumnVector.lengths[rowId];
        Map<Object, Object> map = new HashMap<>();
        for (int j = 0; j < length; j++) {
          int childRowId = offset + j;
          Object key = extractSingleValue(field, mapColumnVector.keys, childRowId, keyCategory);
          Object value = extractValue(field, mapColumnVector.values, valueType, childRowId);
          map.put(key, value);
        }

        return map;
      } else {
        return null;
      }
    } else if (category == TypeDescription.Category.STRUCT) {
      StructColumnVector structColumnVector = (StructColumnVector) columnVector;
      if (!structColumnVector.isNull[rowId]) {
        List<TypeDescription> childrenFieldTypes = fieldType.getChildren();
        List<String> childrenFieldNames = fieldType.getFieldNames();

        Map<Object, Object> convertedMap = new HashMap<>();
        for (int i = 0; i < childrenFieldNames.size(); i++) {
          convertedMap.put(childrenFieldNames.get(i),
              extractValue(childrenFieldNames.get(i), structColumnVector.fields[i], childrenFieldTypes.get(i), rowId));
        }
        return convertedMap;
      } else {
        return null;
      }
    } else {
      // Handle single-value field
      return extractSingleValue(field, columnVector, rowId, category);
    }
  }

  @Nullable
  private static Object extractSingleValue(String field, ColumnVector columnVector, int rowId,
      TypeDescription.Category category) {
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
          return new String(bytesColumnVector.vector[rowId], bytesColumnVector.start[rowId], length, UTF_8);
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
      case DECIMAL:
        // Extract to string
        DecimalColumnVector decimalColumnVector = (DecimalColumnVector) columnVector;
        if (decimalColumnVector.noNulls || !decimalColumnVector.isNull[rowId]) {
          StringBuilder stringBuilder = new StringBuilder();
          decimalColumnVector.stringifyValue(stringBuilder, rowId);
          return stringBuilder.toString();
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
