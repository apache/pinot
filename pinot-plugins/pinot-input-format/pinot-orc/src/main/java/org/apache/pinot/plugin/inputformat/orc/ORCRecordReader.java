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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


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
public class ORCRecordReader implements RecordReader<ORCRecordReader.ORCValue[]> {
  private Schema _schema;
  private List<String> _orcFields;
  private List<TypeDescription> _orcFieldTypes;
  private TypeDescription _orcSchema;
  private org.apache.orc.RecordReader _orcRecordReader;
  private VectorizedRowBatch _rowBatch;
  private boolean _hasNext;
  private int _nextRowId;

  private ORCValue[] _reusableORCRecord;
  private int _numFields;

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _schema = schema;

    Reader orcReader =
        OrcFile.createReader(new Path(dataFile.getAbsolutePath()), OrcFile.readerOptions(new Configuration()));
    _orcSchema = orcReader.getSchema();
    Preconditions
        .checkState(_orcSchema.getCategory() == TypeDescription.Category.STRUCT, "ORC schema must be of type: STRUCT");
    _orcFields = _orcSchema.getFieldNames();
    _orcFieldTypes = _orcSchema.getChildren();
    _numFields = _orcFields.size();

    _orcRecordReader = orcReader.rows(new Reader.Options().schema(orcReader.getSchema()));
    _rowBatch = _orcSchema.createRowBatch();
    _hasNext = _orcRecordReader.nextBatch(_rowBatch);
    _nextRowId = 0;

    _reusableORCRecord = new ORCValue[_numFields];
    for (int i = 0; i < _numFields; i++) {
      TypeDescription fieldType = _orcFieldTypes.get(i);
      TypeDescription.Category category = fieldType.getCategory();
      if (category == TypeDescription.Category.LIST) {
        TypeDescription.Category childCategory = fieldType.getChildren().get(0).getCategory();
        Preconditions
            .checkState(isSupportedSingleValueType(childCategory), "Illegal multi-value field type: %s", childCategory);
      } else if (category == TypeDescription.Category.MAP) {
        List<TypeDescription> children = fieldType.getChildren();
        TypeDescription.Category keyCategory = children.get(0).getCategory();
        Preconditions
            .checkState(isSupportedSingleValueType(keyCategory), "Illegal map key field type: %s", keyCategory);
        TypeDescription.Category valueCategory = children.get(1).getCategory();
        Preconditions
            .checkState(isSupportedSingleValueType(valueCategory), "Illegal map value field type: %s", valueCategory);
      } else {
        // Single-value field
        Preconditions.checkState(isSupportedSingleValueType(category), "Illegal single-value field type: %s", category);
      }

      ORCValue orcValue = new ORCValue();
      orcValue.setField(_orcFields.get(i));
      orcValue.setFieldType(fieldType);
      _reusableORCRecord[i] = orcValue;
    }
  }

  @Override
  public boolean hasNext() {
    return _hasNext;
  }

  @Override
  public ORCValue[] next(ORCValue[] orcRecord) {
    return next(_reusableORCRecord);
  }

  @Override
  public ORCValue[] next()
      throws IOException {
    if (_nextRowId == 0) {
      for (int i = 0; i < _numFields; i++) {
        _reusableORCRecord[i].setColumnVector(_rowBatch.cols[i]);
      }
    }

    for (int i = 0; i < _numFields; i++) {
      if (!_reusableORCRecord[i].getColumnVector().isRepeating) {
        _reusableORCRecord[i].setRowId(_nextRowId);
      }
    }

    if (++_nextRowId == _rowBatch.size) {
      _hasNext = _orcRecordReader.nextBatch(_rowBatch);
      _nextRowId = 0;
    }
    return _reusableORCRecord;
  }

  static class ORCValue {
    private String _field;
    private TypeDescription _fieldType;
    private ColumnVector _columnVector;
    private int _rowId = 0;

    public int getRowId() {
      return _rowId;
    }

    public void setRowId(int rowId) {
      _rowId = rowId;
    }

    public String getField() {
      return _field;
    }

    public void setField(String field) {
      _field = field;
    }

    public TypeDescription getFieldType() {
      return _fieldType;
    }

    public void setFieldType(TypeDescription fieldType) {
      _fieldType = fieldType;
    }

    public ColumnVector getColumnVector() {
      return _columnVector;
    }

    public void setColumnVector(ColumnVector columnVector) {
      _columnVector = columnVector;
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
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public String getRecordExtractorClassName() {
    return ORCRecordExtractor.class.getName();
  }

  @Override
  public void close()
      throws IOException {
    _orcRecordReader.close();
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
}
