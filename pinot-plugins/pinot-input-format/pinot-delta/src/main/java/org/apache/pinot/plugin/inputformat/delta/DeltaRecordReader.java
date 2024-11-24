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
package org.apache.pinot.plugin.inputformat.delta;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.IntegerType;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import io.delta.standalone.data.RowRecord;

public class DeltaRecordReader implements RecordReader {
  private File _dataFile;

  private CloseableIterator<RowRecord> _iterator;

  private Set<String> _fieldsToRead;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;

    // TODO/sudhindra: read configuration from RecordReaderConfig, instead of using a default config here
    DeltaLog log = DeltaLog.forTable(new Configuration(), dataFile.getPath());

    _iterator = log.update().open();
    _fieldsToRead = (fieldsToRead == null) ? new HashSet<>() : fieldsToRead;
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  private void readAllFields(RowRecord record) {
    StructField[] fields = record.getSchema().getFields();

    for (StructField field: fields) {
      _fieldsToRead.add(field.getName());
    }
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    RowRecord record = _iterator.next();

    if (_fieldsToRead.isEmpty()) {
      readAllFields(record);
    }

    for (String fieldName: _fieldsToRead) {
      if (fieldName.contains("mv") || fieldName.contains("column_not_in_source")) {
        continue;
      }

      final DataType dataType = record.getSchema().get(fieldName).getDataType();

      // handles only primitive types for now; hopefully the pinot API handles any null values
      if (dataType instanceof StringType) {
        reuse.putValue(fieldName, record.getString(fieldName));
      } else if (dataType instanceof BooleanType) {
        reuse.putValue(fieldName, record.getBoolean(fieldName));
      } else if (dataType instanceof IntegerType) {
        reuse.putValue(fieldName, record.getInt(fieldName));
      } else if (dataType instanceof DoubleType) {
        reuse.putValue(fieldName, record.getDouble(fieldName));
      } else if (dataType instanceof FloatType) {
        reuse.putValue(fieldName, record.getFloat(fieldName));
      } else if (dataType instanceof ByteType) {
        reuse.putValue(fieldName, record.getByte(fieldName));
      } else if (dataType instanceof LongType) {
        reuse.putValue(fieldName, record.getLong(fieldName));
      } else {
        throw new IOException("unknown data type " + dataType.getSimpleString());
      }
    }

    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    close();
    DeltaLog log = DeltaLog.forTable(new Configuration(), _dataFile.getPath());
    _iterator = log.update().open();
  }

  @Override
  public void close()
      throws IOException {
    _fieldsToRead.clear();
    _iterator.close();
  }
}
