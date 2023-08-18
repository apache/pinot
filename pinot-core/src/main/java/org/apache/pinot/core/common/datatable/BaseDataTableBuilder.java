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
package org.apache.pinot.core.common.datatable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTableUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Base DataTableBuilder implementation.
 */
public abstract class BaseDataTableBuilder implements DataTableBuilder {
  protected final DataSchema _dataSchema;
  protected final int _version;
  protected final int[] _columnOffsets;
  protected final int _rowSizeInBytes;
  protected final ByteArrayOutputStream _fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  protected final DataOutputStream _fixedSizeDataOutputStream =
      new DataOutputStream(_fixedSizeDataByteArrayOutputStream);
  protected final ByteArrayOutputStream _variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  protected final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  protected int _numRows;
  protected ByteBuffer _currentRowDataByteBuffer;

  public BaseDataTableBuilder(DataSchema dataSchema, int version) {
    _dataSchema = dataSchema;
    _version = version;
    _columnOffsets = new int[dataSchema.size()];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets, _version);
  }

  @Override
  public void startRow() {
    _numRows++;
    _currentRowDataByteBuffer = ByteBuffer.allocate(_rowSizeInBytes);
  }

  @Override
  public void setColumn(int colId, int value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(value);
  }

  @Override
  public void setColumn(int colId, long value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putLong(value);
  }

  @Override
  public void setColumn(int colId, float value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putFloat(value);
  }

  @Override
  public void setColumn(int colId, double value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putDouble(value);
  }

  @Override
  public void setColumn(int colId, BigDecimal value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = BigDecimalUtils.serialize(value);
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

  @Override
  public void setColumn(int colId, @Nullable Object value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    if (value == null) {
      _currentRowDataByteBuffer.putInt(0);
      _variableSizeDataOutputStream.writeInt(CustomObject.NULL_TYPE_VALUE);
    } else {
      ObjectSerDeUtils.ObjectType objectType = ObjectSerDeUtils.ObjectType.getObjectType(value);
      int objectTypeValue = objectType.getValue();
      byte[] bytes = ObjectSerDeUtils.serialize(value, objectTypeValue);
      //TODO: Remove this if clause we integrate vector type into the data table like BigDecimal
      // Currently we are using int to store objectTypeValue but we don't add 4 to byte array length which messes up the vector deserialization
      // Is this a bug? Should we fix it?
      _currentRowDataByteBuffer.putInt(bytes.length);
      if (objectType != ObjectSerDeUtils.ObjectType.Vector) {
        _variableSizeDataOutputStream.writeInt(objectTypeValue);
      }
      _variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  @Override
  public void setColumn(int colId, int[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (int value : values) {
      _variableSizeDataOutputStream.writeInt(value);
    }
  }

  @Override
  public void setColumn(int colId, long[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (long value : values) {
      _variableSizeDataOutputStream.writeLong(value);
    }
  }

  @Override
  public void setColumn(int colId, float[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (float value : values) {
      _variableSizeDataOutputStream.writeFloat(value);
    }
  }

  @Override
  public void setColumn(int colId, double[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (double value : values) {
      _variableSizeDataOutputStream.writeDouble(value);
    }
  }

  @Override
  public void finishRow()
      throws IOException {
    _fixedSizeDataByteArrayOutputStream.write(_currentRowDataByteBuffer.array());
  }
}
