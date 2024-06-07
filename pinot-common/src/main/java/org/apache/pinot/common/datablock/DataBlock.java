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
package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public interface DataBlock {
  Map<String, String> getMetadata();

  DataSchema getDataSchema();

  int getNumberOfRows();

  int getNumberOfColumns();

  void addException(ProcessingException processingException);

  void addException(int errCode, String errMsg);

  Map<Integer, String> getExceptions();

  /**
   * This is basically a wrap on top of {@link DataBlockUtils#serialize(DataBlock)} but implementations can catch
   * the result so messages sent to more than one receiving mailbox don't need to be serialized as many times.
   */
  List<ByteBuffer> serialize()
      throws IOException;

  // --------------------------------------------------------------------------
  // The following APIs are copied from {@link DataTable} and will be deprecated soon.
  // --------------------------------------------------------------------------

  int getVersion();

  int getInt(int rowId, int colId);

  long getLong(int rowId, int colId);

  float getFloat(int rowId, int colId);

  double getDouble(int rowId, int colId);

  BigDecimal getBigDecimal(int rowId, int colId);

  String getString(int rowId, int colId);

  ByteArray getBytes(int rowId, int colId);

  int[] getIntArray(int rowId, int colId);

  long[] getLongArray(int rowId, int colId);

  float[] getFloatArray(int rowId, int colId);

  double[] getDoubleArray(int rowId, int colId);

  String[] getStringArray(int rowId, int colId);

  CustomObject getCustomObject(int rowId, int colId);

  @Nullable
  RoaringBitmap getNullRowIds(int colId);

  Type getDataBlockType();

  Raw asRaw();

  enum Type {
    ROW(0),
    COLUMNAR(1),
    METADATA(2);

    private final int _ordinal;

    Type(int ordinal) {
      _ordinal = ordinal;
    }

    public static Type fromOrdinal(int ordinal) {
      switch (ordinal) {
        case 0:
          return ROW;
        case 1:
          return COLUMNAR;
        case 2:
          return METADATA;
        default:
          throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
      }
    }
  }

  /**
   * A raw representation of the block.
   * <p>
   * Do not confuse this with the serialized form of the block. This is a representation of the block in memory and
   * it is completely dependent on the current Pinot version. That means that this representation can change between
   * Pinot versions.
   * <p>
   * The {@link DataBlockSerde} is responsible for serializing and deserializing this raw representation into a binary
   * format that is compatible with the other Pinot versions.
   */
  interface Raw {
    int getNumberOfRows();

    int getNumberOfColumns();

    @Nullable
    Map<Integer, String> getExceptions();

    @Nullable
    String[] getStringDictionary();

    @Nullable
    DataSchema getDataSchema();

    /**
     * The actual content is different depending on whether this is a row-based or columnar data block.
     */
    @Nullable
    DataBuffer getFixedData();

    /**
     * The actual content is different depending on whether this is a row-based or columnar data block.
     */
    @Nullable
    DataBuffer getVarSizeData();

    @Nullable
    List<DataBuffer> getStatsByStage();
  }
}
