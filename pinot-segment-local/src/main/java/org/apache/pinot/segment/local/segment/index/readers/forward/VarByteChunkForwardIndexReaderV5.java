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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Chunk-based raw (non-dictionary-encoded) forward index reader for values of SV variable length data types
 * (BIG_DECIMAL, STRING, BYTES), MV fixed length and MV variable length data types.
 * <p>For data layout, please refer to the documentation for {@link VarByteChunkForwardIndexWriterV4}
 */
public class VarByteChunkForwardIndexReaderV5 extends VarByteChunkForwardIndexReaderV4 {
  public VarByteChunkForwardIndexReaderV5(PinotDataBuffer dataBuffer, FieldSpec.DataType storedType,
      boolean isSingleValue) {
    super(dataBuffer, storedType, isSingleValue);
  }

  @Override
  public void validateIndexVersion(PinotDataBuffer dataBuffer) {
    int version = dataBuffer.getInt(0);
    Preconditions.checkState(version == VarByteChunkForwardIndexWriterV5.VERSION, "Illegal index version: %s", version);
  }

  @Override
  protected int getNumFixedByteValuesMV(ByteBuffer byteBuffer) {
    return byteBuffer.remaining() / _storedType.size();
  }
}
