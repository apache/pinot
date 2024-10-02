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
package org.apache.pinot.segment.local.segment.index.creator;

import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkMVForwardIndexReaderV2;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV5;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 Same as MultiValueFixedByteRawIndexCreatorTest, but the forward index creator and reader are newer version
 */
public class MultiValueFixedByteRawIndexCreatorV2Test extends MultiValueFixedByteRawIndexCreatorTest {
  @Override
  protected ForwardIndexReader getForwardIndexReader(PinotDataBuffer buffer, FieldSpec.DataType dataType,
      int writerVersion) {
    return writerVersion == VarByteChunkForwardIndexWriterV4.VERSION ? new VarByteChunkForwardIndexReaderV5(buffer,
        dataType.getStoredType(), false) : new FixedByteChunkMVForwardIndexReaderV2(buffer, dataType.getStoredType());
  }
}
