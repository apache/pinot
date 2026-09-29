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
package org.apache.pinot.segment.local.io.writer.impl;

import java.io.File;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.local.utils.ArraySerDeUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


/// Forward index writer that extends [VarByteChunkForwardIndexWriterV4] and overrides the data layout for
/// multi-value fixed byte operations to improve space efficiency.
///
/// Consider the following multi-value document as an example: `[int(1), int(2), int(3)]`.
/// The current binary data layout in `VarByteChunkForwardIndexWriterV4` is as follows:
///
/// ```
/// 0x00000010 0x00000003 0x00000001 0x00000002 0x00000003
/// ```
///
/// 1. The first 4 bytes (`0x00000010`) represent the total payload length of the byte array
///      containing the multi-value document content, which in this case is 16 bytes.
///
/// 2. The next 4 bytes (`0x00000003`) represent the number of elements in the multi-value document (i.e., 3)
///    .
///
/// 3. The remaining 12 bytes (`0x00000001 0x00000002 0x00000003`) represent the 3 integer values of the
///      multi-value document: 1, 2, and 3.
///
/// In Pinot, the fixed byte raw forward index can only store one specific fixed-length data type:
/// `int`, `long`, `float`, or `double`. Instead of explicitly storing the number of elements
/// for each document for multi-value document, this value can be inferred by:
///
/// ```
/// number of elements = buffer payload length / size of data type
/// ```
///
/// If the forward index uses the passthrough chunk compression type (i.e., no compression), we can save
/// 4 bytes per document by omitting the explicit element count. This leads to the following space savings:
///
/// - For documents with 0 elements, we save 50%.
/// - For documents with 1 element, we save 33%.
/// - For documents with 2 elements, we save 25%.
/// - As the number of elements increases, the percentage of space saved decreases.
///
/// For forward indexes that use compression to reduce data size, the savings can be even more significant
/// in certain cases. This is demonstrated in the unit test [VarByteChunkV5Test#validateCompressionRatioIncrease],
/// where ZStandard was used as the chunk compressor. In the test, 1 million short multi-value (MV) documents
/// were inserted, following a Gaussian distribution for document lengths. Additionally, the values of each integer
/// in the MV documents were somewhat repetitive. Under these conditions, we observed a 50%+ reduction in on-disk
/// file size compared to the V4 forward index writer version.
///
/// Note that the `VERSION` tag is a `static final` class variable set to `5`. Since static
/// variables are shadowed in the child class thus associated with the class that defines them, care must be taken to
/// ensure that the parent class can correctly observe the child class's `VERSION` value at runtime. To handle
/// this cleanly and correctly, the `getVersion()` method is overridden to return the concrete subclass's
/// `VERSION` value, ensuring that the correct version number is returned even when using a reference to the
/// parent class.
///
/// @see VarByteChunkForwardIndexWriterV4
/// @see VarByteChunkForwardIndexWriterV5#getVersion()
@NotThreadSafe
public class VarByteChunkForwardIndexWriterV5 extends VarByteChunkForwardIndexWriterV4 {
  public static final int VERSION = 5;

  public VarByteChunkForwardIndexWriterV5(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    super(file, compressionType, chunkSize);
  }

  // Override the parent class getVersion();
  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public void putIntMV(int[] values) {
    putBytes(ArraySerDeUtils.serializeIntArrayWithoutLength(values));
  }

  @Override
  public void putLongMV(long[] values) {
    putBytes(ArraySerDeUtils.serializeLongArrayWithoutLength(values));
  }

  @Override
  public void putFloatMV(float[] values) {
    putBytes(ArraySerDeUtils.serializeFloatArrayWithoutLength(values));
  }

  @Override
  public void putDoubleMV(double[] values) {
    putBytes(ArraySerDeUtils.serializeDoubleArrayWithoutLength(values));
  }
}
