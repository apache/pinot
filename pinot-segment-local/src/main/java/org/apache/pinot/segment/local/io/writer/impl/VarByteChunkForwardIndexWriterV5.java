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


/**
 * Forward index writer that extends {@link VarByteChunkForwardIndexWriterV4} and overrides the data layout for
 * multi-value fixed byte operations to improve space efficiency.
 *
 * <p>Consider the following multi-value document as an example: {@code [int(1), int(2), int(3)]}.
 * The current binary data layout in {@code VarByteChunkForwardIndexWriterV4} is as follows:</p>
 * <pre>
 *     0x00000010 0x00000003 0x00000001 0x00000002 0x00000003
 * </pre>
 *
 * <ol>
 * <li>The first 4 bytes ({@code 0x00000010}) represent the total payload length of the byte array
 *   containing the multi-value document content, which in this case is 16 bytes.</li>
 *
 * <li>The next 4 bytes ({@code 0x00000003}) represent the number of elements in the multi-value document (i.e., 3)
 * .</li>
 *
 * <li>The remaining 12 bytes ({@code 0x00000001 0x00000002 0x00000003}) represent the 3 integer values of the
 *   multi-value document: 1, 2, and 3.</li>
 * </ol>
 *
 * <p>In Pinot, the fixed byte raw forward index can only store one specific fixed-length data type:
 * {@code int}, {@code long}, {@code float}, or {@code double}. Instead of explicitly storing the number of elements
 * for each document for multi-value document, this value can be inferred by:</p>
 * <pre>
 *     number of elements = buffer payload length / size of data type
 * </pre>
 *
 * <p>If the forward index uses the passthrough chunk compression type (i.e., no compression), we can save
 * 4 bytes per document by omitting the explicit element count. This leads to the following space savings:</p>
 *
 * <ul>
 *     <li>For documents with 0 elements, we save 50%.</li>
 *     <li>For documents with 1 element, we save 33%.</li>
 *     <li>For documents with 2 elements, we save 25%.</li>
 *     <li>As the number of elements increases, the percentage of space saved decreases.</li>
 * </ul>
 *
 * <p>For forward indexes that use compression to reduce data size, the savings can be even more significant
 * in certain cases. This is demonstrated in the unit test {@link VarByteChunkV5Test#validateCompressionRatioIncrease},
 * where ZStandard was used as the chunk compressor. In the test, 1 million short multi-value (MV) documents
 * were inserted, following a Gaussian distribution for document lengths. Additionally, the values of each integer
 * in the MV documents were somewhat repetitive. Under these conditions, we observed a 50%+ reduction in on-disk
 * file size compared to the V4 forward index writer version.</p>
 *
 * <p>Note that the {@code VERSION} tag is a {@code static final} class variable set to {@code 5}. Since static
 * variables are shadowed in the child class thus associated with the class that defines them, care must be taken to
 * ensure that the parent class can correctly observe the child class's {@code VERSION} value at runtime. To handle
 * this cleanly and correctly, the {@code getVersion()} method is overridden to return the concrete subclass's
 * {@code VERSION} value, ensuring that the correct version number is returned even when using a reference to the
 * parent class.</p>
 *
 * @see VarByteChunkForwardIndexWriterV4
 * @see VarByteChunkForwardIndexWriterV5#getVersion()
 */
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
