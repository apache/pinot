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
package org.apache.pinot.segment.local.io.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.segment.local.io.codec.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.local.io.codec.transform.ChunkTransformFactory;
import org.apache.pinot.segment.spi.codec.ChunkCodec;
import org.apache.pinot.segment.spi.codec.ChunkCodecPipeline;
import org.apache.pinot.segment.spi.codec.ChunkTransform;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;


/**
 * A {@link ChunkCompressor} that applies a pipeline of codec stages: first all
 * {@link ChunkCodec.CodecKind#TRANSFORM TRANSFORM} stages in order (left-to-right),
 * then the terminal {@link ChunkCodec.CodecKind#COMPRESSOR COMPRESSOR}.
 *
 * <p>This is the write-path counterpart of {@link PipelineChunkDecompressor}.</p>
 */
public class PipelineChunkCompressor implements ChunkCompressor {

  private final ChunkCodecPipeline _pipeline;
  private final ChunkTransform[] _transforms;
  private final ChunkCompressor _terminalCompressor;
  private final int _valueSizeInBytes;

  /**
   * Creates a pipeline compressor.
   *
   * @param pipeline the codec pipeline
   * @param valueSizeInBytes size of each typed value (4 for INT, 8 for LONG); used by transforms
   */
  public PipelineChunkCompressor(ChunkCodecPipeline pipeline, int valueSizeInBytes) {
    _pipeline = pipeline;
    _valueSizeInBytes = valueSizeInBytes;

    List<ChunkCodec> transformStages = pipeline.getTransforms();
    _transforms = new ChunkTransform[transformStages.size()];
    for (int i = 0; i < transformStages.size(); i++) {
      _transforms[i] = ChunkTransformFactory.getTransform(transformStages.get(i));
    }

    _terminalCompressor = ChunkCompressorFactory.getCompressor(
        pipeline.getChunkCompressionType());
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    // Apply transforms left-to-right (in-place on the input buffer)
    int numBytes = inUncompressed.remaining();
    for (ChunkTransform transform : _transforms) {
      transform.encode(inUncompressed, numBytes, _valueSizeInBytes);
    }

    // Apply terminal compression
    return _terminalCompressor.compress(inUncompressed, outCompressed);
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    // Transforms are in-place and don't change size; delegate to terminal compressor
    return _terminalCompressor.maxCompressedSize(uncompressedSize);
  }

  @Override
  public ChunkCompressionType compressionType() {
    return _terminalCompressor.compressionType();
  }

  @Override
  public void close()
      throws IOException {
    _terminalCompressor.close();
  }
}
