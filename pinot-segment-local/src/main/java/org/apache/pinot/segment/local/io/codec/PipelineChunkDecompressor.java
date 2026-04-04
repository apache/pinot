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
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;


/**
 * A {@link ChunkDecompressor} that reverses a codec pipeline: first decompresses using the
 * terminal {@link ChunkCodec.CodecKind#COMPRESSOR COMPRESSOR}, then applies all
 * {@link ChunkCodec.CodecKind#TRANSFORM TRANSFORM} stages in reverse order (right-to-left).
 *
 * <p>This is the read-path counterpart of {@link PipelineChunkCompressor}.</p>
 */
public class PipelineChunkDecompressor implements ChunkDecompressor {

  private final ChunkCodecPipeline _pipeline;
  private final ChunkTransform[] _transforms;
  private final ChunkDecompressor _terminalDecompressor;
  private final int _valueSizeInBytes;

  /**
   * Creates a pipeline decompressor.
   *
   * @param pipeline the codec pipeline
   * @param valueSizeInBytes size of each typed value (4 for INT, 8 for LONG); used by transforms
   */
  public PipelineChunkDecompressor(ChunkCodecPipeline pipeline, int valueSizeInBytes) {
    _pipeline = pipeline;
    _valueSizeInBytes = valueSizeInBytes;

    List<ChunkCodec> transformStages = pipeline.getTransforms();
    _transforms = new ChunkTransform[transformStages.size()];
    for (int i = 0; i < transformStages.size(); i++) {
      _transforms[i] = ChunkTransformFactory.getTransform(transformStages.get(i));
    }

    _terminalDecompressor = ChunkCompressorFactory.getDecompressor(
        pipeline.getChunkCompressionType());
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    // Decompress using terminal decompressor.
    // Per Pinot convention, after this call the output buffer is flipped: position=0, limit=dataSize.
    int decompressedSize = _terminalDecompressor.decompress(compressedInput, decompressedOutput);

    if (_transforms.length > 0) {
      // Buffer is already in read mode (flipped). Transforms operate from position=0.
      int numBytes = decompressedOutput.remaining();

      // Apply transforms in reverse order (right-to-left)
      for (int i = _transforms.length - 1; i >= 0; i--) {
        _transforms[i].decode(decompressedOutput, numBytes, _valueSizeInBytes);
      }
      // Buffer remains flipped: position=0, limit=numBytes — ready for the caller to read.
    }

    return decompressedSize;
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput)
      throws IOException {
    return _terminalDecompressor.decompressedLength(compressedInput);
  }

  @Override
  public void close()
      throws IOException {
    _terminalDecompressor.close();
  }
}
