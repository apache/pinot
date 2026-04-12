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
package org.apache.pinot.segment.local.io.codec.transform;

import org.apache.pinot.segment.spi.codec.ChunkCodec;
import org.apache.pinot.segment.spi.codec.ChunkTransform;


/**
 * Factory for obtaining {@link ChunkTransform} instances by their {@link ChunkCodec} identifier.
 */
public class ChunkTransformFactory {

  private ChunkTransformFactory() {
  }

  /**
   * Returns the singleton {@link ChunkTransform} for the given transform codec.
   *
   * @param codec the transform codec (must be a {@link ChunkCodec.CodecKind#TRANSFORM})
   * @return the corresponding transform instance
   * @throws IllegalArgumentException if the codec is not a known transform
   */
  public static ChunkTransform getTransform(ChunkCodec codec) {
    switch (codec) {
      case DELTA:
        return DeltaTransform.INSTANCE;
      case DOUBLE_DELTA:
        return DoubleDeltaTransform.INSTANCE;
      case XOR:
        return XorTransform.INSTANCE;
      default:
        throw new IllegalArgumentException("Unknown transform codec: " + codec);
    }
  }
}
