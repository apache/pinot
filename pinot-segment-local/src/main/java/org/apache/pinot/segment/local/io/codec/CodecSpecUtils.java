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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecInvocation;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecPipeline;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


/// Utilities for classifying codec DSL specs into legacy-compatible chunk compression
/// versus full codec-pipeline transforms.
public final class CodecSpecUtils {

  private CodecSpecUtils() {
  }

  /// Returns `true` when the parsed pipeline contains at least one transform stage.
  public static boolean hasTransform(CodecPipeline pipeline, CodecRegistry registry) {
    for (CodecInvocation invocation : pipeline.stages()) {
      ChunkCodecHandler<?> codec = registry.getOrThrow(invocation.name());
      if (codec.kind() == CodecKind.TRANSFORM) {
        return true;
      }
    }
    return false;
  }

  /// Returns the legacy [ChunkCompressionType] for compression-only specs that can be
  /// represented by the existing raw forward-index formats.
  ///
  /// Specs that require the codec-pipeline format, such as transforms or non-default
  /// compression options, return `null`.
  @Nullable
  public static ChunkCompressionType toLegacyChunkCompressionType(String codecSpec) {
    return toLegacyChunkCompressionType(CodecSpecParser.parse(codecSpec));
  }

  /// Returns the legacy [ChunkCompressionType] for compression-only specs that can be
  /// represented by the existing raw forward-index formats.
  @Nullable
  public static ChunkCompressionType toLegacyChunkCompressionType(CodecPipeline pipeline) {
    List<CodecInvocation> stages = pipeline.stages();
    if (stages.size() != 1) {
      return null;
    }
    CodecInvocation invocation = stages.get(0);
    List<String> args = invocation.args();
    switch (invocation.name()) {
      case Lz4CodecDefinition.NAME:
        return args.isEmpty() ? ChunkCompressionType.LZ4 : null;
      case SnappyCodecDefinition.NAME:
        return args.isEmpty() ? ChunkCompressionType.SNAPPY : null;
      case GzipCodecDefinition.NAME:
        return args.isEmpty() ? ChunkCompressionType.GZIP : null;
      case ZstdCodecDefinition.NAME:
      case "ZSTANDARD": // alias for ZSTD (see CodecRegistry)
        // The legacy ZSTANDARD compressor uses the library default level. In the codec DSL that
        // corresponds to ZSTD / ZSTD(3). Other explicit levels need a format that stores the spec.
        if (args.isEmpty()) {
          return ChunkCompressionType.ZSTANDARD;
        }
        if (args.size() == 1) {
          try {
            return Integer.parseInt(args.get(0)) == ZstdCodecDefinition.DEFAULT_LEVEL
                ? ChunkCompressionType.ZSTANDARD : null;
          } catch (NumberFormatException e) {
            return null;
          }
        }
        return null;
      default:
        return null;
    }
  }
}
