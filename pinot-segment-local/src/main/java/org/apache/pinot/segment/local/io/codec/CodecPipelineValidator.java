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
import org.apache.pinot.segment.spi.codec.CodecInvocation;
import org.apache.pinot.segment.spi.codec.CodecPipeline;


/// Validates a [CodecPipeline] against structural rules and per-codec context.
///
/// A pipeline is a chain of the form: **N typed-layout-preserving transforms → at most one packing
/// transform → N compressions**. This is enforced by walking the stages and tracking whether we
/// are still operating on column-typed values ("typed domain"):
///
/// 1. All codec names must be registered in the supplied [CodecRegistry].
/// 1. Each codec's [ChunkCodecHandler#validateContext()] must pass for the column's context.
/// 1. A [CodecKind#TRANSFORM] stage may only appear while still in the typed domain — i.e. it must
///    not follow a packing transform or any compression stage (those no longer emit a typed value
///    array, so a transform could not consume their output).
/// 1. A **typed-layout-preserving** transform (see
///    [CodecDefinition#preservesTypedValueLayout()]) keeps the pipeline in the typed domain, so any number
///    may be chained. A **packing** transform leaves the typed domain, so it
///    must be the last transform — only compression stages may follow.
/// 1. Any number of [CodecKind#COMPRESSION] stages may follow, in any order; each is byte→byte.
///
/// Evaluation order: stages run **left-to-right on encode** and **right-to-left on decode**
/// — i.e. `A,B` encodes as `B.encode(A.encode(x))` and decodes as
/// `A.decode(B.decode(y))`. The runtime [CodecPipelineExecutor] enforces
/// this ordering.
final class CodecPipelineValidator {

  private CodecPipelineValidator() {
  }

  /// Validates the pipeline.
  ///
  /// @param pipeline pipeline AST to validate
  /// @param registry registry used to resolve codec names
  /// @param ctx      column context for type validation
  /// @throws IllegalArgumentException if any validation rule is violated
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void validate(CodecPipeline pipeline, CodecRegistry registry, CodecContext ctx) {
    List<CodecInvocation> stages = pipeline.stages();

    // `typedDomain` is true while the running value is still a contiguous array of column-typed
    // values. A packing transform or any compression stage ends the typed domain; once ended, only
    // further compression stages are legal (a transform could not consume non-typed input).
    boolean typedDomain = true;
    for (CodecInvocation invocation : stages) {
      ChunkCodecHandler codec = registry.getOrThrow(invocation.name());
      CodecOptions options = codec.parseOptions(invocation.args());
      codec.validateContext(options, ctx);

      if (codec.kind() == CodecKind.TRANSFORM) {
        if (!typedDomain) {
          throw new IllegalArgumentException(
              "Transform stage '" + invocation.name() + "' must operate on column values, but it follows a "
                  + "packing transform or a compression stage in: " + pipeline.toDslString()
                  + ". A packing transform must be the last transform, and all transforms "
                  + "must precede any compression stage.");
        }
        // Packing transforms emit a non-typed byte stream, so nothing typed may follow them;
        // typed-layout-preserving transforms keep the typed domain.
        if (!codec.preservesTypedValueLayout()) {
          typedDomain = false;
        }
      } else {
        // COMPRESSION: byte→byte, always permitted; ends the typed domain.
        typedDomain = false;
      }
    }
  }
}
