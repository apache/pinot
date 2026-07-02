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
package org.apache.pinot.segment.spi.codec;

import java.util.List;


/// Describes a single codec (transform or compression) that can participate in a pipeline.
///
/// Implementations are registered in a `CodecRegistry` and looked up by
/// [#name()] during DSL parsing.  Each implementation is responsible for:
///
/// 1. Parsing its own raw string arguments into typed [CodecOptions].
/// 1. Validating that the options are consistent with the target column's context.
/// 1. Producing a stable canonical string so pipelines round-trip through metadata.
///
/// Implementations must be stateless and thread-safe; a single instance is reused for
/// all columns that reference the codec.
///
/// @param <O> the concrete [CodecOptions] type for this codec
public interface CodecDefinition<O extends CodecOptions> {

  /// The uppercase name used in the DSL, e.g. `"DELTA"` or `"ZSTD"`.
  /// Must be unique across all registered codecs.
  String name();

  /// Whether this codec is a transform or a final compression stage.
  CodecKind kind();

  /// Parses the raw positional arguments from the DSL invocation into typed options.
  ///
  /// @param args positional string arguments; empty list when the codec was invoked without parens
  ///             or with an empty argument list
  /// @return parsed options (never `null`)
  /// @throws IllegalArgumentException if the argument list is invalid for this codec
  O parseOptions(List<String> args);

  /// Validates that the codec can be applied to the given column context.
  ///
  /// @param options     options previously returned by [#parseOptions()]
  /// @param ctx         context describing the target column
  /// @throws IllegalArgumentException if the codec is incompatible with this context
  void validateContext(O options, CodecContext ctx);

  /// Returns the canonical DSL string for the given options, e.g. `"ZSTD(3)"` or
  /// `"DELTA"`.  The canonical form is stored in segment metadata and used when
  /// re-opening the segment.
  String canonicalize(O options);

  /// Whether this codec's `encode` output is a contiguous array of column-typed values of the same
  /// width as its input (so that a subsequent [CodecKind#TRANSFORM] stage can consume it directly).
  ///
  /// This governs how stages may be chained in a pipeline:
  /// - **Value-preserving transforms** (e.g. `DELTA`, `DELTADELTA`) return `true`: they map a typed
  ///   value array to a same-width typed value array, so any number of them may be chained.
  /// - **Packing transforms** (e.g. `T64`, `GORILLA`) return `false`: they emit a bit-packed /
  ///   self-framed byte stream that is no longer a typed value array, so they must be the **last**
  ///   transform in the chain (only [CodecKind#COMPRESSION] stages may follow).
  /// - **Compression** codecs are byte→byte and the value returned here is irrelevant (a compression
  ///   stage always ends the "typed value" domain); the default `false` is appropriate.
  default boolean isValuePreserving() {
    return false;
  }
}
