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

/// Classification of a codec within a pipeline.
///
/// A pipeline may contain any number of typed-layout-preserving [#TRANSFORM] stages, followed by at most
/// one packing transform, followed by any number of [#COMPRESSION] stages. A packing transform or
/// compression stage ends the column-typed value domain, so no transform may follow either one.
/// Stages run left-to-right on encode and right-to-left on decode.
enum CodecKind {
  /// Reversible transformation over column-typed values. Typed-layout-preserving transforms may
  /// be chained; a packing transform emits bytes and
  /// must be the last transform. See [CodecDefinition#preservesTypedValueLayout()].
  TRANSFORM,
  /// Byte-level compression (e.g. ZSTD). Any number of compression stages may follow the
  /// transforms; once compression begins, only further compression stages are allowed.
  COMPRESSION
}
