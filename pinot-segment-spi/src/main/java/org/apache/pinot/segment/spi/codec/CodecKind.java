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

/// Classification of a codec within a pipeline.
///
/// A pipeline may contain any number of [#TRANSFORM] stages (chained — each operates on the
/// output of the previous one) followed by at most one [#COMPRESSION] stage which, if present,
/// must be last. Stages run left-to-right on encode and right-to-left on decode.
public enum CodecKind {
  /// Reversible transformation that typically does not change data size significantly
  /// (e.g. DELTA, DELTADELTA, T64 bit-packing). Multiple transforms may be chained; each one
  /// operates on the output of the previous transform.
  TRANSFORM,
  /// Byte-level compression (e.g. ZSTD). At most one compression stage is allowed and, when
  /// present, it must be the last stage of the pipeline.
  COMPRESSION
}
