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

import java.io.Closeable;


/// Common write API for single-value fixed-width raw forward-index chunk writers, letting callers
/// hold a single writer reference regardless of the on-disk format that backs it.
///
/// Two implementations exist, selected by the configured codec spec:
/// - [FixedByteChunkForwardIndexWriter] — the legacy chunk format (versions ≤ 4), readable by all
///   server versions; used for plain `ChunkCompressionType` compression. Supports INT/LONG/FLOAT/DOUBLE.
/// - [FixedByteChunkForwardIndexWriterV7] — the V7 codec-pipeline format; used for transform specs
///   and compression-only specs that a legacy `ChunkCompressionType` cannot represent. Supports
///   INT/LONG only; `putFloat`/`putDouble` throw [UnsupportedOperationException].
public interface FixedByteValueWriter extends Closeable {

  void putInt(int value);

  void putLong(long value);

  void putFloat(float value);

  void putDouble(double value);

  /// Returns the total uncompressed size of data written so far, or `-1` when tracking is disabled.
  long getRawForwardIndexUncompressedValueSizeInBytes();

  /// Enables uncompressed-size tracking before the first value is written.
  void enableRawForwardIndexUncompressedValueSizeTracking();
}
