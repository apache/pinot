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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;


/// Internal capability configured by the creator factory before any values are written.
///
/// Creators have a single-threaded build lifecycle. Enabling and writing concurrently is not supported.
public interface CompressionStatsTrackingForwardIndexCreator extends ForwardIndexCreator {
  /// Enables tracking before the first value is written.
  void enableRawForwardIndexUncompressedValueSizeTracking();

  /// Returns the tracked uncompressed serialized value size, or `-1` when tracking is disabled.
  @Override
  long getRawForwardIndexUncompressedValueSizeInBytes();

  /// Returns the raw forward-index chunk-compression type, or null when tracking is disabled.
  @Override
  @Nullable
  ChunkCompressionType getRawForwardIndexChunkCompressionType();
}
