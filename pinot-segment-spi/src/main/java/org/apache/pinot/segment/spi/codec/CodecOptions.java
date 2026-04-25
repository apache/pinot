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

/// Marker interface for codec-specific parsed options.
///
/// Each [CodecDefinition] defines its own concrete options class that carries the
/// validated, typed parameters for that codec (e.g. compression level for ZSTD).
///
/// **Implementation contract:**
///
/// - Implementations MUST be immutable and thread-safe — a single instance is shared across
///       all encode/decode calls for a given codec invocation.
/// - `equals`/`hashCode` MUST reflect every configurable parameter so two
///       [canonical][CodecDefinition#canonicalize()] specs that differ only in option values
///       compare unequal. Canonical-spec equality drives reload-on-config-change detection.
/// - `toString` should return a human-readable parameter listing for log/error messages.
public interface CodecOptions {
}
