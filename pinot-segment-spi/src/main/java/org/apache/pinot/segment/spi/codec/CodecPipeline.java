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
import java.util.Objects;


/// AST node representing an ordered sequence of codec stages, e.g.
/// `CODEC(DELTA,ZSTD(3))`.
///
/// A pipeline with a single stage is represented as just that stage's invocation; the
/// outer `CODEC(…)` wrapper is only required when there are multiple stages. Both
/// forms are accepted by [CodecSpecParser].
///
/// Instances are immutable and thread-safe.
public final class CodecPipeline {
  private final List<CodecInvocation> _stages;

  /// @param stages ordered list of codec stages; copied defensively and must not be `null` or empty
  /// @throws NullPointerException if `stages` or a stage is `null`
  /// @throws IllegalArgumentException if the stage list is empty or exceeds the structural DSL limit
  public CodecPipeline(List<CodecInvocation> stages) {
    List<CodecInvocation> checkedStages = Objects.requireNonNull(stages, "stages");
    if (checkedStages.isEmpty()) {
      throw new IllegalArgumentException("A CodecPipeline must have at least one stage");
    }
    if (checkedStages.size() > CodecSpecParser.MAX_PIPELINE_STAGES) {
      throw new IllegalArgumentException(
          "Codec pipeline has " + checkedStages.size() + " stages; maximum is "
              + CodecSpecParser.MAX_PIPELINE_STAGES);
    }
    _stages = List.copyOf(checkedStages);
  }

  /// Returns the immutable ordered list of codec invocations; never empty.
  public List<CodecInvocation> stages() {
    return _stages;
  }

  /// Returns the DSL string as parsed (raw invocation tokens, not handler-normalized).
  /// This is *not* the canonical form — the canonical form is produced by the runtime
  /// pipeline executor by running each codec's `canonicalize(options)` and is the form
  /// stored verbatim in segment file headers.
  public String toDslString() {
    if (_stages.size() == 1) {
      return _stages.get(0).toString();
    }
    StringBuilder sb = new StringBuilder("CODEC(");
    for (int i = 0; i < _stages.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(_stages.get(i).toString());
    }
    sb.append(')');
    return sb.toString();
  }

  @Override
  public String toString() {
    return toDslString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CodecPipeline)) {
      return false;
    }
    CodecPipeline that = (CodecPipeline) o;
    return _stages.equals(that._stages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_stages);
  }
}
