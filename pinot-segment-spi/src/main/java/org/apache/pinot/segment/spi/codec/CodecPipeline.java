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
import java.util.stream.Collectors;


/// Immutable and thread-safe AST node for an ordered list of codec invocations, such as `DELTA,ZSTD(3)`.
public final class CodecPipeline {
  private final List<CodecInvocation> _stages;

  /// @param stages ordered codec invocations; the list is copied defensively
  public CodecPipeline(List<CodecInvocation> stages) {
    List<CodecInvocation> checkedStages = Objects.requireNonNull(stages, "stages");
    if (checkedStages.isEmpty()) {
      throw new IllegalArgumentException("A codec pipeline must have at least one stage");
    }
    if (checkedStages.size() > CodecDslSyntax.MAX_PIPELINE_STAGES) {
      throw new IllegalArgumentException(
          "Codec pipeline has " + checkedStages.size() + " stages; maximum is "
              + CodecDslSyntax.MAX_PIPELINE_STAGES);
    }
    _stages = List.copyOf(checkedStages);
  }

  /// Returns the immutable ordered codec invocations.
  public List<CodecInvocation> stages() {
    return _stages;
  }

  /// Returns the structurally normalized DSL string. Semantic normalization is codec-specific.
  public String toDslString() {
    return _stages.stream().map(CodecInvocation::toDslString).collect(Collectors.joining(","));
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
