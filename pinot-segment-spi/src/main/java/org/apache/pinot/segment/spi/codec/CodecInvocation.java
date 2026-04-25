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

import java.util.Collections;
import java.util.List;
import java.util.Objects;


/// AST node representing a single codec invocation in the DSL, e.g. `ZSTD(3)` or
/// `DELTA`.
///
/// Produced by [CodecSpecParser] during Phase 1 (structural) parsing.  The raw
/// [#args()] are still strings at this point; codec-specific semantic parsing happens
/// in Phase 2 via [CodecDefinition#parseOptions()].
public final class CodecInvocation {
  private final String _name;
  private final List<String> _args;

  /// @param name the codec name, normalized to upper-case by the parser
  /// @param args positional arguments; must not be `null` (use empty list for no args)
  public CodecInvocation(String name, List<String> args) {
    _name = Objects.requireNonNull(name, "name");
    _args = Collections.unmodifiableList(Objects.requireNonNull(args, "args"));
  }

  /// Returns the codec name, normalized to upper-case.
  public String name() {
    return _name;
  }

  /// Returns the raw positional arguments; never `null`, may be empty.
  public List<String> args() {
    return _args;
  }

  @Override
  public String toString() {
    if (_args.isEmpty()) {
      return _name;
    }
    return _name + "(" + String.join(",", _args) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CodecInvocation)) {
      return false;
    }
    CodecInvocation that = (CodecInvocation) o;
    return _name.equals(that._name) && _args.equals(that._args);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _args);
  }
}
