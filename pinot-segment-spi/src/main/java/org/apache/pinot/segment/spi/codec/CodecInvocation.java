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
import java.util.Locale;
import java.util.Objects;


/// Immutable and thread-safe AST node for one codec invocation, such as `DELTA` or `ZSTD(3)`.
///
/// Argument values remain strings after structural parsing. Codec implementations are responsible
/// for interpreting and semantically validating them.
public final class CodecInvocation {
  private final String _name;
  private final List<String> _args;

  /// @param name codec name using the DSL's ASCII identifier syntax
  /// @param args positional numeric arguments
  public CodecInvocation(String name, List<String> args) {
    String checkedName = Objects.requireNonNull(name, "name");
    CodecDslSyntax.validateName(checkedName);
    if (CodecDslSyntax.isRemovedWrapperName(checkedName)) {
      throw new IllegalArgumentException("CODEC is reserved by the removed wrapper syntax");
    }
    _name = checkedName.toUpperCase(Locale.ROOT);

    List<String> checkedArgs = Objects.requireNonNull(args, "args");
    CodecDslSyntax.validateArguments(checkedArgs);
    _args = List.copyOf(checkedArgs);
  }

  /// Returns the codec name normalized to upper case.
  public String name() {
    return _name;
  }

  /// Returns the immutable positional arguments.
  public List<String> args() {
    return _args;
  }

  /// Returns this invocation in its canonical DSL form.
  public String toDslString() {
    return _args.isEmpty() ? _name : _name + "(" + String.join(",", _args) + ")";
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
