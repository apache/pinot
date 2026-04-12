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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;


/**
 * Resolves one partition-expression scalar function call into an executable binding.
 *
 * <p>The compiler owns DSL parsing and raw-column validation. Implementations of this interface own scalar-function
 * discovery, overload resolution, determinism validation and invocation.
 */
public interface PartitionScalarFunctionResolver {
  /**
   * Resolve the scalar function call or throw an {@link IllegalArgumentException} when the call is invalid.
   */
  ResolvedFunction resolve(String functionName, List<Argument> arguments);

  /**
   * One validated partition-expression argument.
   */
  final class Argument {
    private final PartitionValueType _type;
    private final boolean _dynamic;
    @Nullable
    private final PartitionValue _constantValue;

    private Argument(PartitionValueType type, boolean dynamic, @Nullable PartitionValue constantValue) {
      _type = Preconditions.checkNotNull(type, "Partition scalar function argument type must be configured");
      _dynamic = dynamic;
      _constantValue = constantValue;
    }

    public static Argument dynamic(PartitionValueType type) {
      return new Argument(type, true, null);
    }

    public static Argument constant(PartitionValue value) {
      Preconditions.checkNotNull(value, "Partition scalar function constant must be configured");
      return new Argument(value.getType(), false, value);
    }

    public PartitionValueType getType() {
      return _type;
    }

    public boolean isDynamic() {
      return _dynamic;
    }

    @Nullable
    public PartitionValue getConstantValue() {
      return _constantValue;
    }
  }

  /**
   * One resolved scalar-function binding.
   */
  interface ResolvedFunction {
    int getCost();

    boolean isDynamic();

    PartitionValueType getOutputType();

    PartitionValue invoke(@Nullable PartitionValue dynamicInput);
  }
}
