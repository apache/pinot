/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.partition;

import com.google.common.base.Preconditions;


/**
 * Modulo operation based partition function, where:
 * <ul>
 *   <li> partitionId = value % {@link #_divisor}</li>
 * </ul>
 *
 */
public class ModuloPartitionFunction implements PartitionFunction {
  private static final String NAME = "modulo";
  private final int _divisor;

  /**
   * Constructor for the class.
   * <ul>
   *   <li> Requires the divisor to be passed in via args. </li>
   * </ul>
   *
   * @param args Arguments for the function
   */
  public ModuloPartitionFunction(String[] args) {
    Preconditions.checkArgument(args.length == 1);
    _divisor = Integer.parseInt(args[0]);
  }

  @Override
  public int getPartition(Object value) {
    if (value instanceof Integer) {
      return ((Integer) value) % _divisor;
    } else {
      throw new IllegalArgumentException(
          "Illegal argument for partitioning, expected Integer, got: " + value.getClass().getName());
    }
  }

  @Override
  public String toString() {
    return NAME + PartitionFunctionFactory.PARTITION_FUNCTION_DELIMITER + _divisor;
  }
}
