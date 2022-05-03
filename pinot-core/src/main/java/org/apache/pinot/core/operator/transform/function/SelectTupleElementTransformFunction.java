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
package org.apache.pinot.core.operator.transform.function;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;


public abstract class SelectTupleElementTransformFunction extends BaseTransformFunction {

  private static final EnumSet<FieldSpec.DataType> SUPPORTED_DATATYPES = EnumSet.of(FieldSpec.DataType.INT,
      FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.BIG_DECIMAL,
      FieldSpec.DataType.TIMESTAMP, FieldSpec.DataType.STRING);

  private static final EnumMap<FieldSpec.DataType, EnumSet<FieldSpec.DataType>> ACCEPTABLE_COMBINATIONS =
      createAcceptableCombinations();

  private final String _name;

  protected List<TransformFunction> _arguments;
  private TransformResultMetadata _resultMetadata;

  public SelectTupleElementTransformFunction(String name) {
    _name = name;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    if (arguments.isEmpty()) {
      throw new IllegalArgumentException(_name + " takes at least one argument");
    }
    FieldSpec.DataType dataType = null;
    for (int i = 0; i < arguments.size(); i++) {
      TransformFunction argument = arguments.get(i);
      TransformResultMetadata metadata = argument.getResultMetadata();
      if (!metadata.isSingleValue()) {
        throw new IllegalArgumentException(argument.getName() + " at position " + i + " is not single value");
      }
      FieldSpec.DataType argumentType = metadata.getDataType();
      if (!SUPPORTED_DATATYPES.contains(argumentType)) {
        throw new IllegalArgumentException(argumentType + " not supported. Required one of " + SUPPORTED_DATATYPES);
      }
      if (dataType == null) {
        dataType = argumentType;
      } else if (ACCEPTABLE_COMBINATIONS.get(dataType).contains(argumentType)) {
        dataType = getLowestCommonDenominatorType(dataType, argumentType);
      } else {
        throw new IllegalArgumentException(
            "combination " + argumentType + " not supported. Required one of " + ACCEPTABLE_COMBINATIONS.get(dataType));
      }
    }
    _resultMetadata = new TransformResultMetadata(dataType, true, false);
    _arguments = arguments;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public String getName() {
    return _name;
  }

  private static FieldSpec.DataType getLowestCommonDenominatorType(FieldSpec.DataType left, FieldSpec.DataType right) {
    if (left == null || left == right) {
      return right;
    }
    if (left == FieldSpec.DataType.BIG_DECIMAL || right == FieldSpec.DataType.BIG_DECIMAL) {
      return FieldSpec.DataType.BIG_DECIMAL;
    }
    if (left == FieldSpec.DataType.DOUBLE || left == FieldSpec.DataType.FLOAT || right == FieldSpec.DataType.DOUBLE
        || right == FieldSpec.DataType.FLOAT) {
      return FieldSpec.DataType.DOUBLE;
    }
    return FieldSpec.DataType.LONG;
  }

  private static EnumMap<FieldSpec.DataType, EnumSet<FieldSpec.DataType>> createAcceptableCombinations() {
    EnumMap<FieldSpec.DataType, EnumSet<FieldSpec.DataType>> combinations = new EnumMap<>(FieldSpec.DataType.class);
    EnumSet<FieldSpec.DataType> numericTypes = EnumSet.of(FieldSpec.DataType.INT, FieldSpec.DataType.LONG,
        FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.BIG_DECIMAL);
    for (FieldSpec.DataType numericType : numericTypes) {
      combinations.put(numericType, numericTypes);
    }
    combinations.put(FieldSpec.DataType.TIMESTAMP, EnumSet.of(FieldSpec.DataType.TIMESTAMP));
    combinations.put(FieldSpec.DataType.STRING, EnumSet.of(FieldSpec.DataType.STRING));
    return combinations;
  }
}
