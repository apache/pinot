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

package org.apache.pinot.core.query.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.ExpressionContextVisitor;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.Schema;


public class ExpressionTypeResolver implements ExpressionContextVisitor<PinotDataType> {

  private final Schema _schema;
  private final QueryContext _queryContext;

  public ExpressionTypeResolver(Schema schema, QueryContext queryContext) {
    _schema = schema;
    _queryContext = queryContext;
  }

  @Override
  public PinotDataType visitLiteral(LiteralContext literal) {
    return PinotDataType.getPinotDataTypeForIngestion(new DimensionFieldSpec("ignored", literal.getType(), true));
  }

  @Override
  public PinotDataType visitIdentifier(String identifier) {
    if (identifier.equals("*")) {
      // handle this specially, as star should not be the input to any function
      // other than COUNT, which accepts all inputs
      return PinotDataType.OBJECT;
    }
    return PinotDataType.getPinotDataTypeForIngestion(_schema.getFieldSpecFor(identifier));
  }

  @Override
  public PinotDataType visitFunction(FunctionContext function) {
    List<DataSchema.ColumnDataType> argumentTypes =
        function.getArguments()
            .stream()
            .map(ec -> ec.visit(this))
            .map(PinotDataType::toColumnDataType)
            .collect(Collectors.toList());

    String name = function.getFunctionName();
    switch (function.getType()) {
      case TRANSFORM:
        FunctionInfo info = FunctionRegistry.getFunctionInfo(name, argumentTypes.size());
        return info != null ? validateScalar(name, info, argumentTypes) : validateTransform(function);
      case AGGREGATION:
        return validateAggregate(function, argumentTypes);
      default:
        throw new UnsupportedOperationException("Illegal function type: " + function.getType());
    }
  }

  private PinotDataType validateScalar(String name, FunctionInfo info, List<DataSchema.ColumnDataType> argumentTypes) {
    Class<?>[] parameterTypes = info.getMethod().getParameterTypes();
    for (int i = 0; i < parameterTypes.length; i++) {
      DataSchema.ColumnDataType paramType = argumentTypes.get(i);
      if (!paramType.canAssignTo(parameterTypes[i])) {
        throw new IllegalArgumentException("Scalar Function " + name + " does not support input types: "
            + argumentTypes);
      }
    }
    return typeFromClass(info.getMethod().getReturnType());
  }

  private static PinotDataType typeFromClass(Class<?> clazz) {
    clazz = Primitives.wrap(clazz);
    return clazz.isArray()
        ? PinotDataType.getMultiValueType(Primitives.wrap(clazz.getComponentType()))
        : PinotDataType.getSingleValueType(clazz);
  }

  private PinotDataType validateTransform(FunctionContext function) {
    // try Transform Function - note that we pass an ImmutableMap.of for the data source map
    // so that we can call this function without access to the data; at the moment, the data
    // source map is only used in IsNullTransformFunction and IsNotNullTransformFunction to
    // get the null bitmap - this is not necessary for calling getResultMetadata
    // TODO: split TransformFunction#init into two functions so that it is clear that one
    // TODO: only initializes the getResultMetadata part and does not rely on DataSource
    TransformFunction transformFunction =
        TransformFunctionFactory.get(_queryContext, ExpressionContext.forFunction(function), ImmutableMap.of());
    if (transformFunction != null) {
      TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
      return PinotDataType.getPinotDataTypeForIngestion(
          new DimensionFieldSpec("ignored", resultMetadata.getDataType(), resultMetadata.isSingleValue()));
    }
    throw new IllegalArgumentException("Could not find function with name " + function.getFunctionName());
  }

  private PinotDataType validateAggregate(FunctionContext function, List<DataSchema.ColumnDataType> argumentTypes) {
    AggregationFunction<?, ?> agg = AggregationFunctionFactory.getAggregationFunction(function, _queryContext);
    if (!agg.validateInputTypes(argumentTypes)) {
      throw new IllegalArgumentException("Aggregate Function " + function.getFunctionName()
          + " does not support input types: " + argumentTypes);
    } else if (agg instanceof DistinctAggregationFunction) {
      // DISTINCT is a top-level aggregation and should never feed into anything else,
      // just like with "*" we just return OBJECT
      return PinotDataType.OBJECT;
    }
    return PinotDataType.getPinotDataTypeForExecution(agg.getFinalResultColumnType());
  }
}
