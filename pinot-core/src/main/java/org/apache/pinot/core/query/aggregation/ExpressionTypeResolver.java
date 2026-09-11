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
package org.apache.pinot.core.query.aggregation;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver.TypeInferenceUnavailableException;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;


/// Resolves the logical type and cardinality of SSE input expressions from schema and function metadata. No transform
/// functions are constructed or evaluated. Instances contain read-only type lookup state and can be shared by readers.
public final class ExpressionTypeResolver {
  private static final Map<String, TransformFunctionType> TRANSFORM_TYPES = createTransformTypes();
  private final Function<String, ColumnDataType> _columnTypeResolver;

  public ExpressionTypeResolver(Schema schema) {
    _columnTypeResolver = name -> {
      FieldSpec field = schema.getFieldSpecFor(name);
      return field != null ? ColumnDataType.fromDataType(field.getDataType(), field.isSingleValueField()) : null;
    };
  }

  /// Uses the output of an inner query, whose column names and logical types can differ from the base table.
  public ExpressionTypeResolver(DataSchema schema) {
    Map<String, ColumnDataType> types = new HashMap<>();
    for (int i = 0; i < schema.size(); i++) {
      types.put(schema.getColumnName(i), schema.getColumnDataType(i));
    }
    _columnTypeResolver = types::get;
  }

  public ColumnDataType resolve(ExpressionContext expression) {
    switch (expression.getType()) {
      case IDENTIFIER:
        ColumnDataType columnType = _columnTypeResolver.apply(expression.getIdentifier());
        Preconditions.checkArgument(columnType != null, "Cannot resolve type of unknown column: %s",
            expression.getIdentifier());
        return columnType;
      case LITERAL:
        LiteralContext literal = expression.getLiteral();
        return ColumnDataType.fromDataType(literal.getType(), literal.isSingleValue());
      case FUNCTION:
        return resolveFunction(expression.getFunction());
      default:
        throw new IllegalArgumentException("Cannot resolve expression type: " + expression);
    }
  }

  private ColumnDataType resolveFunction(FunctionContext function) {
    Preconditions.checkArgument(function.getType() == FunctionContext.Type.TRANSFORM,
        "Aggregate cannot be used as an aggregation input: %s", function);
    String name = FunctionRegistry.canonicalize(function.getFunctionName());
    List<ExpressionContext> arguments = function.getArguments();
    List<ColumnDataType> argumentTypes = new ArrayList<>(arguments.size());
    for (ExpressionContext argument : arguments) {
      argumentTypes.add(resolve(argument));
    }

    // Native transforms take precedence over scalar registrations at execution time. In particular, native SSE
    // arithmetic can return DOUBLE while the scalar registry has a matching INT or LONG overload.
    if (TransformFunctionFactory.getAllFunctions().containsKey(name)) {
      TransformFunctionType type = TRANSFORM_TYPES.get(name);
      if (type == null) {
        throw new TypeInferenceUnavailableException(name);
      }
      return resolveNative(type, arguments, argumentTypes);
    }
    FunctionInfo functionInfo =
        FunctionRegistry.lookupFunctionInfo(name, argumentTypes.toArray(new ColumnDataType[0]));
    Preconditions.checkArgument(functionInfo != null, "Unsupported function: %s with argument types: %s", name,
        argumentTypes);
    ColumnDataType resultType = FunctionUtils.getColumnDataType(functionInfo.getMethod().getReturnType());
    // ScalarTransformFunctionWrapper represents unrecognized Java return classes as STRING.
    return resultType != null ? resultType : ColumnDataType.STRING;
  }

  private ColumnDataType resolveNative(TransformFunctionType type, List<ExpressionContext> arguments,
      List<ColumnDataType> argumentTypes) {
    switch (type) {
      case CAST:
        Preconditions.checkArgument(arguments.size() == 2, "CAST requires two arguments");
        ColumnDataType target = literalType(arguments, 1);
        return target.isArray() || !argumentTypes.get(0).isArray()
            ? target
            : ColumnDataType.fromDataTypeMV(target.toDataType());
      case JSON_EXTRACT_SCALAR:
      case JSON_EXTRACT_SCALAR_FAST:
      case JSON_EXTRACT_SCALAR_FIRST_MATCH:
      case JSON_EXTRACT_SCALAR_FORY:
        return literalType(arguments, 2);
      case JSON_EXTRACT_INDEX:
        return arguments.size() > 2 ? literalType(arguments, 2) : ColumnDataType.STRING;
      case CASE:
        return resolveCase(arguments, argumentTypes);
      case COALESCE:
        return resolveCoalesce(argumentTypes);
      case LEAST:
      case GREATEST:
        return resolveTuple(argumentTypes);
      case ADD:
      case SUB:
      case MULT:
      case DIV:
      case ABS:
      case CEIL:
      case EXP:
      case FLOOR:
      case LOG:
      case LOG2:
      case LOG10:
      case SQRT:
      case SIGN:
        requireSingleValue(argumentTypes);
        return argumentTypes.contains(ColumnDataType.BIG_DECIMAL) ? ColumnDataType.BIG_DECIMAL : ColumnDataType.DOUBLE;
      case MOD:
      case POWER:
      case ROUND_DECIMAL:
      case TRUNCATE:
      case SIN:
      case COS:
      case TAN:
      case COT:
      case ASIN:
      case ACOS:
      case ATAN:
      case ATAN2:
      case SINH:
      case COSH:
      case TANH:
      case DEGREES:
      case RADIANS:
        requireSingleValue(argumentTypes);
        return ColumnDataType.DOUBLE;
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case IN:
      case NOT_IN:
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
      case IS_NULL:
      case IS_NOT_NULL:
      case IS_DISTINCT_FROM:
      case IS_NOT_DISTINCT_FROM:
      case AND:
      case OR:
      case NOT:
        return ColumnDataType.BOOLEAN;
      case EXTRACT:
      case YEAR:
      case YEAR_OF_WEEK:
      case QUARTER:
      case MONTH_OF_YEAR:
      case WEEK_OF_YEAR:
      case DAY_OF_YEAR:
      case DAY_OF_MONTH:
      case DAY_OF_WEEK:
      case HOUR:
      case MINUTE:
      case SECOND:
      case MILLISECOND:
        return ColumnDataType.INT;
      case JSON_EXTRACT_KEY:
        return ColumnDataType.STRING_ARRAY;
      case ARRAY_MIN:
      case ARRAY_MAX:
        Preconditions.checkArgument(argumentTypes.size() == 1 && argumentTypes.get(0).isArray(),
            "%s requires one multi-value argument", type.getName());
        return ColumnDataType.fromDataTypeSV(argumentTypes.get(0).toDataType());
      case VALUE_IN:
      case FILTER_MV:
        Preconditions.checkArgument(!argumentTypes.isEmpty(), "%s requires arguments", type.getName());
        return argumentTypes.get(0);
      case MAP_VALUE:
        Preconditions.checkArgument(argumentTypes.size() == 3 && argumentTypes.get(2).isArray(),
            "MAP_VALUE requires a multi-value third argument");
        return ColumnDataType.fromDataTypeSV(argumentTypes.get(2).toDataType());
      default:
        if (type.getReturnTypeInference() == null) {
          throw new TypeInferenceUnavailableException(type.getName());
        }
        return AggregationFunctionTypeResolver.inferReturnType(type.getName(), type.getReturnTypeInference(), arguments,
            argumentTypes);
    }
  }

  private static ColumnDataType literalType(List<ExpressionContext> arguments, int index) {
    Preconditions.checkArgument(arguments.size() > index
            && arguments.get(index).getType() == ExpressionContext.Type.LITERAL
            && arguments.get(index).getLiteral().getType() == DataType.STRING,
        "Type argument at position %s must be a string literal", index);
    String name = arguments.get(index).getLiteral().getStringValue().toUpperCase(Locale.ROOT);
    boolean array = name.endsWith("_ARRAY");
    String element = array ? name.substring(0, name.length() - 6) : name;
    ColumnDataType type = switch (element) {
      case "INTEGER" -> ColumnDataType.INT;
      case "BIGINT" -> ColumnDataType.LONG;
      case "REAL" -> ColumnDataType.FLOAT;
      case "BOOL" -> ColumnDataType.BOOLEAN;
      case "VARCHAR", "CHAR" -> ColumnDataType.STRING;
      case "VARBINARY" -> ColumnDataType.BYTES;
      case "DECIMAL", "BIGDECIMAL" -> ColumnDataType.BIG_DECIMAL;
      default -> ColumnDataType.valueOf(element);
    };
    return array ? ColumnDataType.fromDataTypeMV(type.toDataType()) : type;
  }

  /// Matches CASE's numeric widening and treatment of string literals as coercible THEN/ELSE values.
  private static ColumnDataType resolveCase(List<ExpressionContext> arguments, List<ColumnDataType> argumentTypes) {
    Preconditions.checkArgument(arguments.size() >= 2, "CASE requires at least two arguments");
    ColumnDataType result = ColumnDataType.UNKNOWN;
    List<String> pendingLiterals = new ArrayList<>();
    for (int i = 1; i < arguments.size(); i += 2) {
      result = mergeCaseType(result, arguments.get(i), argumentTypes.get(i), pendingLiterals);
    }
    if (arguments.size() % 2 == 1) {
      int last = arguments.size() - 1;
      result = mergeCaseType(result, arguments.get(last), argumentTypes.get(last), pendingLiterals);
    }
    return result != ColumnDataType.UNKNOWN ? result : ColumnDataType.STRING;
  }

  private static ColumnDataType mergeCaseType(ColumnDataType current, ExpressionContext expression, ColumnDataType next,
      List<String> pendingLiterals) {
    Preconditions.checkArgument(!next.isArray(), "Unsupported multi-value expression in CASE THEN/ELSE");
    if (next == ColumnDataType.UNKNOWN || current == next) {
      return current;
    }
    boolean stringLiteral = next == ColumnDataType.STRING && expression.getType() == ExpressionContext.Type.LITERAL;
    if (stringLiteral) {
      String value = expression.getLiteral().getStringValue();
      if (current == ColumnDataType.UNKNOWN) {
        pendingLiterals.add(value);
      } else {
        current.toDataType().convert(value);
      }
      return current;
    }
    if (current == ColumnDataType.UNKNOWN) {
      for (String value : pendingLiterals) {
        next.toDataType().convert(value);
      }
      pendingLiterals.clear();
      return next;
    }
    Preconditions.checkArgument(current.isNumber() && next.isNumber(), "Cannot upcast CASE from %s to %s", current,
        next);
    return current.ordinal() >= next.ordinal() ? current : next;
  }

  private static ColumnDataType resolveCoalesce(List<ColumnDataType> types) {
    Preconditions.checkArgument(!types.isEmpty(), "COALESCE requires arguments");
    requireSingleValue(types);
    ColumnDataType result = ColumnDataType.UNKNOWN;
    for (ColumnDataType type : types) {
      if (result == ColumnDataType.UNKNOWN) {
        result = type;
      } else if (type != ColumnDataType.UNKNOWN) {
        result = result.isNumber() && type.isNumber() ? (result.ordinal() >= type.ordinal() ? result : type)
            : ColumnDataType.STRING;
      }
    }
    Preconditions.checkArgument(
        result.isNumber() || result == ColumnDataType.STRING || result == ColumnDataType.UNKNOWN,
        "COALESCE only supports numerical and string data types");
    return result;
  }

  private static ColumnDataType resolveTuple(List<ColumnDataType> types) {
    Preconditions.checkArgument(!types.isEmpty(), "LEAST/GREATEST requires arguments");
    requireSingleValue(types);
    ColumnDataType result = types.get(0);
    for (ColumnDataType type : types) {
      Preconditions.checkArgument(type.isNumber() || type == ColumnDataType.TIMESTAMP || type == ColumnDataType.STRING
          || type == ColumnDataType.UNKNOWN, "Unsupported LEAST/GREATEST type: %s", type);
      if (result == type) {
        continue;
      }
      Preconditions.checkArgument(result == ColumnDataType.UNKNOWN || type == ColumnDataType.UNKNOWN
          || result.isNumber() && type.isNumber(), "Incompatible LEAST/GREATEST types: %s and %s", result, type);
      if (result == ColumnDataType.BIG_DECIMAL || type == ColumnDataType.BIG_DECIMAL) {
        result = ColumnDataType.BIG_DECIMAL;
      } else if (result == ColumnDataType.FLOAT || result == ColumnDataType.DOUBLE || type == ColumnDataType.FLOAT
          || type == ColumnDataType.DOUBLE) {
        result = ColumnDataType.DOUBLE;
      } else {
        result = ColumnDataType.LONG;
      }
    }
    return result;
  }

  private static void requireSingleValue(List<ColumnDataType> types) {
    for (ColumnDataType type : types) {
      Preconditions.checkArgument(!type.isArray(), "Expected single-value argument, got: %s", type);
    }
  }

  private static Map<String, TransformFunctionType> createTransformTypes() {
    Map<String, TransformFunctionType> types = new HashMap<>();
    for (TransformFunctionType type : TransformFunctionType.values()) {
      for (String name : type.getNames()) {
        types.put(FunctionRegistry.canonicalize(name), type);
      }
    }
    return Map.copyOf(types);
  }
}
