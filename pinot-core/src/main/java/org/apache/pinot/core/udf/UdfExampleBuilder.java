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
package org.apache.pinot.core.udf;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.util.Preconditions;
import org.apache.pinot.spi.data.FieldSpec;

/// A builder for generating test cases for [UDFs][Udf] (User Defined Functions).
///
/// It allows to create test cases for a specific UDF signature, or for endomorphism functions that operate on
/// numeric types.
public interface UdfExampleBuilder {
  Map<UdfSignature, Set<UdfExample>> generateExamples();

  /// Starts a builder for the given signature.
  static SingleBuilder forSignature(UdfSignature signature) {
    return new SingleBuilder(signature);
  }

  /// Starts an endomorphism numeric builder for the given arity.
  ///
  /// This builder is used for functions that take a bunch of numeric values (ie ints) and return the same numeric type.
  /// Most mathematical functions are endomorphism functions, like `abs`, `plus`, `mult` etc.
  ///
  /// These samples can always be generated using [#forSignature], but that would require to write the same test cases
  /// for each numeric type (int, double, big_decimal, etc), which is not very convenient.
  /// Given each numeric type has its own properties (specially the precision), it is still recommended to write some
  /// specific samples for each numeric type, but this builder can be used to generate the most common cases.
  static EndomorphismNumericTypesBuilder forEndomorphismNumeric(int arity) {
    return new EndomorphismNumericTypesBuilder(arity);
  }

  class SingleBuilder {
    private final UdfSignature _signature;
    private final Set<UdfExample> _examples = new HashSet<>();

    protected SingleBuilder(UdfSignature signature) {
      _signature = signature;
    }

    /// Adds a new example for the current signature.
    ///
    /// It is recommended to use the `addCase(String, Object...)` method instead, as it is more convenient.
    public SingleBuilder addExample(UdfExample example) {
      Preconditions.checkArgument(example.getInputValues().size() == _signature.getArity(),
          "Expected %s input values for signature %s, but got %s",
          _signature.getArity(), _signature, example.getInputValues().size());
      if (_examples.stream().anyMatch(e -> e.getId().equals(example.getId()))) {
        throw new IllegalArgumentException(
            "Example with id '" + example.getId() + "' already exists for signature " + _signature);
      }
      _examples.add(example);
      return this;
    }

    /// Adds a new example for the current signature with the given name and values.
    ///
    /// This is equivalent to call `addCase(UdfExample.create(name, values))`.
    /// The created example will use the latest value as the expected result of the function.
    /// Remember that UdfExamples support up to 2 return values, one for when null handling is enabled and one
    /// for when null handling is disabled.
    /// This method assumes that both are going to be the same. In case they are not, create the example using the
    /// [UdfExample#create(String, Object...)] method, decorate it with [UdfExample#withoutNull(Object)] method
    /// and finally call [SingleBuilder#addExample(UdfExample)].
    ///
    /// @param name the name of the example. It should be unique for the given signature.
    /// @param values the values for the example. The last value is the expected result of the function while all the
    ///               other values are the input values. This means that the length of the values array must be
    ///               the arity of the signature plus 1.
    public SingleBuilder addExample(String name, Object... values) {
      UdfExample example = UdfExample.create(name, values);
      return addExample(example);
    }

    /// Adds a new example, which may have a different signature than the current one.
    /// This is used to combine multiple builders in order to create a compound map of examples.
    public MultiBuilder and(UdfExampleBuilder other) {
      MultiBuilder multiBuilder = new MultiBuilder();
      multiBuilder.and(build());
      multiBuilder.and(other);
      return multiBuilder;
    }

    /// Creates the UdfExampleBuilder that generates the examples for the current signature.
    public UdfExampleBuilder build() {
      return () -> Map.of(_signature, _examples);
    }
  }

  /// A builder for generating cases for endomorphism functions that operate on numeric types.
  ///
  /// It means that it takes a bunch of Number as arguments and returns the same Number. For example, if it takes
  /// two ints, it returns an int. If it takes two longs, it returns a long, etc.
  class EndomorphismNumericTypesBuilder {
    private final int _arity;
    private final Set<UdfExample> _cases = new HashSet<>();

    protected EndomorphismNumericTypesBuilder(int arity) {
      _arity = arity;
    }

    /// Like [SingleBuilder#addExample], but for endomorphism functions.
    public EndomorphismNumericTypesBuilder addExample(String name, Number... values) {
      Preconditions.checkArgument(values.length == _arity + 1,
          "Expected %s values, but got %s", _arity + 1, values.length);
      UdfExample example = UdfExample.create(name, values);
      return addExample(example);
    }

    /// Like [SingleBuilder#addExampleWithoutNull], but for endomorphism functions.
    public EndomorphismNumericTypesBuilder addExampleWithoutNull(String name, Number... values) {
      Preconditions.checkArgument(values.length == _arity + 2,
          "Expected %s values, but got %s", _arity + 2, values.length);
      // Copy the first _arity values and add the value returned when null handling is disabled.
      Number[] nullableValues = Arrays.copyOf(values, _arity + 1);
      UdfExample example = UdfExample.create(name, (Object[]) nullableValues)
          // Ad the expected result when null handling is disabled. This value is at the end of the values array.
          // and values.length - 1 == _arity + 1.
          .withoutNull(values[_arity + 1]);

      return addExample(example);
    }

    /// Like [SingleBuilder#addExample], but for endomorphism functions.
    public EndomorphismNumericTypesBuilder addExample(UdfExample example) {
      Preconditions.checkArgument(example.getInputValues().size() == _arity,
          "Expected %s input values for endomorphism function with arity %s, but got %s",
          _arity, _arity, example.getInputValues().size());
      _cases.add(example);
      return this;
    }

    public MultiBuilder and(UdfExampleBuilder other) {
      MultiBuilder multiBuilder = new MultiBuilder();
      multiBuilder.and(build());
      multiBuilder.and(other);
      return multiBuilder;
    }

    /// Builds the UdfExampleBuilder that generates the examples for endomorphism functions.
    public UdfExampleBuilder build() {
      return () -> {
        Set<FieldSpec.DataType> numericTypes = Set.of(
            FieldSpec.DataType.INT,
            FieldSpec.DataType.LONG,
            FieldSpec.DataType.FLOAT,
            FieldSpec.DataType.DOUBLE,
            FieldSpec.DataType.BIG_DECIMAL
        );
        Map<UdfSignature, Set<UdfExample>> cases = Maps.newHashMapWithExpectedSize(numericTypes.size());
        for (FieldSpec.DataType type : numericTypes) {
          UdfParameter[] paramsAndResult = new UdfParameter[_arity + 1];
          for (int i = 0; i < _arity; i++) {
            paramsAndResult[i] = UdfParameter.of("arg" + i, type);
          }
          paramsAndResult[_arity] = UdfParameter.result(type);
          UdfSignature signature = UdfSignature.of(paramsAndResult);
          Set<UdfExample> newCases = _cases.stream()
              .map(testCase -> new EndomorphismNumericTypesBuilder.NumericPinotUdfExample(testCase, type))
              .collect(Collectors.toSet());
          cases.put(signature, newCases);
        }
        return cases;
      };
    }

    private static class NumericPinotUdfExample extends UdfExample {
      private final UdfExample _base;
      private final FieldSpec.DataType _type;

      public NumericPinotUdfExample(UdfExample base, FieldSpec.DataType type) {
        Preconditions.checkArgument(base.getResult(NullHandling.ENABLED) instanceof Number
                || base.getResult(NullHandling.ENABLED) == null,
            "Base test case must return a Number type for numeric endomorphism functions");
        Preconditions.checkArgument(base.getResult(NullHandling.DISABLED) instanceof Number
                || base.getResult(NullHandling.DISABLED) == null,
            "Base test case must return a Number type for numeric endomorphism functions");
        if (base.getInputValues().stream()
            .anyMatch(value -> value != null && !(value instanceof Number))) {
          throw new IllegalStateException(
              "Base test case must have all input values as Number type for numeric endomorphism functions, got: "
                  + base.getInputValues().stream()
                  .map(value -> value + " (of type " + value.getClass().getName() + ")")
                  .collect(Collectors.joining(", "))
          );
        }
        Preconditions.checkArgument(type == FieldSpec.DataType.INT
                || type == FieldSpec.DataType.LONG
                || type == FieldSpec.DataType.FLOAT
                || type == FieldSpec.DataType.DOUBLE
                || type == FieldSpec.DataType.BIG_DECIMAL,
            "Type %s is not a numeric for numeric type", type);
        _base = base;
        _type = type;
      }

      @Override
      public String getId() {
        return _base.getId() + "_" + _type.name().toLowerCase(Locale.US);
      }

      @Override
      public List<Object> getInputValues() {
        return _base.getInputValues().stream()
            .map(value -> {
              if (value instanceof Number) {
                switch (_type) {
                  case INT:
                    return ((Number) value).intValue();
                  case LONG:
                    return ((Number) value).longValue();
                  case FLOAT:
                    return ((Number) value).floatValue();
                  case DOUBLE:
                    return ((Number) value).doubleValue();
                  case BIG_DECIMAL:
                    return BigDecimal.valueOf(((Number) value).doubleValue());
                  default:
                    throw new IllegalArgumentException("Unsupported type: " + _type);
                }
              }
              assert value == null : "Input value must be a Number or null for numeric endomorphism functions, "
                  + "but found " + value + " of type " + value.getClass().getName();
              return null;
            })
            .collect(Collectors.toList());
      }

      @Override
      public Object getResult(NullHandling nullHandling) {
        Object baseValue = _base.getResult(nullHandling);
        if (baseValue instanceof Number) {
          switch (_type) {
            case INT:
              return ((Number) baseValue).intValue();
            case LONG:
              return ((Number) baseValue).longValue();
            case FLOAT:
              return ((Number) baseValue).floatValue();
            case DOUBLE:
              return ((Number) baseValue).doubleValue();
            case BIG_DECIMAL:
              return BigDecimal.valueOf(((Number) baseValue).doubleValue());
            default:
              throw new IllegalArgumentException("Unsupported type: " + _type);
          }
        }
        assert baseValue == null : "Input value must be a Number or null for numeric endomorphism functions, "
            + "but found " + baseValue + " of type " + baseValue.getClass().getName();
        return null;
      }
    }
  }

  class MultiBuilder implements UdfExampleBuilder {
    private final Map<UdfSignature, Set<UdfExample>> _cases = new HashMap<>();

    public MultiBuilder and(UdfExampleBuilder testCaseGenerator) {
      Map<UdfSignature, Set<UdfExample>> newCases = testCaseGenerator.generateExamples();
      for (Map.Entry<UdfSignature, Set<UdfExample>> entry : newCases.entrySet()) {
        _cases.merge(entry.getKey(), entry.getValue(), (existing, newer) -> {
          existing.addAll(newer);
          return existing;
        });
      }
      return this;
    }

    @Override
    public Map<UdfSignature, Set<UdfExample>> generateExamples() {
      return _cases;
    }
  }
}
