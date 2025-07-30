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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/// The inputs and the expected result for a given [UDF][Udf] call.
///
/// These objects are usually created by the [#create(String, Object...)] factory method and decorated when needed
/// calling [#withoutNull(Object)].
public abstract class UdfExample {
  /// A descriptive ID of what this example is showing.
  public abstract String getId();

  /// The input values.
  ///
  /// Notice that values should be the same used to call the function and should match the types indicated by the
  /// signature associated with this example. For example, in the case of plus udf, args could be doubles 1.0 and 2.0 or
  /// ints 1 and 2, but not double 1.0 and int 2. That sum is possible at SQL level, but in that case the different
  /// engines apply automatic casting to make sure the udf is called with either two ints or two doubles.
  public abstract List<Object> getInputValues();

  /// The result of the example. It may be different depending on whether null handling is enabled or not.
  public abstract Object getResult(NullHandling nullHandling);

  /// Creates a new UdfExample with the given name and some values.
  ///
  /// The values array is processed assuming that the last value is the expected result, while all the previous values
  /// are the inputs to the UDF.
  ///
  /// The returned UdfExample will always return the same result regardless of whether null handling is enabled or not.
  /// In case you want to create a UdfExample that returns different results depending on null handling, you should
  /// call [#withoutNull(Object)] on the returned UdfExample and pass the expected result when null handling is
  /// disabled.
  public static UdfExample create(String name, Object... values) {
    int inputsSize = values.length - 1;
    ArrayList<Object> inputs = new ArrayList<>(inputsSize);
    inputs.addAll(Arrays.asList(values).subList(0, inputsSize));
    Object expectedResult = values[inputsSize];
    return new Default(name, inputs, expectedResult);
  }

  /// Returns a new UdfExample that will use the given result when null handling is disabled.
  ///
  /// Remember that normally the result is the same regardless of null handling, but in some cases it may be different.
  public UdfExample withoutNull(Object result) {
    return new WithoutNullHandling(this, result);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UdfExample)) {
      return false;
    }
    UdfExample that = (UdfExample) o;
    if (!getId().equals(that.getId())) {
      return false;
    }
    List<Object> myInputs = getInputValues();
    List<Object> otherInputs = that.getInputValues();
    if (myInputs.size() != otherInputs.size()) {
      return false;
    }
    for (int i = 0; i < myInputs.size(); i++) {
      if (!equalValues(myInputs.get(i), otherInputs.get(i))) {
        return false;
      }
    }
    return equalValues(getResult(NullHandling.DISABLED), that.getResult(NullHandling.DISABLED))
        && equalValues(getResult(NullHandling.ENABLED), that.getResult(NullHandling.ENABLED));
  }

  private boolean equalValues(Object result1, Object result2) {
    if (result1 == null && result2 == null) {
      return true;
    }
    if (result1 == null || result2 == null) {
      return false;
    }
    if (result1.getClass().isArray() && result2.getClass().isArray()) {
      if (!result1.getClass().equals(result2.getClass())) {
        return false;
      }
      if (result1.getClass().equals(byte[].class)) {
        return Arrays.equals((byte[]) result1, (byte[]) result2);
      }
      return Arrays.equals((Object[]) result1, (Object[]) result2);
    }
    return Objects.equals(result1, result2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getId());
  }

  public static class Default extends UdfExample {
    private final String _id;
    private final List<Object> _inputValues;
    private final Object _expectedResult;

    private Default(String id, List<Object> inputValues, Object expectedResult) {
      _id = id;
      _inputValues = inputValues;
      _expectedResult = expectedResult;
    }

    @Override
    public String getId() {
      return _id;
    }

    @Override
    public List<Object> getInputValues() {
      return _inputValues;
    }

    @Override
    public Object getResult(NullHandling nullHandling) {
      return _expectedResult;
    }
  }

  /// A decorator for UdfExample that allows to return a different result when null handling is disabled.
  public static class WithoutNullHandling extends UdfExample {
    private final UdfExample _base;
    private final Object _result;

    private WithoutNullHandling(UdfExample base, Object result) {
      _base = base;
      _result = result;
    }

    @Override
    public List<Object> getInputValues() {
      return _base.getInputValues();
    }

    @Override
    public String getId() {
      return _base.getId();
    }

    @Override
    public Object getResult(NullHandling nullHandling) {
      return nullHandling == NullHandling.DISABLED ? _result : _base.getResult(NullHandling.ENABLED);
    }

    /// Returns a new UdfExample that uses the given result when null handling is disabled instead of the one provided
    /// by the receiver object.
    ///
    /// This is similar to calling [#withoutNull(Object)] on the decorated object.
    /// Calling this method doesn't seem to make much sense, but it is provided for consistency with the decorated
    /// class.
    @Override
    public UdfExample withoutNull(Object result) {
      return _base.withoutNull(result);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof WithoutNullHandling)) {
        return false;
      }
      WithoutNullHandling that = (WithoutNullHandling) o;
      return Objects.equals(_base, that._base) && Objects.equals(_result, that._result);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_base, _result);
    }
  }

  public enum NullHandling {
    ENABLED,
    DISABLED
  }
}
