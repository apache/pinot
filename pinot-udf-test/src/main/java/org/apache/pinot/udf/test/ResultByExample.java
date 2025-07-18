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
package org.apache.pinot.udf.test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.core.udf.UdfExample;

/// The result of executing a bunch of [UdfExample] associated with the same signature.
///
/// This class encapsulates the results of executing a UDF against multiple examples, providing a structured way to
/// represent both successful and failed executions.
///
/// There are two sublasses:
/// 1. `Failure`: Represents a failure to execute the UDF, containing an error message. This is used by some scenarios
/// to indicate an error running the scenario itself. This means that the scenario wasn't able to extract the results
/// for each example, and thus the results are not available.
/// 2. `Partial`: Represents an execution of the UDF against multiple examples. For each example, it contains whether
/// the example passed or failed, the expected and actual results and the equivalence between them.
///
/// Each class has its own [Dto] representation, which can be used to serialize the results into a JSON/YAML format.
/// In order to make this serialization simpler, there is a single `Dto` class that is used to represent both `Partial`
/// and `Failure` results. You can use [#asDto()] to convert a `ResultByExample` instance into its DTO representation
/// and vice versa using [Dto#asResultByExample(Function)].
public abstract class ResultByExample {

  public abstract Dto asDto();

  public static class Partial extends ResultByExample {
    private final Map<UdfExample, UdfExampleResult> _resultsByExample;
    private final Map<UdfExample, UdfTestFramework.EquivalenceLevel> _equivalenceByExample;
    private final Map<UdfExample, String> _errorsByExample;

    public Partial(Map<UdfExample, UdfExampleResult> resultsByExample,
        Map<UdfExample, UdfTestFramework.EquivalenceLevel> equivalenceByExample,
        Map<UdfExample, String> errorsByExample) {
      _resultsByExample = resultsByExample;
      _equivalenceByExample = equivalenceByExample;
      _errorsByExample = errorsByExample;
    }

    public Map<UdfExample, UdfExampleResult> getResultsByExample() {
      return _resultsByExample;
    }

    public Map<UdfExample, UdfTestFramework.EquivalenceLevel> getEquivalenceByExample() {
      return _equivalenceByExample;
    }

    public Map<UdfExample, String> getErrorsByExample() {
      return _errorsByExample;
    }

    @Override
    public Dto asDto() {
      SortedMap<String, Dto.DtoEntry> dtoEntries = new TreeMap<>();
      for (Map.Entry<UdfExample, UdfExampleResult> entry : _resultsByExample.entrySet()) {
        UdfExample example = entry.getKey();
        UdfExampleResult result = entry.getValue();
        String error = _errorsByExample.get(example);
        dtoEntries.put(example.getId(),
            new Dto.DtoEntry(result.getExpectedResult(), result.getActualResult(),
                _equivalenceByExample.get(example), error));
      }
      return new Dto(dtoEntries, null);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Partial)) {
        return false;
      }
      Partial partial = (Partial) o;
      return Objects.equals(getResultsByExample(), partial.getResultsByExample()) && Objects.equals(
          getEquivalenceByExample(), partial.getEquivalenceByExample()) && Objects.equals(getErrorsByExample(),
          partial.getErrorsByExample());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getResultsByExample(), getEquivalenceByExample(), getErrorsByExample());
    }
  }

  public static class Failure extends ResultByExample {
    private final String _errorMessage;

    public Failure(String errorMessage) {
      _errorMessage = errorMessage;
    }

    public String getErrorMessage() {
      return _errorMessage;
    }

    @Override
    public Dto asDto() {
      return new Dto(null, _errorMessage);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Failure)) {
        return false;
      }
      Failure that = (Failure) o;
      return _errorMessage.equals(that._errorMessage);
    }

    @Override
    public int hashCode() {
      return _errorMessage.hashCode();
    }
  }

  public static class Dto {
    @Nullable
    private final SortedMap<String, DtoEntry> _entries;
    @Nullable
    private final String _errorMessage;

    @JsonCreator
    public Dto(
        @Nullable @JsonProperty("entries") SortedMap<String, DtoEntry> entries,
        @JsonProperty("globalError") @Nullable String errorMessage) {
      _entries = entries;
      _errorMessage = errorMessage;
    }

    @Nullable
    public SortedMap<String, DtoEntry> getEntries() {
      return _entries;
    }

    @Nullable
    public String getErrorMessage() {
      return _errorMessage;
    }

    public boolean isError() {
      return _errorMessage != null;
    }

    public ResultByExample asResultByExample(Function<String, UdfExample> exampleById) {
      if (isError()) {
        return new Failure(_errorMessage);
      }
      assert _entries != null;
      int size = _entries.size();
      Map<UdfExample, UdfExampleResult> resultsByExample = Maps.newHashMapWithExpectedSize(size);
      Map<UdfExample, UdfTestFramework.EquivalenceLevel> equivalenceByExample = Maps.newHashMapWithExpectedSize(size);
      Map<UdfExample, String> errorsByExample = Maps.newHashMapWithExpectedSize(size);

      for (Map.Entry<String, DtoEntry> entry : _entries.entrySet()) {
        String exampleId = entry.getKey();
        DtoEntry dtoEntry = entry.getValue();
        UdfExample example = exampleById.apply(exampleId);

        UdfExampleResult result = dtoEntry.getError() == null
            ? UdfExampleResult.success(example, dtoEntry.getActualResult(), dtoEntry.getExpectedResult())
            : UdfExampleResult.error(example, dtoEntry.getError());
        resultsByExample.put(example, result);
        equivalenceByExample.put(example, dtoEntry.getEquivalence());
        errorsByExample.put(example, dtoEntry.getError());
      }

      return new Partial(resultsByExample, equivalenceByExample, errorsByExample);
    }

    public static class DtoEntry {
      private final Object _expectedResult;
      private final Object _actualResult;
      private final UdfTestFramework.EquivalenceLevel _equivalence;
      @Nullable
      private final String _error;

      @JsonCreator
      public DtoEntry(
          @JsonProperty("expectedResult") Object expectedResult,
          @JsonProperty("actualResult") Object actualResult,
          @JsonProperty("equivalence") UdfTestFramework.EquivalenceLevel equivalence,
          @JsonProperty("error") @Nullable String error) {
        _expectedResult = expectedResult;
        _actualResult = actualResult;
        _equivalence = equivalence;
        _error = error;
      }

      public Object getExpectedResult() {
        return _expectedResult;
      }

      public Object getActualResult() {
        return _actualResult;
      }

      public UdfTestFramework.EquivalenceLevel getEquivalence() {
        return _equivalence;
      }

      @Nullable
      public String getError() {
        return _error;
      }
    }
  }
}
