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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.util.Preconditions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


/// The result of [UdfTestFramework#execute()].
///
/// This class is basically a Map from Udf to [UdfTestResult#ByScenario].
///
/// In order to serialize and deserialize this class with Jackson, we use a [UdfTestResult.Dto] pattern where
/// instead of using [Udf], [UdfSignature], etc we use their string identifiers as keys in the map. These DTOs can be
/// created using the [#asDto] method, which converts the result into a DTO representation, and can be converted back
/// to a [UdfTestResult] using the [Dto#asUdfTestResult(Function, Function)] method, which requires functions to map
/// ids to the actual UDF and scenario objects. This means that in order to convert the DTO back to a [UdfTestResult],
/// you need to have access to the UDFs and scenarios that were used in the test, as the DTOs only contain their IDs.
/// This should not be problematic, as:
/// * The DTOs are not used to transfer the results between processes, but rather to serialize them to a file in order
/// to use it later on different tests.
/// * The DTOs are usually good enough to create reports or to display the results in a UI.
public class UdfTestResult {

  private final Map<Udf, ByScenario> _results;

  public UdfTestResult(Map<Udf, ByScenario> results) {
    _results = results;
  }

  public Map<Udf, ByScenario> getResults() {
    return _results;
  }

  public Dto asDto() {
    Map<String, ByScenario.Dto> dtoMap = _results.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getKey().getMainName(),
            entry -> entry.getValue().asDto()
        ));
    return new Dto(dtoMap);
  }

  public static class Dto {
    private final Map<String, ByScenario.Dto> _map;

    @JsonCreator
    public Dto(@JsonAnySetter Map<String, ByScenario.Dto> map) {
      _map = map;
    }

    @JsonAnyGetter
    public Map<String, ByScenario.Dto> getMap() {
      return _map;
    }

    /// Converts this DTO into a [UdfTestResult] using the provided functions to map UDF and scenario IDs to their
    /// actual objects.
    /// @param udfById a function that maps a UDF ID to the actual UDF object
    /// @param scenarioById a function that maps a scenario ID to the actual UdfTestScenario object
    public UdfTestResult asUdfTestResult(
        Function<String, Udf> udfById,
        Function<String, UdfTestScenario> scenarioById
    ) {
      Map<Udf, ByScenario> udfMap = Maps.newHashMapWithExpectedSize(_map.size());

      for (Map.Entry<String, ByScenario.Dto> entry : _map.entrySet()) {
        String udfId = entry.getKey();
        Udf udf = udfById.apply(udfId);
        Preconditions.checkState(udf != null, "No UDF object found for: %s", udfId);
        ByScenario byScenario = entry.getValue().asByScenario(udf, scenarioById);
        udfMap.put(udf, byScenario);
      }
      return new UdfTestResult(udfMap);
    }
  }

  public static class ByScenario {
    private final Map<UdfTestScenario, BySignature> _map;

    public ByScenario(Map<UdfTestScenario, BySignature> map) {
      _map = map;
    }

    public Map<UdfTestScenario, BySignature> getMap() {
      return _map;
    }

    public Dto asDto() {
      TreeMap<String, BySignature.Dto> dtoMap = _map.entrySet().stream()
          .collect(Collectors.toMap(
              entry -> entry.getKey().getTitle(),
              entry -> entry.getValue().asDto(),
              (existing, replacement) -> existing, // This should not happen, but just in case
              TreeMap::new
          ));
      return new Dto(dtoMap);
    }

    public static class Dto {
      private final SortedMap<String, BySignature.Dto> _map;

      @JsonCreator
      public Dto(@JsonAnySetter SortedMap<String, BySignature.Dto> map) {
        _map = map;
      }

      @JsonAnyGetter
      public SortedMap<String, BySignature.Dto> getMap() {
        return _map;
      }

      /// Converts this DTO into a [ByScenario] using the provided UDF and scenario functions.
      /// @param udf the UDF object to use for the conversion
      /// @param scenarioById a function that maps a scenario ID to the actual UdfTestScenario object
      public ByScenario asByScenario(Udf udf, Function<String, UdfTestScenario> scenarioById) {
        Map<UdfTestScenario, BySignature> scenarioMap = Maps.newHashMapWithExpectedSize(_map.size());

        for (Map.Entry<String, BySignature.Dto> entry : _map.entrySet()) {
          String scenarioId = entry.getKey();
          UdfTestScenario scenario = scenarioById.apply(scenarioId);
          Preconditions.checkState(scenario != null, "No scenario object found for: %s", scenarioId);

          scenarioMap.put(scenario, entry.getValue().asBySignature(udf));
        }
        return new ByScenario(scenarioMap);
      }
    }
  }

  public static class BySignature {
    private final Map<UdfSignature, ResultByExample> _map;

    public BySignature(Map<UdfSignature, ResultByExample> map) {
      _map = map;
    }

    public Map<UdfSignature, ResultByExample> getMap() {
      return _map;
    }

    public Dto asDto() {
      SortedMap<String, ResultByExample.Dto> dtoMap = _map.entrySet().stream()
          .collect(Collectors.toMap(
              entry -> entry.getKey().toString(),
              entry -> entry.getValue().asDto(),
              (existing, replacement) -> existing, // This should not happen, but just in case
              TreeMap::new
          ));
      return new Dto(dtoMap);
    }

    public static class Dto {
      private final SortedMap<String, ResultByExample.Dto> _map;

      @JsonCreator
      public Dto(@JsonAnySetter SortedMap<String, ResultByExample.Dto> map) {
        _map = map;
      }

      @JsonAnyGetter
      public SortedMap<String, ResultByExample.Dto> getMap() {
        return _map;
      }

      /// Converts this DTO into a [BySignature] using the provided UDF object.
      /// @param udf the UDF object to use for the conversion
      public BySignature asBySignature(Udf udf) {
        Map<UdfSignature, ResultByExample> signatureMap = Maps.newHashMapWithExpectedSize(_map.size());

        Map<String, UdfSignature> signatureByString = udf.getExamples().keySet().stream()
            .collect(Collectors.toMap(
                UdfSignature::toString,
                Function.identity()
            ));

        for (Map.Entry<String, ResultByExample.Dto> entry : _map.entrySet()) {
          UdfSignature signature = signatureByString.get(entry.getKey());

          Preconditions.checkState(signature != null, "No signature object found for: %s", entry.getKey());
          Map<String, UdfExample> exampleById = udf.getExamples().get(signature).stream()
              .collect(Collectors.toMap(
                  UdfExample::getId,
                  Function.identity()
              ));

          signatureMap.put(signature, entry.getValue().asResultByExample(exampleById::get));
        }
        return new BySignature(signatureMap);
      }
    }
  }
}
