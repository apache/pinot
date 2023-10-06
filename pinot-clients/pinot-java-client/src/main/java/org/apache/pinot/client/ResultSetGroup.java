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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;


/**
 * A Pinot result set group, containing the results given back by Pinot for a given query.
 */
public class ResultSetGroup {
  private final List<ResultSet> _resultSets;
  @Getter
  private final ExecutionStats _executionStats;
  @Getter
  private final List<PinotClientException> _exceptions;

  ResultSetGroup(BrokerResponse brokerResponse) {
    _resultSets = new ArrayList<>();

    if (brokerResponse.getResultTable() != null) {
      _resultSets.add(new ResultTableResultSet(brokerResponse.getResultTable()));
    } else {

      if (brokerResponse.getSelectionResults() != null) {
        _resultSets.add(new SelectionResultSet(brokerResponse.getSelectionResults()));
      }

      int aggregationResultCount = brokerResponse.getAggregationResultsSize();
      for (int i = 0; i < aggregationResultCount; i++) {
        JsonNode aggregationResult = brokerResponse.getAggregationResults().get(i);
        if (aggregationResult.has("value")) {
          _resultSets.add(new AggregationResultSet(aggregationResult));
        } else if (aggregationResult.has("groupByResult")) {
          _resultSets.add(new GroupByResultSet(aggregationResult));
        } else {
          throw new PinotClientException("Unrecognized result group, neither a value nor group by result");
        }
      }
    }
    _executionStats = brokerResponse.getExecutionStats();
    _exceptions = getPinotClientExceptions(brokerResponse.getExceptions());
  }

  private static List<PinotClientException> getPinotClientExceptions(
      @Nullable JsonNode exceptionsJson) {
    List<PinotClientException> exceptions = new ArrayList<>();
    if (exceptionsJson != null && exceptionsJson.isArray()) {
      for (int i = 0; i < exceptionsJson.size(); i++) {
        exceptions.add(new PinotClientException(exceptionsJson.get(i).toPrettyString()));
      }
    }
    return exceptions;
  }

  /**
   * Returns the number of result sets in this result set group, or 0 if there are no result sets; there is one result
   * set per aggregation function in the original query and one result set in the case of a selection query.
   *
   * @return The number of result sets in this result set
   */
  public int getResultSetCount() {
    return _resultSets.size();
  }

  /**
   * Obtains the result set at the given index, starting from zero.
   *
   * @param index The index for which to obtain the result set
   * @return The result set at the given index
   */
  public ResultSet getResultSet(int index) {
    return _resultSets.get(index);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (ResultSet resultSet : _resultSets) {
      sb.append(resultSet);
      sb.append("\n");
    }
    sb.append(_executionStats.toString());
    sb.append("\n");
    for (PinotClientException exception : _exceptions) {
      sb.append(exception);
      sb.append("\n");
    }
    return sb.toString();
  }
}
