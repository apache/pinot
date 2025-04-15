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
package org.apache.pinot.common.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/**
 * POJO returned by the Pinot broker in a time-series query response. Format is defined
 * <a href="https://prometheus.io/docs/prometheus/latest/querying/api/">in the prometheus docs.</a>
 * TODO: We will evolve this until Pinot 1.3. At present we are mimicking Prometheus HTTP API.
 */
@InterfaceStability.Evolving
public class PinotBrokerTimeSeriesResponse {
  public static final String SUCCESS_STATUS = "success";
  public static final String ERROR_STATUS = "error";
  /**
   * Prometheus returns a __name__ tag to denote the "name" of a time-series query. Time series language developers
   * can set this tag in the final operator in their queries, which would allow them to configure the name tag
   * returned by the Pinot Broker. By default, we use {@link TimeSeries#getTagsSerialized()} as the name of a series.
   */
  public static final String METRIC_NAME_KEY = "__name__";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private String _status;
  private Data _data;
  private String _errorType;
  private String _error;

  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  @JsonCreator
  public PinotBrokerTimeSeriesResponse(@JsonProperty("status") String status, @JsonProperty("data") Data data,
      @JsonProperty("errorType") String errorType, @JsonProperty("error") String error) {
    _status = status;
    _data = data;
    _errorType = errorType;
    _error = error;
  }

  public String getStatus() {
    return _status;
  }

  public Data getData() {
    return _data;
  }

  public String getErrorType() {
    return _errorType;
  }

  public String getError() {
    return _error;
  }

  public String serialize()
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  public static PinotBrokerTimeSeriesResponse newSuccessResponse(Data data) {
    return new PinotBrokerTimeSeriesResponse(SUCCESS_STATUS, data, null, null);
  }

  public static PinotBrokerTimeSeriesResponse newErrorResponse(String errorType, String errorMessage) {
    return new PinotBrokerTimeSeriesResponse(ERROR_STATUS, Data.EMPTY, errorType, errorMessage);
  }

  public static PinotBrokerTimeSeriesResponse fromTimeSeriesBlock(TimeSeriesBlock seriesBlock) {
    if (seriesBlock.getTimeBuckets() == null) {
      throw new UnsupportedOperationException("Non-bucketed series block not supported yet");
    }
    return convertBucketedSeriesBlock(seriesBlock);
  }

  private static PinotBrokerTimeSeriesResponse convertBucketedSeriesBlock(TimeSeriesBlock seriesBlock) {
    Long[] timeValues = Objects.requireNonNull(seriesBlock.getTimeBuckets()).getTimeBuckets();
    List<PinotBrokerTimeSeriesResponse.Value> result = new ArrayList<>();
    for (var listOfTimeSeries : seriesBlock.getSeriesMap().values()) {
      Preconditions.checkState(!listOfTimeSeries.isEmpty(), "Received empty time-series");
      TimeSeries anySeries = listOfTimeSeries.get(0);
      Map<String, String> metricMap = new HashMap<>();
      // Set the default metric name key. This may be overridden in the very next line if the language implementation
      // sets its own name tag.
      metricMap.put(METRIC_NAME_KEY, anySeries.getTagsSerialized());
      metricMap.putAll(anySeries.getTagKeyValuesAsMap());
      for (TimeSeries timeSeries : listOfTimeSeries) {
        Object[][] values = new Object[timeValues.length][];
        for (int i = 0; i < timeValues.length; i++) {
          Object nullableValue = timeSeries.getDoubleValues()[i];
          values[i] = new Object[]{timeValues[i], nullableValue == null ? null : nullableValue.toString()};
        }
        result.add(new PinotBrokerTimeSeriesResponse.Value(metricMap, values));
      }
    }
    PinotBrokerTimeSeriesResponse.Data data = PinotBrokerTimeSeriesResponse.Data.newMatrix(result);
    return PinotBrokerTimeSeriesResponse.newSuccessResponse(data);
  }

  public static class Data {
    public static final Data EMPTY = new Data("", new ArrayList<>());
    private final String _resultType;
    private final List<Value> _result;

    @JsonCreator
    public Data(@JsonProperty("resultType") String resultType, @JsonProperty("result") List<Value> result) {
      _resultType = resultType;
      _result = result;
    }

    public String getResultType() {
      return _resultType;
    }

    public List<Value> getResult() {
      return _result;
    }

    public static Data newMatrix(List<Value> result) {
      return new Data("matrix", result);
    }
  }

  public static class Value {
    private final Map<String, String> _metric;
    private final Object[][] _values;

    @JsonCreator
    public Value(@JsonProperty("metric") Map<String, String> metric, @JsonProperty("values") Object[][] values) {
      _metric = metric;
      _values = values;
    }

    public Map<String, String> getMetric() {
      return _metric;
    }

    public Object[][] getValues() {
      return _values;
    }
  }
}
