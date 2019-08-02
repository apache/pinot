/*
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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;


/**
 * Flatten the anomaly results for UI purpose.
 * Convert a list of anomalies to rows of columns so that UI can directly convert the anomalies to table
 */
@Path("thirdeye/table")
public class AnomalyFlattenResource {
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String ANOMALY_ID = "anomalyId";
  public static final String ANOMALY_COMMENT = "comment";
  public static final String FUNCTION_ID = "functionId";
  public static final String WINDOW_START = "start";
  public static final String WINDOW_END = "end";
  private static final String DIMENSION_KEYS  = "keys";

  private static final List<String> REQUIRED_JSON_KEYS =Arrays.asList(FUNCTION_ID, WINDOW_START, WINDOW_END);
  private static final String DEFAULT_DELIMINATOR = ",";

  public AnomalyFlattenResource() {
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
  }

  public AnomalyFlattenResource(MergedAnomalyResultManager mergedAnomalyResultDAO) {
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
  }

  /**
   * Flatten a list of anomaly results to a list of map
   * @param jsonPayload a json string; the jsonPayload should include keys: [functionId, start, end].
   *                    start and end are in the format of epoch time
   * @return a list of map
   * @throws IOException
   */
  @GET
  @ApiOperation(value = "View a flatted merged anomalies for collection")
  public List<Map<String, String>> flatAnomalyResults(String jsonPayload) throws IOException {
    if (StringUtils.isBlank(jsonPayload)) {
      throw new IllegalArgumentException("jsonPayload cannot be null or empty");
    }
    Map<String, Object> inputMap = OBJECT_MAPPER.readValue(jsonPayload, Map.class);

    // Assert if reuired keys are in the map
    for (String requiredKey : REQUIRED_JSON_KEYS) {
      if (!inputMap.containsKey(requiredKey)) {
        throw new IllegalArgumentException(String.format("Miss %s in input json String; %s are required", requiredKey,
            REQUIRED_JSON_KEYS.toString()));
      }
    }

    // Retrieve anomalies
    long functionId = Long.valueOf(inputMap.get(FUNCTION_ID).toString());
    long start = Long.valueOf(inputMap.get(WINDOW_START).toString());
    long end = Long.valueOf(inputMap.get(WINDOW_END).toString());
    List<MergedAnomalyResultDTO> anomalies = mergedAnomalyResultDAO.
        findByStartTimeInRangeAndDetectionConfigId(start, end, functionId);

    // flatten anomaly result information
    List<String> tableKeys = null;
    if (inputMap.containsKey(DIMENSION_KEYS)){
      Object dimensionKeysObj = inputMap.get(DIMENSION_KEYS);
      if (dimensionKeysObj instanceof List) {
        tableKeys = (List<String>) dimensionKeysObj;
      } else {
        tableKeys = Arrays.asList(dimensionKeysObj.toString().split(DEFAULT_DELIMINATOR));
      }
    }
    List<Map<String, String>> resultList = new ArrayList<>();
    for (MergedAnomalyResultDTO result : anomalies) {
      resultList.add(flatAnomalyResult(result, tableKeys));
    }
    return resultList;
  }

  /**
   * Flat an anomaly to a flat map structure
   * @param anomalyResult an instance of MergedAnomalyResultDTO
   * @param tableKeys a list of keys in dimensions; if null, use all keys in dimension
   * @return a map of information in the anomaly result with the required keys
   */
  public static Map<String, String> flatAnomalyResult(MergedAnomalyResultDTO anomalyResult,
      List<String> tableKeys) {
    Preconditions.checkNotNull(anomalyResult);
    Map<String, String> flatMap = new HashMap<>();
    flatMap.put(ANOMALY_ID, Long.toString(anomalyResult.getId()));
    DimensionMap dimension = anomalyResult.getDimensions();
    if (tableKeys == null) {
      tableKeys = new ArrayList<>(dimension.keySet());
    }
    for (String key : tableKeys) {
      flatMap.put(key, dimension.containsKey(key)? dimension.get(key) : "");
    }
    AnomalyFeedback feedback = anomalyResult.getFeedback();
    if (feedback != null) {
      flatMap.put(ANOMALY_COMMENT, feedback.getComment());
    } else {
      flatMap.put(ANOMALY_COMMENT, "");
    }
    flatMap.put(anomalyResult.getMetric(), Double.toString(anomalyResult.getAvgCurrentVal()));
    return flatMap;
  }
}
