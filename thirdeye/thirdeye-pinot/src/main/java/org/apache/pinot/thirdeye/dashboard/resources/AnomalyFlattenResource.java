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

import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


/**
 * Flatten the anomaly results for UI purpose.
 * Convert a list of anomalies to rows of columns so that UI can directly convert the anomalies to table
 */
@Path("thirdeye/table")
@Api(tags = {Constants.ANOMALY_TAG})
public class AnomalyFlattenResource {
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  public static final String ANOMALY_ID = "anomalyId";
  public static final String ANOMALY_COMMENT = "comment";

  public AnomalyFlattenResource(MergedAnomalyResultManager mergedAnomalyResultDAO) {
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
  }

  /**
   * Returns a list of formatted merged anomalies for UI to render a table
   * @param detectionConfigId detectionConfigId
   * @param start start time in epoc milliseconds
   * @param end end time in epoc milliseconds
   * @param dimensionKeys a list of keys in dimensions; if null, will return all  dimension keys for the anomaly
   * @return a list of formatted anomalies
   */
  @GET
  @ApiOperation(value = "Returns a list of formatted merged anomalies ")
  public List<Map<String, Object>> flatAnomalyResults(
      @ApiParam("detection config id") @QueryParam("detectionConfigId") long detectionConfigId,
      @ApiParam("start time for anomalies") @QueryParam("start") long start,
      @ApiParam("end time for anomalies") @QueryParam("end") long end,
      @ApiParam("dimension keys") @QueryParam("dimensionKeys") List<String> dimensionKeys) {
    // Retrieve anomalies
    List<MergedAnomalyResultDTO> anomalies = mergedAnomalyResultDAO.
        findByStartTimeInRangeAndDetectionConfigId(start, end, detectionConfigId);

    // flatten anomaly result information
    List<Map<String, Object>> resultList = new ArrayList<>();
    for (MergedAnomalyResultDTO result : anomalies) {
      resultList.add(flatAnomalyResult(result, dimensionKeys));
    }
    return resultList;
  }

  /**
   * Flat an anomaly to a flat map structure
   * @param anomalyResult an instance of MergedAnomalyResultDTO
   * @param tableKeys a list of keys in dimensions; if null, use all keys in dimension
   * @return a map of information in the anomaly result with the required keys
   */
  public static Map<String, Object> flatAnomalyResult(MergedAnomalyResultDTO anomalyResult,
      List<String> tableKeys) {
    Preconditions.checkNotNull(anomalyResult);
    Map<String, Object> flatMap = new HashMap<>();
    flatMap.put(ANOMALY_ID, anomalyResult.getId());
    DimensionMap dimension = anomalyResult.getDimensions();
    if (tableKeys == null) {
      tableKeys = new ArrayList<>(dimension.keySet());
    }
    for (String key : tableKeys) {
      flatMap.put(key, dimension.getOrDefault(key, ""));
    }
    AnomalyFeedback feedback = anomalyResult.getFeedback();
    if (feedback != null) {
      flatMap.put(ANOMALY_COMMENT, feedback.getComment());
    } else {
      flatMap.put(ANOMALY_COMMENT, "");
    }
    flatMap.put(anomalyResult.getMetric(), anomalyResult.getAvgCurrentVal());
    return flatMap;
  }
}
