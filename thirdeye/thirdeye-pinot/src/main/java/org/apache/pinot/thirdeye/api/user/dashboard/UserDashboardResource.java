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

package org.apache.pinot.thirdeye.api.user.dashboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ResourceUtils;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomalySummary;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Endpoints for user-customized dashboards (currently alerts only)
 */
@Api(tags = {Constants.DASHBOARD_TAG})
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class UserDashboardResource {
  protected static final Logger LOG = LoggerFactory.getLogger(UserDashboardResource.class);

  private static final int ANOMALIES_LIMIT_DEFAULT = 500;

  private final MergedAnomalyResultManager anomalyDAO;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final DetectionConfigManager detectionDAO;
  private final DetectionAlertConfigManager detectionAlertDAO;

  @Inject
  public UserDashboardResource(MergedAnomalyResultManager anomalyDAO,
      MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO,
      DetectionConfigManager detectionDAO,
      DetectionAlertConfigManager detectionAlertDAO) {
    this.anomalyDAO = anomalyDAO;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.detectionDAO = detectionDAO;
    this.detectionAlertDAO = detectionAlertDAO;
  }

  public List<AnomalySummary> queryAnomalies(Long start, Long end, String application, String group, String metric,
      String dataset, List<MetricDatasetPair> metricDatasetPairs, boolean fetchTrueAnomaly, Integer limit) {
    if (limit == null) {
      LOG.warn("No upper limit specified while fetching anomalies. Defaulting to " + ANOMALIES_LIMIT_DEFAULT);
      limit = ANOMALIES_LIMIT_DEFAULT;
    }
    Preconditions.checkNotNull(start, "Please specify the start time of the anomaly retrieval window");

    List<Set<MergedAnomalyResultDTO>> anomalySets = new ArrayList<>();
    if (group != null) {
      // Fetch anomalies by group
      anomalySets.add(new HashSet<>(fetchAnomaliesBySubsGroup(start, end, group)));
    }
    if (application != null) {
      // Fetch anomalies by application
      anomalySets.add(new HashSet<>(fetchAnomaliesByApplication(start, end, application)));
    }
    if (metric != null || dataset != null) {
      // Fetch anomalies by metric and/or dataset
      anomalySets.add(new HashSet<>(fetchAnomaliesByMetricDataset(start, end, metric, dataset)));
    }
    if (metricDatasetPairs != null && !metricDatasetPairs.isEmpty()) {
      // Fetch anomalies by metric dataset pairs
      anomalySets.add(new HashSet<>(fetchAnomaliesByMetricDatasetPairs(start, end, metricDatasetPairs)));
    }
    if (anomalySets.isEmpty()) {
      return getAnomalyFormattedOutput(new ArrayList<>());
    }
    // calculate intersection of all non-empty results
    for (int i = 1; i < anomalySets.size(); i++) {
      anomalySets.get(0).retainAll(anomalySets.get(i));
    }
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>(anomalySets.get(0));

    // sort descending by start time
    Collections.sort(anomalies, (o1, o2) -> -1 * Long.compare(o1.getStartTime(), o2.getStartTime()));

    if (fetchTrueAnomaly) {
      // Filter and retain only true anomalies
      List<MergedAnomalyResultDTO> trueAnomalies = new ArrayList<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        if (anomaly.getFeedback() != null && anomaly.getFeedback().getFeedbackType().isAnomaly()) {
          trueAnomalies.add(anomaly);
        }
      }
      anomalies = trueAnomalies;
    }
    // filter child anomalies
    anomalies = anomalies.stream().filter(anomaly -> !anomaly.isChild()).collect(Collectors.toList());
    // limit result size
    anomalies = anomalies.subList(0, Math.min(anomalies.size(), limit));

    return getAnomalyFormattedOutput(anomalies);
  }

  protected static class MetricDatasetPair {
    String datasetName;
    String metricName;

    MetricDatasetPair(String dataset, String metric) {
      this.datasetName = dataset;
      this.metricName = metric;
    }

    public static MetricDatasetPair fromString(String metricDatasetPair){
      String[] metricDataset = metricDatasetPair.trim().split("::");
      if (metricDataset.length != 2) {
        throw new RuntimeException("Unable to parse dataset::metric pair " + metricDatasetPair);
      }

      return new MetricDatasetPair(metricDataset[0], metricDataset[1]);
    }
  }

  /**
   * Returns a list of AnomalySummary for a set of query parameters. Anomalies are
   * sorted by start time (descending).
   *
   * <br/><b>Example:</b>
   * <pre>
   *   [ {
   *     "id" : 12345,
   *     "start" : 1517000000000,
   *     "end" : 1517100000000,
   *     "dimensions" : { "country": "us" },
   *     "severity" : 0.517,
   *     "current" : 1213.0,
   *     "baseline" : 550.0,
   *     "feedback" : "NO_FEEDBACK",
   *     "comment": "",
   *     "metricId" : 12346
   *     "metric" : "page_views",
   *     "metricUrn" : "thirdeye:metric:12345:country%3Dus"
   *     "functionId" : 12347
   *     "functionName" : "page_views_monitoring"
   *     },
   *     ...
   *   ]
   * </pre>
   *
   * @see AnomalySummary
   *
   * @param start window start time
   * @param end window end time (optional)
   * @param application anomaly function for application alert groups only (optional)
   *
   * @return List of AnomalySummary
   */
  @GET
  @Path("/anomalies")
  @ApiOperation(value = "Query anomalies")
  public Response fetchAnomalies(
      @ApiParam(value = "start time of anomaly retrieval window")
      @QueryParam("start") Long start,
      @ApiParam(value = "end time of anomaly retrieval window")
      @QueryParam("end") Long end,
      @ApiParam(value = "alert application/product/team")
      @QueryParam("application") String application,
      @ApiParam(value = "subscription group")
      @QueryParam("group") String group,
      @ApiParam(value = "The name of the metric to fetch anomalies from")
      @QueryParam("metric") String metric,
      @ApiParam(value = "The name of the pinot table to which this metric belongs")
      @QueryParam("dataset") String dataset,
      @ApiParam(value = "Specify multiple dataset::metric pairs")
      @QueryParam("metricAlias") List<MetricDatasetPair> metricDatasetPairs,
      @ApiParam(value = "Specify if you want to only fetch true anomalies")
      @QueryParam("fetchTrueAnomaly") @DefaultValue("false") boolean fetchTrueAnomaly,
      @ApiParam(value = "max number of results")
      @QueryParam("limit") Integer limit) {
    Map<String, String> responseMessage = new HashMap<>();
    List<AnomalySummary> output;
    try {
      output = queryAnomalies(start, end, application, group, metric, dataset, metricDatasetPairs, fetchTrueAnomaly, limit);
    } catch (Exception e) {
      LOG.warn("Error while fetching anomalies.", e.getMessage());
      responseMessage.put("message", "Failed to fetch all the anomalies.");
      responseMessage.put("more-info", "Error = " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Successfully returned " + output.size() + " anomalies.");
    return Response.ok(output).build();
  }

  private List<AnomalySummary> getAnomalyFormattedOutput(List<MergedAnomalyResultDTO> anomalies) {
    List<AnomalySummary> output = new ArrayList<>();

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      long metricId = this.getMetricId(anomaly);

      AnomalySummary summary = new AnomalySummary();
      summary.setId(anomaly.getId());
      summary.setStart(anomaly.getStartTime());
      summary.setEnd(anomaly.getEndTime());
      summary.setCurrent(anomaly.getAvgCurrentVal());
      summary.setBaseline(anomaly.getAvgBaselineVal());

      summary.setDetectionConfigId(-1);
      if (anomaly.getDetectionConfigId() != null) {
        long detectionConfigId = anomaly.getDetectionConfigId();
        DetectionConfigDTO detectionDTO = this.detectionDAO.findById(detectionConfigId);
        if (detectionDTO != null) {
          summary.setFunctionName(detectionDTO.getName());
          summary.setDetectionConfigId(detectionConfigId);
        } else {
          // this can happen when legacy anomalies are retrieved
          LOG.error("Cannot find detectionConfig with id {}, so skipping the anomaly {}...",
              detectionConfigId, anomaly.getId());
          continue;
        }
      }

      summary.setMetricName(anomaly.getMetric());
      summary.setDimensions(anomaly.getDimensions());
      summary.setDataset(anomaly.getCollection());
      summary.setMetricId(metricId);

      if (metricId > 0) {
        summary.setMetricUrn(anomaly.getMetricUrn());
      }

      // TODO use alert filter if necessary
      summary.setSeverity(Math.abs(anomaly.getWeight()));


      summary.setFeedback(AnomalyFeedbackType.NO_FEEDBACK);
      summary.setComment("");
      if (anomaly.getFeedback() != null) {
        summary.setFeedback(anomaly.getFeedback().getFeedbackType());
        summary.setComment(anomaly.getFeedback().getComment());
      }

      summary.setClassification(ResourceUtils.getStatusClassification(anomaly));
      summary.setSource(anomaly.getAnomalyResultSource());

      output.add(summary);
    }

    return output;
  }

  private Collection<MergedAnomalyResultDTO> fetchAnomaliesByMetricDatasetPairs(Long start, Long end, List<MetricDatasetPair> metricsRef) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();

    if (metricsRef == null) {
      return Collections.emptyList();
    }

    List<Predicate> predicates = new ArrayList<>();
    predicates.add(Predicate.GE("endTime", start));
    if (end != null) {
      predicates.add(Predicate.LT("startTime", end));
    }

    for (MetricDatasetPair metricDatasetPair : metricsRef) {
      List<Predicate> metricDatasetPred = new ArrayList<>(predicates);

      metricDatasetPred.add(Predicate.EQ("collection", metricDatasetPair.datasetName));
      metricDatasetPred.add(Predicate.EQ("metric", metricDatasetPair.metricName));
      anomalies.addAll(this.anomalyDAO.findByPredicate(Predicate.AND(metricDatasetPred.toArray(new Predicate[0]))));
    }

    return anomalies;
  }

  private Collection<MergedAnomalyResultDTO> fetchAnomaliesByMetricDataset(Long start, Long end, String metric, String dataset) {
    if (StringUtils.isBlank(metric) && StringUtils.isBlank(dataset)) {
      return Collections.emptyList();
    }

    List<Predicate> predicates = new ArrayList<>();
    predicates.add(Predicate.GE("endTime", start));
    if (end != null) {
      predicates.add(Predicate.LT("startTime", end));
    }

    // Filter by metric and dataset
    if (metric != null) {
      predicates.add(Predicate.EQ("metric", metric));
    }
    if (dataset != null) {
      predicates.add(Predicate.EQ("collection", dataset));
    }

    return this.anomalyDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[predicates.size()])));
  }

  private Collection<MergedAnomalyResultDTO> fetchAnomaliesByApplication(Long start, Long end, String application) {
    if (StringUtils.isBlank(application)) {
      return Collections.emptyList();
    }

    List<DetectionAlertConfigDTO> alerts =
        this.detectionAlertDAO.findByPredicate(Predicate.EQ("application", application));

    Set<Long> detectionConfigIds = new HashSet<>();
    for (DetectionAlertConfigDTO alertConfigDTO : alerts) {
      detectionConfigIds.addAll(alertConfigDTO.getVectorClocks().keySet());
    }

    return fetchAnomaliesByConfigIds(start, end, detectionConfigIds);
  }

  private Collection<MergedAnomalyResultDTO> fetchAnomaliesBySubsGroup(Long start, Long end, String group) {
    if (StringUtils.isBlank(group)) {
      return Collections.emptyList();
    }

    List<DetectionAlertConfigDTO> alerts =
        this.detectionAlertDAO.findByPredicate(Predicate.EQ("name", group));

    Set<Long> detectionConfigIds = new HashSet<>();
    for (DetectionAlertConfigDTO alertConfigDTO : alerts) {
      detectionConfigIds.addAll(alertConfigDTO.getVectorClocks().keySet());
    }

    return fetchAnomaliesByConfigIds(start, end, detectionConfigIds);
  }

  private Collection<MergedAnomalyResultDTO> fetchAnomaliesByConfigIds(Long start, Long end, Set<Long> detectionConfigIds) {
    if (detectionConfigIds.isEmpty()) {
      return Collections.emptyList();
    }

    List<Predicate> predicates = new ArrayList<>();

    // anomaly window start
    if (start != null) {
      predicates.add(Predicate.GT("endTime", start));
    }

    // anomaly window end
    if (end != null) {
      predicates.add(Predicate.LT("startTime", end));
    }

    predicates.add(Predicate.IN("detectionConfigId", detectionConfigIds.toArray()));

    Collection<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[predicates.size()])));

    anomalies = Collections2.filter(anomalies, new com.google.common.base.Predicate<MergedAnomalyResultDTO>() {
      @Override
      public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return !mergedAnomalyResultDTO.isChild();
      }
    });

    return anomalies;
  }

  /**
   * Helper to work around for anomaly function not setting metric id
   *
   * @param anomaly anomaly dto
   * @return metric id, or {@code -1} if the metric/dataset cannot be resolved
   */
  private long getMetricId(MergedAnomalyResultDTO anomaly) {
    if (anomaly.getFunction() != null && anomaly.getFunction().getMetricId() > 0) {
      return anomaly.getFunction().getMetricId();
    }
    try {
      return this.metricDAO.findByMetricAndDataset(anomaly.getMetric(), anomaly.getCollection()).getId();
    } catch (Exception e) {
      return -1;
    }
  }
}
