/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomalySummary;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detection.CurrentAndBaselineLoader;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;


/**
 * Endpoints for user-customized dashboards (currently alerts only)
 */
@Api(tags = {Constants.DASHBOARD_TAG})
@Path(value = "/userdashboard")
@Produces(MediaType.APPLICATION_JSON)
public class UserDashboardResource {
  private static final int ANOMALIES_LIMIT_DEFAULT = 500;

  private final MergedAnomalyResultManager anomalyDAO;
  private final AnomalyFunctionManager functionDAO;
  private final AlertConfigManager alertDAO;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final DetectionConfigManager detectionDAO;
  private final DetectionAlertConfigManager detectionAlertDAO;
  private final AggregationLoader aggregationLoader;
  private final CurrentAndBaselineLoader currentAndBaselineLoader;


  public UserDashboardResource(MergedAnomalyResultManager anomalyDAO, AnomalyFunctionManager functionDAO,
      MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, AlertConfigManager alertDAO,
      DetectionConfigManager detectionDAO, DetectionAlertConfigManager detectionAlertDAO) {
    this.anomalyDAO = anomalyDAO;
    this.functionDAO = functionDAO;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.alertDAO = alertDAO;
    this.detectionDAO = detectionDAO;
    this.detectionAlertDAO = detectionAlertDAO;

    this.aggregationLoader =
        new DefaultAggregationLoader(this.metricDAO, this.datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
    this.currentAndBaselineLoader = new CurrentAndBaselineLoader(this.metricDAO, this.datasetDAO, this.aggregationLoader);
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
   * @param owner anomaly function owner only (optional)
   * @param application anomaly function for application alert groups only (optional)
   *
   * @return List of AnomalySummary
   */
  @GET
  @Path("/anomalies")
  @ApiOperation(value = "Query anomalies")
  public List<AnomalySummary> queryAnomalies(
      @ApiParam(value = "start time of anomaly retrieval window")
      @QueryParam("start") Long start,
      @ApiParam(value = "end time of anomaly retrieval window")
      @QueryParam("end") Long end,
      @ApiParam(value = "alert owner")
      @QueryParam("owner") String owner,
      @ApiParam(value = "alert application/product/team")
      @QueryParam("application") String application,
      @ApiParam(value = "subscription group")
      @QueryParam("group") String group,
      @ApiParam(value = "max number of results")
      @QueryParam("limit") Integer limit) throws Exception {

    //
    // query params
    //
    if (limit == null) {
      limit = ANOMALIES_LIMIT_DEFAULT;
    }

    // safety condition
    Preconditions.checkNotNull(start);

    // TODO support index select on user-reported anomalies
//    predicates.add(Predicate.OR(
//        Predicate.EQ("notified", true),
//        Predicate.EQ("anomalyResultSource", AnomalyResultSource.USER_LABELED_ANOMALY)));

    // application (indirect)
    Set<Long> applicationFunctionIds = new HashSet<>();
    if (StringUtils.isNotBlank(application)) {
      List<AnomalyFunctionDTO> functions = this.functionDAO.findAllByApplication(application);
      for (AnomalyFunctionDTO function : functions) {
        if (function.getIsActive()) {
          applicationFunctionIds.add(function.getId());
        }
      }
    }

    // group (indirect)
    Set<Long> groupFunctionIds = new HashSet<>();
    if (StringUtils.isNotBlank(group)) {
      AlertConfigDTO alert = this.alertDAO.findWhereNameEquals(group);
      if (alert != null) {
        groupFunctionIds.addAll(alert.getEmailConfig().getFunctionIds());
      }
    }

    // owner (indirect)
    Set<Long> ownerFunctionIds = new HashSet<>();
    if (StringUtils.isNotBlank(owner)) {
      // TODO: replace database scan with targeted select
      List<AnomalyFunctionDTO> functions = this.functionDAO.findAll();
      for (AnomalyFunctionDTO function : functions) {
        if (Objects.equals(function.getCreatedBy(), owner)) {
          ownerFunctionIds.add(function.getId());
        }
      }
    }

    // anomaly function ids
    List<Predicate> predicates = new ArrayList<>();

    if (StringUtils.isNotBlank(application) || StringUtils.isNotBlank(group) || StringUtils.isNotBlank(owner)) {
      Set<Long> functionIds = new HashSet<>();
      functionIds.addAll(applicationFunctionIds);
      functionIds.addAll(groupFunctionIds);
      functionIds.addAll(ownerFunctionIds);

      predicates.add(Predicate.IN("functionId", functionIds.toArray()));
    }

    // anomaly window start
    if (start != null) {
      predicates.add(Predicate.GT("endTime", start));
    }

    // anomaly window end
    if (end != null) {
      predicates.add(Predicate.LT("startTime", end));
    }

    //
    // fetch anomalies
    //

    // fetch legacy anomalies via predicates
    List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[predicates.size()])));

    // filter (un-notified && non-user-reported) anomalies
    // TODO remove once index select on user-reported anomalies available
    Iterator<MergedAnomalyResultDTO> itAnomaly = anomalies.iterator();
    while (itAnomaly.hasNext()) {
      MergedAnomalyResultDTO anomaly = itAnomaly.next();
      if (!anomaly.isNotified() &&
          !AnomalyResultSource.USER_LABELED_ANOMALY.equals(anomaly.getAnomalyResultSource())) {
        itAnomaly.remove();
      }
    }

    // fetch new detection framework anomalies by group
    if (StringUtils.isNotBlank(group)) {
      anomalies.addAll(fetchFrameworkAnomaliesByGroup(start, end, group));
    }

    // fetch new detection framework anomalies by application
    if (StringUtils.isNotBlank(application)) {
      anomalies.addAll(fetchFrameworkAnomaliesByApplication(start, end, application));
    }

    // sort descending by start time
    Collections.sort(anomalies, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return -1 * Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    // limit result size
    anomalies = anomalies.subList(0, Math.min(anomalies.size(), limit));

    //
    // fetch functions
    //
    Set<Long> anomalyFunctionIds = new HashSet<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getFunctionId() != null) {
        anomalyFunctionIds.add(anomaly.getFunctionId());
      }
    }

    List<AnomalyFunctionDTO> functions = this.functionDAO.findByPredicate(Predicate.IN("baseId", anomalyFunctionIds.toArray()));
    Map<Long, AnomalyFunctionDTO> id2function = new HashMap<>();
    for (AnomalyFunctionDTO function : functions) {
      id2function.put(function.getId(), function);
    }

    //
    // format output
    //
    List<AnomalySummary> output = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      long metricId = this.getMetricId(anomaly);

      AnomalySummary summary = new AnomalySummary();
      summary.setId(anomaly.getId());
      summary.setStart(anomaly.getStartTime());
      summary.setEnd(anomaly.getEndTime());
      summary.setCurrent(anomaly.getAvgCurrentVal());
      summary.setBaseline(anomaly.getAvgBaselineVal());

      summary.setFunctionId(-1);
      if (anomaly.getFunctionId() != null) {
        summary.setFunctionId(anomaly.getFunctionId());
        if (id2function.get(anomaly.getFunctionId()) != null) {
          summary.setFunctionName(id2function.get(anomaly.getFunctionId()).getFunctionName());
        }
      }

      summary.setDetectionConfigId(-1);
      if (anomaly.getDetectionConfigId() != null) {
        long detectionConfigId = anomaly.getDetectionConfigId();
        DetectionConfigDTO detectionDTO = this.detectionDAO.findById(detectionConfigId);
        summary.setFunctionName(detectionDTO.getName());
        summary.setDetectionConfigId(detectionConfigId);
      }

      summary.setMetricName(anomaly.getMetric());
      summary.setDimensions(anomaly.getDimensions());
      summary.setDataset(anomaly.getCollection());
      summary.setMetricId(metricId);

      if (metricId > 0) {
        summary.setMetricUrn(this.getMetricUrn(anomaly));
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

  private Collection<MergedAnomalyResultDTO> fetchFrameworkAnomaliesByApplication(Long start, Long end, String application) throws Exception {
    List<DetectionAlertConfigDTO> alerts =
        this.detectionAlertDAO.findByPredicate(Predicate.EQ("application", application));

    Set<Long> detectionConfigIds = new HashSet<>();
    for (DetectionAlertConfigDTO alertConfigDTO : alerts) {
      detectionConfigIds.addAll(alertConfigDTO.getVectorClocks().keySet());
    }

    return fetchFrameworkAnomaliesByConfigIds(start, end, detectionConfigIds);
  }

  private Collection<MergedAnomalyResultDTO> fetchFrameworkAnomaliesByGroup(Long start, Long end, String group) throws Exception {
    List<DetectionAlertConfigDTO> alerts =
        this.detectionAlertDAO.findByPredicate(Predicate.EQ("name", group));

    Set<Long> detectionConfigIds = new HashSet<>();
    for (DetectionAlertConfigDTO alertConfigDTO : alerts) {
      detectionConfigIds.addAll(alertConfigDTO.getVectorClocks().keySet());
    }

    return fetchFrameworkAnomaliesByConfigIds(start, end, detectionConfigIds);
  }

  private Collection<MergedAnomalyResultDTO> fetchFrameworkAnomaliesByConfigIds(Long start, Long end, Set<Long> detectionConfigIds) throws Exception {
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

    this.currentAndBaselineLoader.fillInCurrentAndBaselineValue(anomalies);

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

  /**
   * Returns an URN matching the anomalies associated metric (and dimensions)
   *
   * @param anomaly anomaly dto
   * @return metric urn
   */
  private String getMetricUrn(MergedAnomalyResultDTO anomaly) {
    return MetricEntity.fromMetric(1.0, this.getMetricId(anomaly), ResourceUtils.getAnomalyFilters(anomaly, this.datasetDAO)).getUrn();
  }
}
