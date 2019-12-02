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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import org.apache.pinot.thirdeye.anomalydetection.alertFilterAutotune.BaseAlertFilterAutoTune;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.detector.email.filter.BaseAlertFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.pinot.thirdeye.anomaly.detection.DetectionJobScheduler;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pinot.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import org.apache.pinot.thirdeye.anomaly.job.JobConstants.JobStatus;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.AutotuneConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AutotuneConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilter;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.email.filter.PrecisionRecallEvaluator;

import static org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO.*;


@Path("/detection-job")
@Api(tags = {Constants.DASHBOARD_TAG} )
@Produces(MediaType.APPLICATION_JSON)
@Deprecated
public class DetectionJobResource {
  private final DetectionJobScheduler detectionJobScheduler;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final AlertConfigManager alertConfigDAO;
  private final AutotuneConfigManager autotuneConfigDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private final AlertFilterAutotuneFactory alertFilterAutotuneFactory;
  private final AlertFilterFactory alertFilterFactory;
  private EmailResource emailResource;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobResource.class);
  public static final String AUTOTUNE_FEATURE_KEY = "features";
  public static final String AUTOTUNE_PATTERN_KEY = "pattern";
  public static final String AUTOTUNE_MTTD_KEY = "mttd";
  public static final String AUTOTUNE_PATTERN_ONLY = "tunePatternOnly";
  private static final String SIGN_TEST_WINDOW_SIZE = "signTestWindowSize";

  /**
   * TODO: Remove the constructor
   */
  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler, AlertFilterFactory alertFilterFactory,
      AlertFilterAutotuneFactory alertFilterAutotuneFactory, EmailResource emailResource) {
    this(detectionJobScheduler, alertFilterFactory, alertFilterAutotuneFactory);
    this.emailResource = emailResource;
  }

  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler, AlertFilterFactory alertFilterFactory,
      AlertFilterAutotuneFactory alertFilterAutotuneFactory) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.autotuneConfigDAO = DAO_REGISTRY.getAutotuneConfigDAO();
    this.alertConfigDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.alertFilterAutotuneFactory = alertFilterAutotuneFactory;
    this.alertFilterFactory = alertFilterFactory;
  }

  private void toggleRequiresCompletenessCheck(Long id, boolean state) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    Preconditions.checkNotNull(anomalyFunctionSpec, "Function spec not found");
    anomalyFunctionSpec.setRequiresCompletenessCheck(state);
    anomalyFunctionDAO.update(anomalyFunctionSpec);
  }

  @POST
  @Path("/{id}/ad-hoc")
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso) throws Exception {
    Long startTime = null;
    Long endTime = null;
    if (StringUtils.isBlank(startTimeIso) || StringUtils.isBlank(endTimeIso)) {
      throw new IllegalStateException("startTimeIso and endTimeIso must not be null");
    }
    startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    detectionJobScheduler.runAdhocAnomalyFunction(id, startTime, endTime);
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/offlineAnalysis")
  public Response generateAnomaliesInTrainingData(@PathParam("id") @NotNull long id,
      @QueryParam("time") String analysisTimeIso) throws Exception {

    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    if (anomalyFunction == null) {
      return Response.noContent().build();
    }

    DateTime analysisTime = DateTime.now();
    if (StringUtils.isNotEmpty(analysisTimeIso)) {
      analysisTime = ISODateTimeFormat.dateTimeParser().parseDateTime(analysisTimeIso);
    }

    Long jobId = detectionJobScheduler.runOfflineAnalysis(id, analysisTime);
    List<Long> anomalyIds = new ArrayList<>();
    if (jobId == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Anomaly function " + Long.toString(id) + " is inactive. Or thread is interrupted.")
          .build();
    } else {
      JobStatus jobStatus = detectionJobScheduler.waitForJobDone(jobId);
      if (jobStatus.equals(JobStatus.FAILED)) {
        return Response.status(Response.Status.NO_CONTENT).entity("Detection job failed").build();
      } else {
        List<MergedAnomalyResultDTO> mergedAnomalies =
            mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(0, analysisTime.getMillis(), id);
        for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
          anomalyIds.add(mergedAnomaly.getId());
        }
      }
    }

    return Response.ok(anomalyIds).build();
  }

  /**
   * Given a list of holiday starts and holiday ends, return merged anomalies with holidays removed
   * @param functionId: the functionId to fetch merged anomalies with holidays removed
   * @param startTime: start time in milliseconds of merged anomalies
   * @param endTime: end time of in milliseconds merged anomalies
   * @param holidayStarts: holidayStarts in ISO Format ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds: holidayEnds in in ISO Format ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return a list of merged anomalies with holidays removed
   */
  public static List<MergedAnomalyResultDTO> getMergedAnomaliesRemoveHolidays(long functionId, long startTime,
      long endTime, String holidayStarts, String holidayEnds) {
    StringTokenizer starts = new StringTokenizer(holidayStarts, ",");
    StringTokenizer ends = new StringTokenizer(holidayEnds, ",");
    MergedAnomalyResultManager anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    List<MergedAnomalyResultDTO> totalAnomalies =
        anomalyMergedResultDAO.findByStartTimeInRangeAndFunctionId(startTime, endTime, functionId);
    int origSize = totalAnomalies.size();
    long start;
    long end;
    while (starts.hasMoreElements() && ends.hasMoreElements()) {
      start = ISODateTimeFormat.dateTimeParser().parseDateTime(starts.nextToken()).getMillis();
      end = ISODateTimeFormat.dateTimeParser().parseDateTime(ends.nextToken()).getMillis();
      List<MergedAnomalyResultDTO> holidayMergedAnomalies =
          anomalyMergedResultDAO.findByStartTimeInRangeAndFunctionId(start, end, functionId);
      totalAnomalies.removeAll(holidayMergedAnomalies);
    }
    if (starts.hasMoreElements() || ends.hasMoreElements()) {
      LOG.warn("Input holiday starts and ends length not equal!");
    }
    LOG.info("Removed {} merged anomalies", origSize - totalAnomalies.size());
    return totalAnomalies;
  }

  /**
   *
   * @param ids a list of anomaly function ids, separate by comma, id1,id2,id3
   * @param startTimeIso start time of anomalies to tune alert filter in ISO format ex: 2016-5-23T00:00:00Z
   * @param endTimeIso end time of anomalies to tune alert filter in ISO format ex: 2016-5-23T00:00:00Z
   * @param autoTuneType the type of auto tune to invoke (default is "AUTOTUNE")
   * @param holidayStarts: holidayStarts in ISO Format, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds: holidayEnds in in ISO Format, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param features: a list of features separated by comma, ex: weight,score.
   *                Note that, the feature must be a existing field name in MergedAnomalyResult or a pre-set feature name
   * @param pattern: users' customization on pattern. Format can be one of them: "UP", "DOWN" or "UP,DOWN"
   * @param mttd: mttd string to specify severity on minimum-time-to-detection. Format example: "window_size_in_hour=2.5;weight=0.3"
   * @return HTTP response of request: a list of autotune config id
   */
  @POST
  @Path("/autotune/filter/{functionIds}")
  public Response tuneAlertFilter(@PathParam("functionIds") String ids, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso, @QueryParam("autoTuneType") @DefaultValue("AUTOTUNE") String autoTuneType,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds, @QueryParam("tuningFeatures") String features,
      @QueryParam("mttd") String mttd, @QueryParam("pattern") String pattern,
      @QueryParam("tunePatternOnly") String tunePatternOnly) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    // get anomalies by function id, start time and end time
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    List<Long> anomalyFunctionIds = new ArrayList<>();
    for (String idString : ids.split(",")) {
      long id = Long.valueOf(idString);
      AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
      if (anomalyFunctionSpec == null) {
        LOG.warn("Anomaly detection function id {} doesn't exist", id);
        continue;
      }
      anomalyFunctionIds.add(id);
      anomalies.addAll(getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds));
    }
    if (anomalyFunctionIds.isEmpty()) {
      return Response.status(Status.BAD_REQUEST).entity("No valid function ids").build();
    }

    // create alert filter auto tune
    AutotuneConfigDTO autotuneConfig = new AutotuneConfigDTO();
    if (anomalyFunctionIds.size() >= 1) {
      AnomalyFunctionDTO functionSpec =
          DAO_REGISTRY.getAnomalyFunctionDAO().findById(Long.valueOf(anomalyFunctionIds.get(0)));
      BaseAlertFilter currentAlertFilter = alertFilterFactory.fromSpec(functionSpec.getAlertFilter());
      autotuneConfig.initAlertFilter(currentAlertFilter);
    }

    Properties autotuneProperties = autotuneConfig.getTuningProps();

    // if new feature set being specified
    if (StringUtils.isNotBlank(features)) {
      String previousFeatures = autotuneProperties.getProperty(AUTOTUNE_FEATURE_KEY);
      LOG.info("The previous features for autotune is {}; now change to {}", previousFeatures, features);
      autotuneProperties.setProperty(AUTOTUNE_FEATURE_KEY, features);
      autotuneConfig.setTuningProps(autotuneProperties);
    }

    // if new pattern being specified
    if (StringUtils.isNotBlank(pattern)) {
      String previousPattern = autotuneProperties.getProperty(AUTOTUNE_PATTERN_KEY);
      LOG.info("The previous pattern for autotune is {}; now changed to {}", previousPattern, pattern);
      autotuneProperties.setProperty(AUTOTUNE_PATTERN_KEY, pattern);
      autotuneConfig.setTuningProps(autotuneProperties);
    }

    // if new mttd requirement specified
    if (StringUtils.isNotBlank(mttd)) {
      String previousMttd = autotuneProperties.getProperty(AUTOTUNE_MTTD_KEY);
      LOG.info("The previous mttd for autotune is {}; now changed to {}", previousMttd, mttd);
      autotuneProperties.setProperty(AUTOTUNE_MTTD_KEY, mttd);
      autotuneConfig.setTuningProps(autotuneProperties);
    }

    // if specified "tunePatternOnly"
    if (StringUtils.isNotBlank(tunePatternOnly)) {
      String previousTunePatternOnly = autotuneProperties.getProperty(AUTOTUNE_PATTERN_ONLY);
      LOG.info("The previous tunePatternOnly for autotune is {}; now changed to {}", previousTunePatternOnly, tunePatternOnly);
      autotuneProperties.setProperty(AUTOTUNE_PATTERN_ONLY, tunePatternOnly);
      autotuneConfig.setTuningProps(autotuneProperties);
    }

    BaseAlertFilterAutoTune alertFilterAutotune =
        alertFilterAutotuneFactory.fromSpec(autoTuneType, autotuneConfig, anomalies);
    LOG.info("initiated alertFilterAutoTune of Type {}", alertFilterAutotune.getClass().toString());

    // tune
    try {
      Map<String, String> tunedAlertFilter = alertFilterAutotune.tuneAlertFilter();
      LOG.info("Tuned alert filter with configurations: {}", tunedAlertFilter);
    } catch (Exception e) {
      LOG.warn("Exception when tuning alert filter: {}", e.getMessage());
    }

    // write to DB
    List<Long> autotuneIds = new ArrayList<>();
    for (long functionId : anomalyFunctionIds) {
      autotuneConfig.setId(null);
      autotuneConfig.setFunctionId(functionId);
      long autotuneId = DAO_REGISTRY.getAutotuneConfigDAO().save(autotuneConfig);
      autotuneIds.add(autotuneId);
    }

    try {
      String autotuneIdsJson = OBJECT_MAPPER.writeValueAsString(autotuneIds);
      return Response.ok(autotuneIdsJson).build();
    } catch (JsonProcessingException e) {
      LOG.error("Failed to covert autotune ID list to Json String. Property: {}.", autotuneIds.toString(), e);
      return Response.serverError().build();
    }
  }

  /**
   * Endpoint to check if merged anomalies given a time period have at least one positive label
   * @param id functionId to test anomalies
   * @param startTimeIso start time to check anomaly history labels in ISO format ex: 2016-5-23T00:00:00Z
   * @param endTimeIso end time to check anomaly history labels in ISO format ex: 2016-5-23T00:00:00Z
   * @param holidayStarts optional: holidayStarts in ISO format as string, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds optional:holidayEnds in ISO format as string, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return true if the list of merged anomalies has at least one positive label, false otherwise
   */
  @POST
  @Path("/initautotune/checkhaslabel/{functionId}")
  public Response checkAnomaliesHasLabel(@PathParam("functionId") long id, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso, @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {
    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);
    return Response.ok(AnomalyUtils.checkHasLabels(anomalyResultDTOS)).build();
  }

  /**
   * End point to trigger initiate alert filter auto tune
   * @param id functionId to initiate alert filter auto tune
   * @param startTimeIso: training data starts time ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: training data ends time ex: 2016-5-23T00:00:00Z
   * @param autoTuneType: By default is "AUTOTUNE"
   * @param holidayStarts optional: holidayStarts in ISO format: start1,start2,...
   * @param holidayEnds optional:holidayEnds in ISO format: end1,end2,...
   * @return true if alert filter has successfully being initiated, false otherwise
   */
  @POST
  @Path("/initautotune/filter/{functionId}")
  public Response initiateAlertFilterAutoTune(@PathParam("functionId") long id,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso,
      @QueryParam("autoTuneType") @DefaultValue("AUTOTUNE") String autoTuneType,
      @QueryParam("userDefinedPattern") @DefaultValue("UP,DOWN") String userDefinedPattern,
      @QueryParam("Sensitivity") @DefaultValue("MEDIUM") String sensitivity,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    // get anomalies by function id, start time and end time
    List<MergedAnomalyResultDTO> anomalies =
        getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);

    //initiate AutoTuneConfigDTO
    Properties tuningProperties = new Properties();
    tuningProperties.put("pattern", userDefinedPattern);
    tuningProperties.put("sensitivity", sensitivity);
    AutotuneConfigDTO autotuneConfig = new AutotuneConfigDTO(tuningProperties);

    // create alert filter auto tune
    BaseAlertFilterAutoTune alertFilterAutotune =
        alertFilterAutotuneFactory.fromSpec(autoTuneType, autotuneConfig, anomalies);
    LOG.info("initiated alertFilterAutoTune of Type {}", alertFilterAutotune.getClass().toString());

    String autotuneId = null;
    // tune
    try {
      Map<String, String> tunedAlertFilterConfig = alertFilterAutotune.tuneAlertFilter();
      LOG.info("Get tunedAlertFilter: {}", tunedAlertFilterConfig);
    } catch (Exception e) {
      LOG.warn("Exception when tune alert filter, {}", e.getMessage());
    }
    // write to DB
    autotuneConfig.setFunctionId(id);
    autotuneConfig.setAutotuneMethod(AutotuneMethodType.INITIATE_ALERT_FILTER_LOGISTIC_AUTO_TUNE);
    autotuneId = DAO_REGISTRY.getAutotuneConfigDAO().save(autotuneConfig).toString();

    return Response.ok(autotuneId).build();
  }

  /**
   * Evaluate the performance based on the given list of anomaly results and the alert filter spec
   * The projected performance is based on if the anomaly qualified based on the current/given alert filter; if false,
   * it uses the isNotified flag directly.
   * @param alertFilterSpec the parameters of alert filter
   * @param anomalyResults the list of anomaly results
   * @param isProjected Boolean to indicate is to return projected performance for current alert filter.
   *                   If "true", return projected performance for current alert filter
   * @return feedback summary, precision and recall as Number map
   */
  public Map<String, Number> performanceEvaluation(Map<String, String> alertFilterSpec,
      List<MergedAnomalyResultDTO> anomalyResults, boolean isProjected) {
    PrecisionRecallEvaluator evaluator;
    if (Boolean.valueOf(isProjected)) {
      // create alert filter and evaluator
      AlertFilter alertFilter = alertFilterFactory.fromSpec(alertFilterSpec);
      //evaluate current alert filter (calculate current precision and recall)
      evaluator = new PrecisionRecallEvaluator(alertFilter, anomalyResults);

      LOG.info("AlertFilter of Type {}, has been evaluated with precision: {}, recall:{}",
          alertFilter.getClass().getSimpleName(), evaluator.getWeightedPrecision(), evaluator.getRecall());
    } else {
      evaluator = new PrecisionRecallEvaluator(anomalyResults);
    }

    return evaluator.toNumberMap();
  }

  /**
   * Evaluate the performance based on the given list of anomaly results and the alert filter spec
   * The projected performance is based on if the anomaly qualified based on the current/given alert filter; if false,
   * it uses the isNotified flag directly.
   * @param alertFilterSpec the parameters of alert filter
   * @param anomalyFunctions the list of anomaly functions
   * @param startTime the start time of the evaluation
   * @param endTime  the end time of the evaluation
   * @param isProjected Boolean to indicate is to return projected performance for current alert filter.
   *                   If "true", return projected performance for current alert filter
   * @return feedback summary, precision and recall as Number map
   */
  public Map<String, Number> performanceEvaluation(Map<String, String> alertFilterSpec,
      List<AnomalyFunctionDTO> anomalyFunctions, DateTime startTime, DateTime endTime, boolean isProjected) {
    if (anomalyFunctions == null || anomalyFunctions.size() == 0) {
      return Collections.emptyMap();
    }
    Set<MergedAnomalyResultDTO> mergedAnomalyResults = new HashSet<>();
    for (AnomalyFunctionDTO function : anomalyFunctions) {
      mergedAnomalyResults.addAll(mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime.getMillis(),
          endTime.getMillis(), function.getId()));
    }
    if (alertFilterSpec != null || !isProjected) {
      return performanceEvaluation(alertFilterSpec, new ArrayList<>(mergedAnomalyResults), isProjected);
    } else {
      PrecisionRecallEvaluator evaluator =
          new PrecisionRecallEvaluator(new ArrayList<>(mergedAnomalyResults), alertFilterFactory);
      return evaluator.toNumberMap();
    }
  }

  /**
   * The endpoint to evaluate system performance. The evaluation will be based on sent and non sent anomalies
   * The projected performance is based on if the anomaly qualified based on the current/given alert filter; if false,
   * it uses the isNotified flag directly.
   * @param id: function ID
   * @param startTimeIso: startTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: endTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param isProjected: Boolean to indicate is to return projected performance for current alert filter.
   *                   If "true", return projected performance for current alert filter
   * @return feedback summary, precision and recall as json object
   * @throws Exception when data has no positive label or model has no positive prediction
   */
  @GET
  @Path("/eval/filter/{functionId}")
  public Response evaluateAlertFilterByFunctionId(@PathParam("functionId") long id,
      @QueryParam("start") @NotNull String startTimeIso, @QueryParam("end") @NotNull String endTimeIso,
      @QueryParam("isProjected") @DefaultValue("false") boolean isProjected,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    // get anomalies by function id, start time and end time`
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);

    Map<String, Number> evaluatorValues = performanceEvaluation(anomalyFunctionSpec.getAlertFilter(), anomalyResultDTOS, isProjected);
    return numberMapToResponse(evaluatorValues);
  }

  /**
   * The endpoint to evaluate system performance. The evaluation will be based on sent and non sent anomalies
   * The projected performance is based on if the anomaly qualified based on the current/given alert filter; if false,
   * it uses the isNotified flag directly.
   * @param id: alert config ID
   * @param startTimeIso: startTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: endTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param isProjected: Boolean to indicate is to return projected performance for current alert filter.
   *                   If "true", return projected performance for current alert filter
   * @return feedback summary, precision and recall as json object
   * @throws Exception when data has no positive label or model has no positive prediction
   */
  @GET
  @Path("/eval/alert/{alertConfigId}")
  public Response evaluateAlertFilterByAlertConfigId(@PathParam("alertConfigId") @NotNull long id,
      @QueryParam("start") @NotNull String startTimeIso, @QueryParam("end") @NotNull String endTimeIso,
      @QueryParam("isProjected") @DefaultValue("false") boolean isProjected,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {
    AlertConfigDTO alertConfigDTO = alertConfigDAO.findById(id);

    if (alertConfigDTO == null) {
      return Response.status(Status.BAD_REQUEST).entity("Cannot find alert config id " + id + " in db").build();
    }
    if (alertConfigDTO.getEmailConfig() == null) {
      return Response.status(Status.NO_CONTENT).entity(Collections.emptyMap()).build();
    }

    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);

    // get the list of anomaly functions under the alert config
    List<Long> anomalyFunctionIds = alertConfigDTO.getEmailConfig().getFunctionIds();
    List<AnomalyFunctionDTO> anomalyFunctions = new ArrayList<>();
    for (Long functionId : anomalyFunctionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      if (anomalyFunction != null) {
        anomalyFunctions.add(anomalyFunction);
      }
    }
    Map<String, Number> evaluatorValues = performanceEvaluation(null, anomalyFunctions, startTime, endTime, true);
    return numberMapToResponse(evaluatorValues);
  }

  /**
   * The endpoint to evaluate system performance. The evaluation will be based on sent and non sent anomalies
   * The projected performance is based on if the anomaly qualified based on the current/given alert filter; if false,
   * it uses the isNotified flag directly.
   * @param name: application name
   * @param startTimeIso: startTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: endTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param isProjected: Boolean to indicate is to return projected performance for current alert filter.
   *                   If "true", return projected performance for current alert filter
   * @return feedback summary, precision and recall as json object
   * @throws Exception when data has no positive label or model has no positive prediction
   */
  @GET
  @Path("/eval/application/{applicationName}")
  public Response evaluateAlertFilterByApplicationName(@PathParam("applicationName") @NotNull String name,
      @QueryParam("start") @NotNull String startTimeIso, @QueryParam("end") @NotNull String endTimeIso,
      @QueryParam("isProjected") @DefaultValue("false") boolean isProjected,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {
    List<ApplicationDTO> applications = DAO_REGISTRY.getApplicationDAO().findByName(name);

    if (applications == null || applications.size() == 0) {
      return Response.status(Status.BAD_REQUEST).entity("Cannot find application name " + name + " in db").build();
    }
    if (applications.size() != 1) {
      return Response.status(Status.BAD_REQUEST).entity("More than one applications with the similar name, " + name).build();
    }

    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);

    // get the list of anomaly functions under the alert config
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByApplication(name);
    Map<String, Number> evaluatorValues = performanceEvaluation(null, anomalyFunctions, startTime, endTime, true);
    return numberMapToResponse(evaluatorValues);
  }

  /**
   * To evaluate alert filte directly by autotune Id using autotune_config_index table
   * This is to leverage the intermediate step before updating tuned alert filter configurations
   * @param id: autotune Id
   * @param startTimeIso: merged anomalies start time. ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: merged anomalies end time  ex: 2016-5-23T00:00:00Z
   * @param holidayStarts: holiday starts time to remove merged anomalies in ISO format. ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds: holiday ends time to remove merged anomlaies in ISO format. ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return HTTP response of evaluation results
   */
  @GET
  @Path("/eval/autotune/{autotuneId}")
  public Response evaluateAlertFilterByAutoTuneId(@PathParam("autotuneId") long id,
      @QueryParam("start") @NotNull String startTimeIso, @QueryParam("end") @NotNull String endTimeIso,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(functionId, startTime, endTime, holidayStarts, holidayEnds);

    Map<String, Number> evaluatorValues = performanceEvaluation(target.getConfiguration(), anomalyResultDTOS, true);
    return numberMapToResponse(evaluatorValues);
  }

  /**
   * Generate the response from the evaluator output
   * @param evaluatorValues the output of percision recall evaluator
   * @return a HTTP response
   */
  private Response numberMapToResponse(Map<String, Number> evaluatorValues) {
    try {
      String propertiesJson = OBJECT_MAPPER.writeValueAsString(evaluatorValues);
      return Response.ok(propertiesJson).build();
    } catch (JsonProcessingException e) {
      LOG.error("Failed to covert evaluator values to a Json String. Property: {}.", evaluatorValues.toString(), e);
      return Response.serverError().build();
    }

  }

  /**
   * Parse the jsonobject and list all the possible configuration combinations
   * @param tuningJSON the input json string from user
   * @return full list of all the possible configurations
   * @throws JSONException
   */
  private List<Map<String, String>> listAllTuningParameters(JSONObject tuningJSON) throws JSONException {
    List<Map<String, String>> tuningParameters = new ArrayList<>();
    Iterator<String> jsonFieldIterator = tuningJSON.keys();
    Map<String, List<String>> fieldToParams = new HashMap<>();

    int numPermutations = 1;
    while (jsonFieldIterator.hasNext()) {
      String field = jsonFieldIterator.next();

      if (field != null && !field.isEmpty()) {
        // JsonArray to String List
        List<String> params = new ArrayList<>();
        JSONArray paramArray = tuningJSON.getJSONArray(field);
        if (paramArray.length() == 0) {
          continue;
        }
        for (int i = 0; i < paramArray.length(); i++) {
          params.add(paramArray.get(i).toString());
        }
        numPermutations *= params.size();
        fieldToParams.put(field, params);
      }
    }

    if (fieldToParams.size() == 0) { // No possible tuning parameters
      return tuningParameters;
    }
    List<String> fieldList = new ArrayList<>(fieldToParams.keySet());
    for (int i = 0; i < numPermutations; i++) {
      Map<String, String> combination = new HashMap<>();
      int index = i;
      for (String field : fieldList) {
        List<String> params = fieldToParams.get(field);
        combination.put(field, params.get(index % params.size()));
        index /= params.size();
      }
      tuningParameters.add(combination);
    }

    return tuningParameters;
  }

  /**
   * Given alert filter autotune Id, update to function spec
   * @param id alert filte autotune id
   * @return function Id being updated
   */
  @POST
  @Path("/update/filter/{autotuneId}")
  public Response updateAlertFilterToFunctionSpecByAutoTuneId(@PathParam("autotuneId") long id) {
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    AnomalyFunctionManager anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);
    anomalyFunctionSpec.setAlertFilter(target.getConfiguration());
    anomalyFunctionDAO.update(anomalyFunctionSpec);
    return Response.ok(functionId).build();
  }

  /**
   * Extract alert filter boundary given auto tune Id
   * @param id autotune Id
   * @return alert filter boundary in json format
   */
  @POST
  @Path("/eval/autotuneboundary/{autotuneId}")
  public Response getAlertFilterBoundaryByAutoTuneId(@PathParam("autotuneId") long id) {
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    return Response.ok(target.getConfiguration()).build();
  }

  /**
   * Extract alert filter training data
   * @param id alert filter autotune id
   * @param startTimeIso: alert filter trainig data start time in ISO format: e.g. 2017-02-27T00:00:00.000Z
   * @param endTimeIso: alert filter training data end time in ISO format
   * @param holidayStarts holiday starts time in ISO format to remove merged anomalies: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds holiday ends time in ISO format to remove merged anomalies: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return training data in json format
   */
  @GET
  @Path("/eval/autotunemetadata/{autotuneId}")
  public Response getAlertFilterMetaDataByAutoTuneId(@PathParam("autotuneId") long id,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime;
    long endTime;
    try {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    } catch (Exception e) {
      throw new WebApplicationException(
          "Unable to parse strings, " + startTimeIso + " and " + endTimeIso + ", in ISO DateTime format", e);
    }

    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(functionId, startTime, endTime, holidayStarts, holidayEnds);
    List<AnomalyUtils.MetaDataNode> metaData = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalyResultDTOS) {
      metaData.add(new AnomalyUtils.MetaDataNode(anomaly));
    }
    return Response.ok(metaData).build();
  }

  /**
   * Get Minimum Time to Detection for Function.
   * This endpoint evaluate both alert filter's MTTD and bucket size for function and returns the maximum of the two as MTTD
   * @param id function Id to be evaluated
   * @param severity severity value to evaluate minimum-time-to-detect
   * @return minimum time to detection in HOUR
   */
  @GET
  @Path("/eval/mttd/{functionId}")
  public Response getAlertFilterMTTD(@PathParam("functionId") @NotNull long id,
      @QueryParam("severity") @NotNull double severity) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    BaseAlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    // Compute minimum-time-to-detect for both pattern, and choose the maximum value as the minimum-time-to-detect for alert filter
    // For one pattern alert, the MTTD is always +Infinity for the other pattern, hence this step we only use MTTD from the interested pattern
    // For two pattern alerts, the MTTD is computed to take as the maximum value of both side
    double alertFilterMTTDInHour = 0;
    double alertFilterMTTDUP = alertFilter.getAlertFilterMTTD(severity);
    double alertFilterMTTDDOWN = alertFilter.getAlertFilterMTTD(-1.0 * severity);
    if (!Double.isInfinite(alertFilterMTTDUP) && !Double.isNaN(alertFilterMTTDUP)) {
      alertFilterMTTDInHour = Math.max(alertFilterMTTDInHour, alertFilterMTTDUP);
    }
    if (!Double.isInfinite(alertFilterMTTDDOWN) && !Double.isNaN(alertFilterMTTDDOWN) ) {
      alertFilterMTTDInHour = Math.max(alertFilterMTTDInHour, alertFilterMTTDDOWN);
    }

    TimeUnit detectionUnit = anomalyFunctionSpec.getBucketUnit();
    int detectionBucketSize = anomalyFunctionSpec.getBucketSize();
    int detectionMinBuckets = 1;
    Properties functionProps = toProperties(anomalyFunctionSpec.getProperties());
    detectionMinBuckets = Integer.valueOf(functionProps.getProperty(SIGN_TEST_WINDOW_SIZE, "1"));
    double functionMTTDInHour = TimeUnit.HOURS.convert(detectionBucketSize * detectionMinBuckets, detectionUnit);
    return Response.ok(Math.max(functionMTTDInHour, alertFilterMTTDInHour)).build();
  }

  /**
   * Get Minimum Time to Detection for Autotuned (Preview) Alert Filter.
   * This endpoint evaluate both alert filter's MTTD and bucket size for function and returns the maximum of the two as MTTD
   * @param id autotune Id to be evaluated
   * @param severity severity value
   * @return minimum time to detection in HOUR
   */
  @GET
  @Path("/eval/projected/mttd/{autotuneId}")
  public Response getProjectedMTTD(@PathParam("autotuneId") @NotNull long id,
      @QueryParam("severity") @NotNull double severity) {
    //Initiate tuned alert filter
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    Map<String, String> tunedParams = target.getConfiguration();
    BaseAlertFilter alertFilter = alertFilterFactory.fromSpec(tunedParams);
    // Get current function
    long functionId = target.getFunctionId();
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);
    // Compute minimum-time-to-detect for both pattern, and choose the maximum value as the minimum-time-to-detect for alert filter
    // For one pattern alert, the MTTD is always +Infinity for the other pattern, hence this step we only use MTTD from the interested pattern
    // For two pattern alerts, the MTTD is computed to take as the maximum value of both side

    double alertFilterMTTDInHour = 0;
    double alertFilterMTTDUP = alertFilter.getAlertFilterMTTD(severity);
    double alertFilterMTTDDOWN = alertFilter.getAlertFilterMTTD(-1.0 * severity);
    if (!Double.isInfinite(alertFilterMTTDUP) && !Double.isNaN(alertFilterMTTDUP)) {
      alertFilterMTTDInHour = Math.max(alertFilterMTTDInHour, alertFilterMTTDUP);
    }
    if (!Double.isInfinite(alertFilterMTTDDOWN) && !Double.isNaN(alertFilterMTTDDOWN)) {
      alertFilterMTTDInHour = Math.max(alertFilterMTTDInHour, alertFilterMTTDDOWN);
    }

    TimeUnit detectionUnit = anomalyFunctionSpec.getBucketUnit();
    int detectionBucketSize = anomalyFunctionSpec.getBucketSize();
    int detectionMinBuckets = 1;
    Properties functionProps = toProperties(anomalyFunctionSpec.getProperties());
    detectionMinBuckets = Integer.valueOf(functionProps.getProperty(SIGN_TEST_WINDOW_SIZE, "1"));
    double functionMTTDInHour = TimeUnit.HOURS.convert(detectionBucketSize * detectionMinBuckets, detectionUnit);
    return Response.ok(Math.max(functionMTTDInHour, alertFilterMTTDInHour)).build();
  }

  /**
   * Given autotuneId and a list of anomalies, return the anomaly Ids that qualified for alert filter
   * @param autotuneId
   * @param startTimeIso Start time of the monitoring window in ISO format
   * @param endTimeIso End time of the monitoring window in ISO format
   * @return Array list of anomaly id that pass the alert filter in autotune
   */
  @GET
  @Path("/eval/projected/anomalies/{autotuneId}")
  public ArrayList<Long> getPreviewedAnomaliesByAutoTuneId(@PathParam("autotuneId") long autotuneId,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso) {
    long startTime;
    long endTime;
    try {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    } catch (Exception e) {
      throw new WebApplicationException(
          "Unable to parse strings, " + startTimeIso + " and " + endTimeIso + ", in ISO DateTime format", e);
    }

    // Initiate tuned alert filter
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(autotuneId);
    // Get function Id belongs to this autotune
    long functionId = target.getFunctionId();
    // Fetch anomalies within the time range
    List<MergedAnomalyResultDTO> mergedResults =
        mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime, endTime, functionId);
    // Initiate alert filter to BaseAlertFilter
    Map<String, String> tunedParams = target.getConfiguration();
    BaseAlertFilter alertFilter = alertFilterFactory.fromSpec(tunedParams);
    // Apply tuned alert filter to anomalies
    ArrayList<Long> anomalyIdList = new ArrayList<>();
    for (MergedAnomalyResultDTO mergedAnomaly : mergedResults) {
      if (alertFilter.isQualified(mergedAnomaly)) {
        anomalyIdList.add(mergedAnomaly.getId());
      }
    }
    return anomalyIdList;
  }
}
