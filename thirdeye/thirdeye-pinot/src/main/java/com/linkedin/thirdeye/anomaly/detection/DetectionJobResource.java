package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutoTune;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterEvaluationUtil;
import com.linkedin.thirdeye.util.SeverityComputationUtil;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.*;


@Path("/detection-job")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionJobResource {
  private final DetectionJobScheduler detectionJobScheduler;
  private final AnomalyFunctionManager anomalyFunctionSpecDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final RawAnomalyResultManager rawAnomalyResultDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private final AlertFilterAutotuneFactory alertFilterAutotuneFactory;
  private final AlertFilterFactory alertFilterFactory;

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobResource.class);

  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler, AlertFilterFactory alertFilterFactory, AlertFilterAutotuneFactory alertFilterAutotuneFactory) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.anomalyFunctionSpecDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.rawAnomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.alertFilterAutotuneFactory = alertFilterAutotuneFactory;
    this.alertFilterFactory = alertFilterFactory;
  }


  @POST
  @Path("/{id}")
  public Response enable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, true);
    return Response.ok().build();
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

  @DELETE
  @Path("/{id}")
  public Response disable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, false);
    return Response.ok().build();
  }

  private void toggleActive(Long id, boolean state) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if(anomalyFunctionSpec == null) {
      throw new NullArgumentException("Function spec not found");
    }
    anomalyFunctionSpec.setIsActive(state);
    anomalyFunctionSpecDAO.update(anomalyFunctionSpec);
  }


  @POST
  @Path("/requiresCompletenessCheck/enable/{id}")
  public Response enableRequiresCompletenessCheck(@PathParam("id") Long id) throws Exception {
    toggleRequiresCompletenessCheck(id, true);
    return Response.ok().build();
  }

  @POST
  @Path("/requiresCompletenessCheck/disable/{id}")
  public Response disableRequiresCompletenessCheck(@PathParam("id") Long id) throws Exception {
    toggleRequiresCompletenessCheck(id, false);
    return Response.ok().build();
  }

  private void toggleRequiresCompletenessCheck(Long id, boolean state) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if(anomalyFunctionSpec == null) {
      throw new NullArgumentException("Function spec not found");
    }
    anomalyFunctionSpec.setRequiresCompletenessCheck(state);
    anomalyFunctionSpecDAO.update(anomalyFunctionSpec);
  }


  /**
   * Returns the weight of the metric at the given window. The calculation of baseline (history) data is specified by
   * seasonal period (in days) and season count. Seasonal period is the difference of duration from one window to the
   * other. For instance, to use the data that is one week before current window, set seasonal period to 7. The season
   * count specify how many seasons of history data to retrieve. If there are more than 1 season, then the baseline is
   * the average of all seasons.
   *
   * Examples of the configuration of baseline:
   * 1. Week-Over-Week: seasonalPeriodInDays = 7, seasonCount = 1
   * 2. Week-Over-4-Weeks-Mean: seasonalPeriodInDays = 7, seasonCount = 4
   * 3. Month-Over-Month: seasonalPeriodInDays = 30, seasonCount = 1
   *
   * @param collectionName the collection to which the metric belong
   * @param metricName the metric name
   * @param startTimeIso start time of current window, inclusive
   * @param endTimeIso end time of current window, exclusive
   * @param seasonalPeriodInDays the difference of duration between the start time of each window
   * @param seasonCount the number of history windows
   *
   * @return the weight of the metric at the given window
   * @throws Exception
   */
  @POST
  @Path("/anomaly-weight")
  public Response computeSeverity(@NotNull @QueryParam("collection") String collectionName,
      @NotNull @QueryParam("metric") String metricName,
      @NotNull @QueryParam("start") String startTimeIso, @NotNull @QueryParam("end") String endTimeIso,
      @QueryParam("period") String seasonalPeriodInDays, @QueryParam("seasonCount") String seasonCount)
      throws Exception {
    DateTime startTime = null;
    DateTime endTime = null;
    if (StringUtils.isNotBlank(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    if (StringUtils.isNotBlank(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }
    long currentWindowStart = startTime.getMillis();
    long currentWindowEnd = endTime.getMillis();

    // Default is using one week data priors current values for calculating weight
    long seasonalPeriodMillis = TimeUnit.DAYS.toMillis(7);
    if (StringUtils.isNotBlank(seasonalPeriodInDays)) {
      seasonalPeriodMillis = TimeUnit.DAYS.toMillis(Integer.parseInt(seasonalPeriodInDays));
    }
    int seasonCountInt = 1;
    if (StringUtils.isNotBlank(seasonCount)) {
      seasonCountInt = Integer.parseInt(seasonCount);
    }

    ThirdEyeClient thirdEyeClient = CACHE_REGISTRY_INSTANCE.getQueryCache().getClient();
    SeverityComputationUtil util = new SeverityComputationUtil(thirdEyeClient, collectionName, metricName);
    Map<String, Object> severity =
        util.computeSeverity(currentWindowStart, currentWindowEnd, seasonalPeriodMillis, seasonCountInt);

    return Response.ok(severity.toString(), MediaType.TEXT_PLAIN_TYPE).build();
  }

  /**
   * Breaks down the given range into consecutive monitoring windows as per function definition
   * Regenerates anomalies for each window separately
   *
   * As the anomaly result regeneration is a heavy job, we move the function from Dashboard to worker
   * @param id anomaly function id
   * @param startTimeIso The start time of the monitoring window (in ISO Format), ex: 2016-5-23T00:00:00Z
   * @param endTimeIso The start time of the monitoring window (in ISO Format)
   * @param isForceBackfill false to resume backfill from the latest left off
   * @return HTTP response of this request
   * @throws Exception
   */
  @POST
  @Path("/{id}/generateAnomaliesInRange")
  public Response generateAnomaliesInRange(@PathParam("id") String id,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("force") @DefaultValue("false") String isForceBackfill) throws Exception {
    final long functionId = Long.valueOf(id);
    final boolean forceBackfill = Boolean.valueOf(isForceBackfill);
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      return Response.noContent().build();

    }

    // Check if the timestamps are available
    DateTime startTime = null;
    DateTime endTime = null;
    if (startTimeIso == null || startTimeIso.isEmpty()) {
      throw new IllegalArgumentException(String.format("[functionId %s] Monitoring start time is not found", id));
    }
    if (endTimeIso == null || endTimeIso.isEmpty()) {
      throw new IllegalArgumentException(String.format("[functionId %s] Monitoring end time is not found", id));
    }

    startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);

    if(startTime.isAfter(endTime)){
      throw new IllegalArgumentException(String.format(
          "[functionId %s] Monitoring start time is after monitoring end time", id));
    }
    if(endTime.isAfterNow()){
      throw new IllegalArgumentException(String.format(
          "[functionId %s] Monitor end time {} should not be in the future", id, endTime.toString()));
    }

    // Check if the merged anomaly results have been cleaned up before regeneration
    List<MergedAnomalyResultDTO> mergedResults =
        mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime.getMillis(), endTime.getMillis(),
            functionId);
    if (CollectionUtils.isNotEmpty(mergedResults) && !forceBackfill) {
      throw new IllegalArgumentException(String.format(
          "[functionId %s] Merged anomaly results should be cleaned up before regeneration", id));
    }

    //  Check if the raw anomaly results have been cleaned up before regeneration
    List<RawAnomalyResultDTO> rawResults =
        rawAnomalyResultDAO.findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), functionId);
    if(CollectionUtils.isNotEmpty(rawResults) && !forceBackfill){
      throw new IllegalArgumentException(String.format(
          "[functionId {}] Raw anomaly results should be cleaned up before regeneration", id));
    }

    // Check if the anomaly function is active
    if (!anomalyFunction.getIsActive()) {
      throw new IllegalArgumentException(String.format("Skipping deactivated function %s", id));
    }

    String response = null;

    final DateTime innerStartTime = startTime;
    final DateTime innerEndTime = endTime;

    new Thread(new Runnable() {
      @Override
      public void run() {
        detectionJobScheduler.runBackfill(functionId, innerStartTime, innerEndTime, forceBackfill);
      }
    }).start();

    return Response.ok().build();
  }

  /**
   * Given a list of holiday starts and holiday ends, return merged anomalies with holidays removed
   * @param functionId: the functionId to fetch merged anomalies with holidays removed
   * @param startTime: start time in milliseconds of merged anomalies
   * @param endTime: end time of in milliseconds merged anomalies
   * @param holidayStarts: holidayStarts in milliseconds as string, in format {start1,start2,...}
   * @param holidayEnds: holidayEnds in milliseconds as string, in format {end1, end2,...}
   * @return a list of merged anomalies with holidays removed
   */
  public static List<MergedAnomalyResultDTO> getMergedAnomaliesRemoveHolidays(long functionId, long startTime, long endTime, String holidayStarts, String holidayEnds){
    StringTokenizer starts = new StringTokenizer(holidayStarts, ",");
    StringTokenizer ends = new StringTokenizer(holidayEnds, ",");
    MergedAnomalyResultManager anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    List<MergedAnomalyResultDTO> totalAnomalies = anomalyMergedResultDAO.findByStartTimeInRangeAndFunctionId(startTime, endTime, functionId);
    int origSize = totalAnomalies.size();
    while (starts.hasMoreElements() && ends.hasMoreElements()){
      List<MergedAnomalyResultDTO> holidayMergedAnomalies = anomalyMergedResultDAO.findByStartTimeInRangeAndFunctionId(Long.valueOf(starts.nextToken()), Long.valueOf(ends.nextToken()), functionId);
      totalAnomalies.removeAll(holidayMergedAnomalies);
    }
    if(starts.hasMoreElements() || ends.hasMoreElements()){
      LOG.warn("Input holiday starts and ends length not equal!");
    }
    LOG.info("Removed {} merged anomalies", origSize - totalAnomalies.size());
    return totalAnomalies;
  }


  /**
   *
   * @param id anomaly function id
   * @param startTime start time of anomalies to tune alert filter
   * @param endTime end time of anomalies to tune alert filter
   * @param autoTuneType the type of auto tune to invoke (default is "AUTOTUNE")
   * @param holidayStarts optional: holidayStarts in milliseconds as string, in format {start1,start2,...}
   * @param holidayEnds optional:holidayEnds in milliseconds as string, in format {end1,end2,...}
   * @return HTTP response of request: string of alert filter
   */
  @POST
  @Path("/autotune/filter/{functionId}")
  public Response tuneAlertFilter(@PathParam("functionId") long id,
      @QueryParam("startTime") long startTime,
      @QueryParam("endTime") long endTime,
      @QueryParam("autoTuneType") @DefaultValue("AUTOTUNE") String autoTuneType,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    // get anomalies by function id, start time and end time
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);
    AutotuneConfigDTO target = new AutotuneConfigDTO();

    // create alert filter and evaluator
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    AlertFilterEvaluationUtil evaluator = new AlertFilterEvaluationUtil(alertFilter);

    // create alert filter auto tune
    AlertFilterAutoTune alertFilterAutotune = alertFilterAutotuneFactory.fromSpec(autoTuneType);
    LOG.info("initiated alertFilterAutoTune of Type {}", alertFilterAutotune.getClass().toString());
    long autotuneId = -1;
    try {
      //evaluate current alert filter (calculate current precision and recall)
      evaluator.updatePrecisionAndRecall(anomalyResultDTOS);
      LOG.info("AlertFilter of Type {}, has been evaluated with precision: {}, recall: {}", alertFilter.getClass().toString(), evaluator.getPrecision(), evaluator.getRecall());

      // get tuned alert filter
      Map<String,String> tunedAlertFilter = alertFilterAutotune.tuneAlertFilter(anomalyResultDTOS, evaluator.getPrecision(), evaluator.getRecall());
      LOG.info("tuned AlertFilter");

      // if alert filter auto tune has updated alert filter, write to autotune_config_index, and get the autotuneId
      // otherwise do nothing and return alert filter
      if (alertFilterAutotune.isUpdated()){
        target.setFunctionId(id);
        target.setAutotuneMethod(ALERT_FILTER_LOGISITC_AUTO_TUNE);
        target.setConfiguration(tunedAlertFilter);
        target.setPerformanceEvaluationMethod(PerformanceEvaluationMethod.PRECISION_AND_RECALL);
        autotuneId = DAO_REGISTRY.getAutotuneConfigDAO().save(target);
        LOG.info("Model has been updated");
      } else {
        LOG.info("Model hasn't been updated because tuned model cannot beat original model");
      }
    } catch (Exception e) {
      LOG.warn("AutoTune throws exception due to: {}", e.getMessage());
    }
    return Response.ok(autotuneId).build();
  }

  /**
   * Endpoint to check if merged anomalies given a time period have at least one positive label
   * @param id functionId to test anomalies
   * @param startTime
   * @param endTime
   * @param holidayStarts optional: holidayStarts in milliseconds as string, in format {start1,start2,...}
   * @param holidayEnds optional:holidayEnds in milliseconds as string, in format {end1,end2,...}
   * @return true if the list of merged anomalies has at least one positive label, false otherwise
   */
  @POST
  @Path("/initautotune/checkhaslabel/{functionId}")
  public Response checkAnomaliesHasLabel(@PathParam("functionId") long id,
      @QueryParam("startTime") long startTime,
      @QueryParam("endTime") long endTime,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds){
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);
    return Response.ok(AnomalyUtils.checkHasLabels(anomalyResultDTOS)).build();
  }

  /**
   * End point to trigger initiate alert filter auto tune
   * @param id functionId to initiate alert filter auto tune
   * @param startTime: training data starts time
   * @param endTime: training data ends time
   * @param autoTuneType: By default is "AUTOTUNE"
   * @param nExpectedAnomalies: number of expected anomalies to recommend users to label
   * @param holidayStarts optional: holidayStarts in milliseconds as string, in format {start1,start2,...}
   * @param holidayEnds optional:holidayEnds in milliseconds as string, in format {end1,end2,...}
   * @return true if alert filter has successfully being initiated, false otherwise
   */
  @POST
  @Path("/initautotune/filter/{functionId}")
  public Response initiateAlertFilterAutoTune(@PathParam("functionId") long id,
      @QueryParam("startTime") long startTime,
      @QueryParam("endTime") long endTime,
      @QueryParam("autoTuneType") @DefaultValue("AUTOTUNE") String autoTuneType, @QueryParam("nExpected") int nExpectedAnomalies,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds){

    // get anomalies by function id, start time and end time
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);

    //initiate AutoTuneConfigDTO
    AutotuneConfigDTO target = new AutotuneConfigDTO();

    // create alert filter auto tune
    AlertFilterAutoTune alertFilterAutotune = alertFilterAutotuneFactory.fromSpec(autoTuneType);
    long autotuneId = -1;
    try{
      Map<String,String> tunedAlertFilter = alertFilterAutotune.initiateAutoTune(anomalyResultDTOS, nExpectedAnomalies);

      // if achieved the initial alert filter
      if (alertFilterAutotune.isUpdated()) {
        target.setFunctionId(id);
        target.setConfiguration(tunedAlertFilter);
        target.setAutotuneMethod(INITIATE_ALERT_FILTER_LOGISTIC_AUTO_TUNE);
        autotuneId = DAO_REGISTRY.getAutotuneConfigDAO().save(target);
      } else {
        LOG.info("Failed init alert filter since model hasn't been updated");
      }
    } catch (Exception e){
      LOG.warn("Failed to achieve init alert filter: {}", e.getMessage());
    }
    return Response.ok(autotuneId).build();
  }


  /**
   * The endpoint to evaluate alert filter
   * @param id: function ID
   * @param startTime: startTime of merged anomaly
   * @param endTime: endTime of merged anomaly
   * @return feedback summary, precision and recall as json object
   * @throws Exception when data has no positive label or model has no positive prediction
   */
  @POST
  @Path("/eval/filter/{functionId}")
  public Response evaluateAlertFilterByFunctionId(@PathParam("functionId") long id,
      @QueryParam("startTime") long startTime,
      @QueryParam("endTime") long endTime,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    // get anomalies by function id, start time and end time`
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);

    // create alert filter and evaluator
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    AlertFilterEvaluationUtil evaluator = new AlertFilterEvaluationUtil(alertFilter);

    try{
      //evaluate current alert filter (calculate current precision and recall)
      evaluator.updatePrecisionAndRecall(anomalyResultDTOS);
      LOG.info("AlertFilter of Type {}, has been evaluated with precision: {}, recall:{}", alertFilter.getClass().toString(),
          evaluator.getPrecision(), evaluator.getRecall());
    } catch (Exception e) {
      LOG.warn("Updating precision and recall failed because: {}", e.getMessage());
    }

    // get anomaly summary from merged anomaly results
    evaluator.updateFeedbackSummary(anomalyResultDTOS);
    return Response.ok(evaluator.toProperties().toString()).build();
  }

  /**
   * To evaluate alert filte directly by autotune Id using autotune_config_index table
   * This is to leverage the intermediate step before updating tuned alert filter configurations
   * @param id: autotune Id
   * @param startTime: merged anomalies start time
   * @param endTime: merged anomalies end time
   * @param holidayStarts: holiday starts time to remove merged anomalies
   * @param holidayEnds: holiday ends time to remove merged anomlaies
   * @return HTTP response of evaluation results
   */
  @POST
  @Path("/eval/autotune/{autotuneId}")
  public Response evaluateAlertFilterByAutoTuneId(@PathParam("autotuneId") long id,
      @QueryParam("startTime") long startTime, @QueryParam("endTime") long endTime,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds){
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(functionId, startTime, endTime, holidayStarts, holidayEnds);

    Map<String, String> alertFilterParams = target.getConfiguration();
    AlertFilter alertFilter = alertFilterFactory.fromSpec(alertFilterParams);
    AlertFilterEvaluationUtil evaluator = new AlertFilterEvaluationUtil(alertFilter);

    try{
      evaluator.updatePrecisionAndRecall(anomalyResultDTOS);
    } catch (Exception e){
      LOG.warn("Updating precision and recall failed");
    }
    evaluator.updateFeedbackSummary(anomalyResultDTOS);
    return Response.ok(evaluator.toProperties().toString()).build();
  }

}
