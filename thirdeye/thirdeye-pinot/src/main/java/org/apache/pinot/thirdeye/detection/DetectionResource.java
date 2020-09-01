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

package org.apache.pinot.thirdeye.detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ResourceUtils;
import org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause.AnomalyEventFormatter;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalySubscriptionGroupNotificationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalySubscriptionGroupNotificationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.finetune.GridSearchTuningAlgorithm;
import org.apache.pinot.thirdeye.detection.finetune.TuningAlgorithm;
import org.apache.pinot.thirdeye.detection.health.DetectionHealth;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.formatter.DetectionAlertConfigFormatter;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.AnomalyOffset;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.DETECTION_TAG})
@Singleton
public class DetectionResource {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DAILY_CRON = "0 0 14 * * ? *";
  private static final String HOURLY_CRON = "0 0 * * * ? *";
  private static final String MINUTE_CRON = "0 0/15 * * * ? *";

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;
  private final DetectionConfigManager configDAO;
  private final EvaluationManager evaluationDAO;
  private final TaskManager taskDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;
  private final DetectionConfigFormatter detectionConfigFormatter;
  private final DetectionAlertConfigFormatter subscriptionConfigFormatter;
  private final AggregationLoader aggregationLoader;
  private final DetectionConfigurationResource detectionConfigurationResource;
  private final AnomalySubscriptionGroupNotificationManager anomalySubscriptionGroupNotificationManager;

  @Inject
  public DetectionResource(
      final DetectionConfigurationResource detectionConfigurationResource) {
    this.detectionConfigurationResource = detectionConfigurationResource;

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.anomalySubscriptionGroupNotificationManager = DAORegistry.getInstance().getAnomalySubscriptionGroupNotificationManager();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());

    this.aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, loader, TimeSeriesCacheBuilder.getInstance(),
        AnomaliesCacheBuilder.getInstance());
    this.detectionConfigFormatter = new DetectionConfigFormatter(metricDAO, datasetDAO);
    this.subscriptionConfigFormatter = new DetectionAlertConfigFormatter();
  }

  @Path("rule")
  public DetectionConfigurationResource getDetectionConfigurationResource() {
    return detectionConfigurationResource;
  }

  @Path("/{id}")
  @GET
  @ApiOperation("get a detection config with yaml")
  public Response getDetectionConfig(@ApiParam("the detection config id") @PathParam("id") long id){
    DetectionConfigDTO config = this.configDAO.findById(id);
    return Response.ok(this.detectionConfigFormatter.format(config)).build();
  }

  @Path("/notification/{id}")
  @GET
  @ApiOperation("get a detection alert config with yaml")
  public Response getDetectionAlertConfig(@ApiParam("the detection alert config id") @PathParam("id") long id){
    DetectionAlertConfigDTO config = this.detectionAlertConfigDAO.findById(id);
    return Response.ok(this.subscriptionConfigFormatter.format(config)).build();
  }

  @Path("/dataset")
  @GET
  @ApiOperation("get a dataset config by name")
  public Response getDetectionAlertConfig(@ApiParam("the dataset name") @QueryParam("name") String name){
    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(name);
    return Response.ok(dataset).build();
  }

  @Path("/subscription-groups/{id}")
  @GET
  @ApiOperation("get a list of detection alert configs for a given detection config id")
  public Response getSubscriptionGroups(@ApiParam("the detection config id") @PathParam("id") long id){
    List<DetectionAlertConfigDTO> detectionAlertConfigDTOs = this.detectionAlertConfigDAO.findAll();
    Set<DetectionAlertConfigDTO> subscriptionGroupAlertDTOs = new HashSet<>();
    for (DetectionAlertConfigDTO alertConfigDTO : detectionAlertConfigDTOs){
      if (alertConfigDTO.getVectorClocks().containsKey(id) || ConfigUtils.getLongs(alertConfigDTO.getProperties().get("detectionConfigIds")).contains(id)){
        subscriptionGroupAlertDTOs.add(alertConfigDTO);
      }
    }
    return Response.ok(subscriptionGroupAlertDTOs.stream().map(this.subscriptionConfigFormatter::format).collect(Collectors.toList())).build();
  }


  @Path("/subscription-groups")
  @GET
  @ApiOperation("get all detection alert configs")
  public Response getAllSubscriptionGroups(){
    List<DetectionAlertConfigDTO> detectionAlertConfigDTOs = this.detectionAlertConfigDAO.findAll();
    return Response.ok(detectionAlertConfigDTOs.stream().map(this.subscriptionConfigFormatter::format).collect(Collectors.toList())).build();
  }

  @Path("{id}/anomalies")
  @GET
  @ApiOperation("Get all anomalies within the time range for a detection config id")
  public Response getAnomalies(@PathParam("id") Long detectionConfigId, @QueryParam("start") long startTime,
      @QueryParam("end") long endTime) {
    List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(Predicate.AND(
            Predicate.EQ("detectionConfigId", detectionConfigId),
            Predicate.LT("startTime", endTime),
            Predicate.GT("endTime", startTime)));
    List result = anomalies.stream().filter(anomaly -> !anomaly.isChild()).map(anomaly -> {
      Map<String, Object> anomalyResult = OBJECT_MAPPER.convertValue(anomaly, Map.class);
      anomalyResult.put(AnomalyEventFormatter.ATTR_STATUS_CLASSIFICATION, ResourceUtils.getStatusClassification(anomaly).toString());
      return anomalyResult;
    }).collect(Collectors.toList());
    return Response.ok(result).build();
  }

  @POST
  @Path("/preview")
  @ApiOperation("preview a detection with detection config json")
  public Response detectionPreview(
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("diagnostics") Boolean diagnostics,
      @ApiParam("jsonPayload") String jsonPayload) throws Exception {
    if (jsonPayload == null) {
      throw new IllegalArgumentException("Empty Json Payload");
    }

    Map<String, Object> properties = OBJECT_MAPPER.readValue(jsonPayload, Map.class);

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setId(Long.MAX_VALUE);
    config.setName("preview");
    config.setDescription("previewing the detection");
    config.setProperties(ConfigUtils.getMap(properties.get("properties")));
    config.setComponentSpecs(ConfigUtils.getMap(properties.get("componentSpecs")));

    DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
    DetectionPipelineResult result = pipeline.run();

    if (diagnostics == null || !diagnostics) {
      result.setDiagnostics(Collections.<String, Object>emptyMap());
    }

    return Response.ok(result).build();
  }

  @POST
  @Path("/gridsearch")
  public Response gridSearch(
      @QueryParam("configId") long configId,
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @ApiParam("jsonPayload") String jsonPayload) throws Exception {
    if (jsonPayload == null) {
      throw new IllegalArgumentException("Empty Json Payload");
    }

    Map<String, Object> json = OBJECT_MAPPER.readValue(jsonPayload, Map.class);

    LinkedHashMap<String, List<Number>> parameters = (LinkedHashMap<String, List<Number>>) json.get("parameters");

    AnomalySlice slice = new AnomalySlice().withDetectionId(configId).withStart(start).withEnd(end);

    TuningAlgorithm gridSearch = new GridSearchTuningAlgorithm(OBJECT_MAPPER.writeValueAsString(json.get("properties")), parameters);
    gridSearch.fit(slice, configId);

    return Response.ok(gridSearch.bestDetectionConfig().getProperties()).build();
  }

  @GET
  @Path("/preview/{id}")
  @ApiOperation("preview a detection with a existing detection config")
  public Response detectionPreview(
      @PathParam("id") long id,
      @QueryParam("start") long start,
      @QueryParam("diagnostics") Boolean diagnostics,
      @QueryParam("end") long end) throws Exception {

    DetectionConfigDTO config = this.configDAO.findById(id);
    if (config == null) {
      throw new IllegalArgumentException("Detection Config not exist");
    }

    DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
    DetectionPipelineResult result = pipeline.run();

    if (diagnostics == null || !diagnostics) {
      result.setDiagnostics(Collections.emptyMap());
    }

    return Response.ok(result).build();
  }

  // return default bucket size based on cron schedule.
  private long getBucketSize(DetectionConfigDTO config){
    switch (config.getCron()) {
      case DAILY_CRON:
        // daily
        return TimeUnit.DAYS.toMillis(1);
      case MINUTE_CRON:
        // minute level
        return TimeUnit.MINUTES.toMillis(15);
      case HOURLY_CRON:
        // hourly
        return TimeUnit.HOURS.toMillis(1);
    }
    throw new IllegalArgumentException("bucket size not determined");
  }

  // return default window size based on cron schedule.
  private long getWindowSize(DetectionConfigDTO config) {
    switch (config.getCron()) {
      case DAILY_CRON:
        // daily
        return TimeUnit.DAYS.toMillis(1);
      case MINUTE_CRON:
        // minute level
        return TimeUnit.HOURS.toMillis(6);
      case HOURLY_CRON:
        // hourly
        return TimeUnit.HOURS.toMillis(24);
    }
    throw new IllegalArgumentException("window size not determined");
  }

  /*
  Generates monitoring window based on cron schedule.
   */
  private List<Interval> getReplayMonitoringWindows(DetectionConfigDTO config, long start, long end, Long windowSize, Long bucketSize) throws ParseException {
    List<Interval> monitoringWindows = new ArrayList<>();
    CronExpression cronExpression = new CronExpression(config.getCron());
    DateTime currentStart = new DateTime(start);

    long legacyWindowSize;
    if(windowSize == null){
      legacyWindowSize = getWindowSize(config);
      LOG.warn("[Legacy replay] window size not set when replay {}. Use default window size {}", config.getId(), legacyWindowSize);
    } else {
      legacyWindowSize = windowSize;
    }

    long legacyBucketSize;
    if (bucketSize == null){
      legacyBucketSize = getBucketSize(config);
      LOG.warn("[Legacy replay] bucket size not set when replay {}. Use default bucket size {}", config.getId(), legacyBucketSize);
    } else {
      legacyBucketSize = bucketSize;
    }

    // add offsets to that it would replay all the moving windows within start time and end time
    currentStart = currentStart.plus(legacyWindowSize).minus(legacyBucketSize);
    if (config.getCron().equals(DAILY_CRON)){
      // daily detection offset by 1 to pick up the first moving window
      currentStart.minus(1L);
    }

    DateTime currentEnd = new DateTime(cronExpression.getNextValidTimeAfter(currentStart.toDate()));
    DateTime endBoundary = new DateTime(cronExpression.getNextValidTimeAfter(new Date(end)));
    while (currentEnd.isBefore(endBoundary)) {
      monitoringWindows.add(new Interval(currentStart.getMillis(), currentEnd.getMillis()));
      currentStart = currentEnd;
      currentEnd =  new DateTime(cronExpression.getNextValidTimeAfter(currentStart.toDate()));
    }
    return monitoringWindows;
  }

  /**
   * Replay for a given time range. Without cron schedule behavior
   *
   * @param detectionId detection config id (must exist)
   * @param start start time in epoch (millis)
   * @param end end time in epoch (millis)
   * @param deleteExistingAnomaly (optional, default false) delete existing anomaly or not
   */
  @POST
  @Path("/replay/{id}")
  @ApiOperation("Replay for a given time range for a existing detection config id")
  public Response detectionReplay(
      @PathParam("id") long detectionId,
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("deleteExistingAnomaly") @DefaultValue("false") boolean deleteExistingAnomaly) throws Exception {
    Map<String, String> responseMessage = new HashMap<>();
    long ts = System.currentTimeMillis();
    DetectionPipelineResult result;
    try {
      DetectionConfigDTO config = this.configDAO.findById(detectionId);
      if (config == null) {
        throw new IllegalArgumentException(String.format("Cannot find config %d", detectionId));
      }

      if (deleteExistingAnomaly) {
        AnomalySlice slice = new AnomalySlice().withDetectionId(detectionId).withStart(start).withEnd(end);
        Collection<MergedAnomalyResultDTO> existing =
            this.provider.fetchAnomalies(Collections.singleton(slice)).get(slice);

        List<Long> existingIds = new ArrayList<>();
        for (MergedAnomalyResultDTO anomaly : existing) {
          existingIds.add(anomaly.getId());
        }

        this.anomalyDAO.deleteByIds(existingIds);
      }

      // execute replay
      DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
      result = pipeline.run();

      // Update state
      if (result.getLastTimestamp() > config.getLastTimestamp()) {
        config.setLastTimestamp(result.getLastTimestamp());
        this.configDAO.update(config);
      }

      for (MergedAnomalyResultDTO anomaly : result.getAnomalies()) {
        anomaly.setAnomalyResultSource(AnomalyResultSource.ANOMALY_REPLAY);
        this.anomalyDAO.save(anomaly);
      }
    } catch (Exception e) {
      LOG.error("Error running replay on detection id " + detectionId, e);
      responseMessage.put("message", "Failed to run the replay due to " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    long duration = System.currentTimeMillis() - ts;
    LOG.info("Replay detection pipeline {} generated {} anomalies and took {} millis.",
        detectionId, result.getAnomalies().size(), duration);
    return Response.ok(result).build();
  }


  /**
   * Create a user-reported anomaly for a new detection pipeline
   *
   * @param detectionConfigId detection config id (must exist)
   * @param startTime start time utc (in millis)
   * @param endTime end time utc (in millis)
   * @param metricUrn the metric urn of the anomaly
   * @param feedbackType anomaly feedback type
   * @param comment anomaly feedback comment (optional)
   * @throws IllegalArgumentException if the anomaly function id cannot be found
   * @throws IllegalArgumentException if the anomaly cannot be stored
   */
  @POST
  @Path(value = "/report-anomaly/{detectionConfigId}")
  @ApiOperation("Report a missing anomaly for a detection config")
  public Response createUserAnomaly(
      @PathParam("detectionConfigId") @ApiParam(value = "detection config id") long detectionConfigId,
      @QueryParam("startTime") @ApiParam("start time utc (in millis)") Long startTime,
      @QueryParam("endTime") @ApiParam("end time utc (in millis)") Long endTime,
      @QueryParam("metricUrn") @ApiParam("the metric urn of the anomaly") String metricUrn,
      @QueryParam("feedbackType") @ApiParam("the metric urn of the anomaly") AnomalyFeedbackType feedbackType,
      @QueryParam("comment") @ApiParam("comments") String comment,
      @QueryParam("baselineValue") @ApiParam("the baseline value for the anomaly") @DefaultValue("NaN") double baselineValue) {

    DetectionConfigDTO detectionConfigDTO = this.configDAO.findById(detectionConfigId);
    if (detectionConfigDTO == null) {
      throw new IllegalArgumentException(String.format("Could not resolve detection config id %d", detectionConfigId));
    }

    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(startTime);
    anomaly.setEndTime(endTime);
    anomaly.setDetectionConfigId(detectionConfigId);
    anomaly.setAnomalyResultSource(AnomalyResultSource.USER_LABELED_ANOMALY);
    anomaly.setProperties(Collections.<String, String>emptyMap());

    MetricEntity me = MetricEntity.fromURN(metricUrn);
    MetricConfigDTO metric = this.metricDAO.findById(me.getId());
    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    anomaly.setMetricUrn(metricUrn);
    anomaly.setMetric(metric.getName());
    anomaly.setCollection(dataset.getDataset());

    try {
      MetricSlice currentSlice = MetricSlice.from(me.getId(), startTime, endTime, me.getFilters());
      DataFrame df = this.aggregationLoader.loadAggregate(currentSlice, Collections.<String>emptyList(), -1);
      anomaly.setAvgCurrentVal(df.getDouble(COL_VALUE, 0));
    } catch (Exception e) {
      LOG.warn("Can't get the current value for {}, from {}-{}", me.getId(), startTime, endTime, e);
      anomaly.setAvgCurrentVal(Double.NaN);
    }
    anomaly.setAvgBaselineVal(baselineValue);

    if (this.anomalyDAO.save(anomaly) == null) {
      throw new IllegalArgumentException(String.format("Could not store user reported anomaly: '%s'", anomaly));
    }

    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setFeedbackType(feedbackType);
    feedback.setComment(comment);
    anomaly.setFeedback(feedback);
    this.anomalyDAO.updateAnomalyFeedback(anomaly);

    return Response.ok(anomaly.getId()).build();
  }

  @DELETE
  @Path(value="/report-anomaly/{id}")
  public Response deleteUserReportedAnomaly(@PathParam("id") long anomalyId) {
    MergedAnomalyResultDTO anomaly = this.anomalyDAO.findById(anomalyId);
    if (anomaly == null || !anomaly.getAnomalyResultSource().equals(AnomalyResultSource.USER_LABELED_ANOMALY)) {
      return Response.status(Response.Status.BAD_REQUEST).entity(String.format("Couldn't delete anomaly %d", anomalyId)).build();
    }
    this.anomalyDAO.deleteById(anomalyId);
    return Response.ok().build();
  }

  @GET
  @ApiOperation("get the current time series and predicted baselines for an anomaly within a time range")
  @Path(value = "/predicted-baseline/{anomalyId}")
  public Response getPredictedBaseline(
    @PathParam("anomalyId") @ApiParam("anomalyId") long anomalyId,
      @ApiParam("Start time for the predicted baselines") @QueryParam("start") long start,
      @ApiParam("End time for the predicted baselines") @QueryParam("end") long end,
      @ApiParam("Add padding to the window based on metric granularity") @QueryParam("padding") @DefaultValue("false") boolean padding
  ) throws Exception {
    MergedAnomalyResultDTO anomaly = anomalyDAO.findById(anomalyId);
    if (anomaly == null) {
      throw new IllegalArgumentException(String.format("Could not resolve anomaly id %d", anomalyId));
    }
    if (padding) {
      // add paddings for the time range
      DatasetConfigDTO dataset = this.datasetDAO.findByDataset(anomaly.getCollection());
      if (dataset == null) {
        throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for anomaly id %d", anomaly.getCollection(), anomalyId));
      }
      AnomalyOffset offsets = BaseAnomalyFunction.getDefaultOffsets(dataset);
      DateTimeZone dataTimeZone = DateTimeZone.forID(dataset.getTimezone());
      start = new DateTime(start, dataTimeZone).minus(offsets.getPreOffsetPeriod()).getMillis();
      end = new DateTime(end, dataTimeZone).plus(offsets.getPostOffsetPeriod()).getMillis();
    }
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    DataFrame baselineTimeseries = DetectionUtils.getBaselineTimeseries(anomaly, me.getFilters(), me.getId(),
        configDAO.findById(anomaly.getDetectionConfigId()), start, end, loader, provider).getDataFrame();
    if (!baselineTimeseries.contains(COL_CURRENT)) {
      // add current time series if not exists
      MetricSlice currentSlice = MetricSlice.from(me.getId(), start, end, me.getFilters());
      DataFrame dfCurrent = this.provider.fetchTimeseries(Collections.singleton(currentSlice)).get(currentSlice).renameSeries(COL_VALUE, COL_CURRENT);
      baselineTimeseries = dfCurrent.joinOuter(baselineTimeseries);
    }
    return Response.ok(baselineTimeseries).build();
  }


  @GET
  @Path(value = "/health/{id}")
  @ApiOperation("Get the detection health metrics and statuses for a detection config")
  public Response getDetectionHealth(@PathParam("id") @ApiParam("detection config id") long id,
      @ApiParam("Start time for the the health metric") @QueryParam("start") long start,
      @ApiParam("End time for the the health metric") @QueryParam("end") long end,
      @ApiParam("Max number of detection tasks returned") @QueryParam("limit") @DefaultValue("500") long limit) {
    DetectionHealth health;
    try {
      health = new DetectionHealth.Builder(id, start, end).addRegressionStatus(this.evaluationDAO)
          .addAnomalyCoverageStatus(this.anomalyDAO)
          .addDetectionTaskStatus(this.taskDAO, limit)
          .addOverallHealth()
          .build();
    } catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
    }
    return Response.ok(health).build();
  }

  @POST
  @Path(value = "/re-notify")
  @ApiOperation("Resend the notification for the anomalies to the subscribed notification groups, if the subscription group supports re-notify")
  public Response alert(@QueryParam("id") List<Long> anomalyIds) {
    List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByIds(anomalyIds);
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      AnomalySubscriptionGroupNotificationDTO anomalySubscriptionGroupNotificationDTO =
          new AnomalySubscriptionGroupNotificationDTO();
      anomalySubscriptionGroupNotificationDTO.setAnomalyId(anomaly.getId());
      anomalySubscriptionGroupNotificationDTO.setDetectionConfigId(anomaly.getDetectionConfigId());
      anomalySubscriptionGroupNotificationManager.save(anomalySubscriptionGroupNotificationDTO);
    }
    return Response.ok().build();
  }
}
