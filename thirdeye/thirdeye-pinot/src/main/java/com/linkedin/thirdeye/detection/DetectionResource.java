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

package com.linkedin.thirdeye.detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobSchedulerUtils;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import com.linkedin.thirdeye.detection.finetune.GridSearchTuningAlgorithm;
import com.linkedin.thirdeye.detection.finetune.TuningAlgorithm;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.wordnik.swagger.annotations.ApiParam;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/detection")
@Produces(MediaType.APPLICATION_JSON)
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

  public DetectionResource() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, timeseriesLoader, aggregationLoader, loader);
  }

  @POST
  @Path("/preview")
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
    config.setProperties(MapUtils.getMap(properties, "properties"));
    config.setComponentSpecs(MapUtils.getMap(properties, "componentSpecs"));

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

    AnomalySlice slice = new AnomalySlice().withStart(start).withEnd(end);

    TuningAlgorithm gridSearch = new GridSearchTuningAlgorithm(OBJECT_MAPPER.writeValueAsString(json.get("properties")), parameters);
    gridSearch.fit(slice, configId);

    return Response.ok(gridSearch.bestDetectionConfig().getProperties()).build();
  }

  @POST
  @Path("/preview/{id}")
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
   * Legacy replay endpoint. Replay all the moving windows within start time and end time.
   * Saves anomaly for each moving window before starting detection for next window.
   * Behaves exactly like the legacy replay endpoint.
   * See also {@link com.linkedin.thirdeye.dashboard.resources.DetectionJobResource#generateAnomaliesInRangeForFunctions(String, String, String, String, Boolean, Boolean)}}
   * @param configId the config id to replay
   * @param start start time
   * @param end end time
   * @param deleteExistingAnomaly (optional) delete existing anomaly or not
   * @param windowSize (optional) override the default window size
   * @param bucketSize (optional) override the default window size
   * @return anomalies
   * @throws Exception
   */
  @POST
  @Path("/legacy-replay/{id}")
  public Response legacyReplay(
      @PathParam("id") long configId,
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("deleteExistingAnomaly") @DefaultValue("false") boolean deleteExistingAnomaly,
      @QueryParam("windowSize") Long windowSize,
      @QueryParam("bucketSize") Long bucketSize) throws Exception {

    DetectionConfigDTO config = this.configDAO.findById(configId);
    if (config == null) {
      throw new IllegalArgumentException(String.format("Cannot find config %d", configId));
    }

    AnomalySlice slice = new AnomalySlice().withStart(start).withEnd(end);
    if (deleteExistingAnomaly) {
      // clear existing anomalies
      Collection<MergedAnomalyResultDTO> existing =
          this.provider.fetchAnomalies(Collections.singleton(slice), configId).get(slice);

      List<Long> existingIds = new ArrayList<>();
      for (MergedAnomalyResultDTO anomaly : existing) {
        existingIds.add(anomaly.getId());
      }
      this.anomalyDAO.deleteByIds(existingIds);
    }

    // execute replay
    List<Interval> monitoringWindows = getReplayMonitoringWindows(config, start, end, windowSize, bucketSize);
    for (Interval monitoringWindow : monitoringWindows){
      DetectionPipeline pipeline = this.loader.from(this.provider, config, monitoringWindow.getStartMillis(), monitoringWindow.getEndMillis());
      DetectionPipelineResult result = pipeline.run();

      // save state
      if (result.getLastTimestamp() > 0) {
        config.setLastTimestamp(result.getLastTimestamp());
      }

      this.configDAO.update(config);

      for (MergedAnomalyResultDTO anomaly : result.getAnomalies()) {
        anomaly.setAnomalyResultSource(AnomalyResultSource.ANOMALY_REPLAY);
        this.anomalyDAO.save(anomaly);
      }
    }

    Collection<MergedAnomalyResultDTO> replayResult = this.provider.fetchAnomalies(Collections.singleton(slice), configId).get(slice);

    LOG.info("replay detection pipeline {} generated {} anomalies.", config.getId(), replayResult.size());
    return Response.ok(replayResult).build();
  }

  /**
   * Replay for a given time range. Without cron schedule behavior
   */
  @POST
  @Path("/replay/{id}")
  public Response detectionReplay(
      @PathParam("id") long configId,
      @QueryParam("start") long start,
      @QueryParam("end") long end) throws Exception {

    DetectionConfigDTO config = this.configDAO.findById(configId);
    if (config == null) {
      throw new IllegalArgumentException(String.format("Cannot find config %d", configId));
    }

    // clear existing anomalies
    AnomalySlice slice = new AnomalySlice().withStart(start).withEnd(end);
    Collection<MergedAnomalyResultDTO> existing = this.provider.fetchAnomalies(Collections.singleton(slice), configId).get(slice);

    List<Long> existingIds = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : existing) {
      existingIds.add(anomaly.getId());
    }

    this.anomalyDAO.deleteByIds(existingIds);

    // execute replay
    DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
    DetectionPipelineResult result = pipeline.run();

    // save state
    if (result.getLastTimestamp() > 0) {
      config.setLastTimestamp(result.getLastTimestamp());
    }

    this.configDAO.update(config);

    for (MergedAnomalyResultDTO anomaly : result.getAnomalies()) {
      anomaly.setAnomalyResultSource(AnomalyResultSource.ANOMALY_REPLAY);
      this.anomalyDAO.save(anomaly);
    }

    return Response.ok(result).build();
  }
}
