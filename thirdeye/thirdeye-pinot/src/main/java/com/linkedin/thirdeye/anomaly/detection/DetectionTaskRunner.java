package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.constant.MetricAggFunction;

import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.context.internal.ManagedSessionContext;
import org.joda.time.DateTime;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionTaskRunner.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private QueryCache queryCache;
  private TimeSeriesHandler timeSeriesHandler;
  private TimeSeriesResponseConverter timeSeriesResponseConverter;

  private AnomalyResultDAO resultDAO;
  private AnomalyFunctionRelationDAO relationDAO;
  private SessionFactory sessionFactory;

  private String collection;
  private List<String> collectionDimensions;
  private MetricFunction metricFunction;
  private DateTime windowStart;
  private DateTime windowEnd;
  private List<AnomalyResult> knownAnomalies;
  private int anomalyCounter;
  private AnomalyFunction anomalyFunction;
  private AnomalyFunctionSpec anomalyFunctionSpec;
  private AnomalyFunctionFactory anomalyFunctionFactory;

  public DetectionTaskRunner() {
    queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    timeSeriesHandler = new TimeSeriesHandler(queryCache);
    timeSeriesResponseConverter = TimeSeriesResponseConverter.getInstance();
  }

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {

    DetectionTaskInfo detectionTaskInfo = (DetectionTaskInfo) taskInfo;
    List<TaskResult> taskResult = new ArrayList<>();
    LOG.info("Begin executing task {}", taskInfo);
    resultDAO = taskContext.getResultDAO();
    relationDAO = taskContext.getRelationDAO();
    sessionFactory = taskContext.getSessionFactory();
    anomalyFunctionFactory = taskContext.getAnomalyFunctionFactory();

    anomalyFunctionSpec = detectionTaskInfo.getAnomalyFunctionSpec();
    anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    windowStart = detectionTaskInfo.getWindowStartTime();
    windowEnd = detectionTaskInfo.getWindowEndTime();

    // Compute metric function
    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());
    // TODO put sum into the function config
    metricFunction = new MetricFunction(MetricAggFunction.SUM, anomalyFunctionSpec.getMetric());

    // Collection
    collection = anomalyFunctionSpec.getCollection();

    // Filters
    String filters = anomalyFunctionSpec.getFilters();

    LOG.info("Running anomaly detection job with metricFunction: {}, collection: {}",
        metricFunction, collection);

    CollectionSchema collectionSchema = null;
    try {
      collectionSchema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(collection);
    } catch (Exception e) {
      LOG.error("Exception when reading collection schema cache", e);
    }
    collectionDimensions = collectionSchema.getDimensionNames();

    // Get existing anomalies for this time range
    knownAnomalies = getExistingAnomalies();

    // Seed request with top-level...
    TimeSeriesRequest topLevelRequest = new TimeSeriesRequest();
    topLevelRequest.setCollectionName(collection);
    List<MetricFunction> metricFunctions = Collections.singletonList(metricFunction);
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metricFunctions);
    topLevelRequest.setMetricExpressions(metricExpressions);
    topLevelRequest.setAggregationTimeGranularity(timeGranularity);
    topLevelRequest.setStart(windowStart);
    topLevelRequest.setEnd(windowEnd);
    topLevelRequest.setEndDateInclusive(false);
    if (StringUtils.isNotBlank(filters)) {
      topLevelRequest.setFilterSet(ThirdEyeUtils.getFilterSet(filters));
    }
    String exploreDimension = detectionTaskInfo.getGroupByDimension();
    if (StringUtils.isNotBlank(exploreDimension)) {
      topLevelRequest
          .setGroupByDimensions(Collections.singletonList(detectionTaskInfo.getGroupByDimension()));
    }

    LOG.info(
        "Running anomaly detection job with windowStartProp: {}, windowEndProp: {}, metricExpressions: {}, timeGranularity: {}, windowStart: {}, windowEnd: {}",
        windowStart, windowEnd, metricExpressions, timeGranularity);

    List<AnomalyResult> results = exploreCombination(topLevelRequest);
    LOG.info("{} anomalies found in total", anomalyCounter);

    return taskResult;
  }

  private List<AnomalyResult> exploreCombination(TimeSeriesRequest request) throws Exception {
    LOG.info("Exploring {}", request);
    List<AnomalyResult> results = null;

    // Query server
    TimeSeriesResponse response;
    try {
      LOG.debug("Executing {}", request);
      response = timeSeriesHandler.handle(request);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
    Map<DimensionKey, MetricTimeSeries> res =
        timeSeriesResponseConverter.toMap(response, collectionDimensions);

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      if (entry.getValue().getTimeWindowSet().size() < 2) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", entry.getKey());
        continue;
      }

      try {
        // Run algorithm
        DimensionKey dimensionKey = entry.getKey();
        MetricTimeSeries metricTimeSeries = entry.getValue();
        LOG.info("Analyzing anomaly function with dimensionKey: {}, windowStart: {}, windowEnd: {}",
            dimensionKey, windowStart, windowEnd);

        results = anomalyFunction.analyze(dimensionKey, metricTimeSeries, windowStart, windowEnd,
            knownAnomalies);

        // Handle results
        handleResults(results);

        // Remove any known anomalies
        results.removeAll(knownAnomalies);

        LOG.info("{} has {} anomalies in window {} to {}", entry.getKey(), results.size(),
            windowStart, windowEnd);
        anomalyCounter += results.size();
      } catch (Exception e) {
        LOG.error("Could not compute for {}", entry.getKey(), e);
      }
    }
    return results;
  }

  private List<AnomalyResult> getExistingAnomalies() {
    List<AnomalyResult> results = new ArrayList<>();

    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);

      // The ones for this function
      results.addAll(resultDAO.findAllByCollectionTimeAndFunction(collection, windowStart,
          windowEnd, anomalyFunction.getSpec().getId()));

      // The ones for any related functions
      List<AnomalyFunctionRelation> relations =
          relationDAO.findByParent(anomalyFunction.getSpec().getId());
      for (AnomalyFunctionRelation relation : relations) {
        results.addAll(resultDAO.findAllByCollectionTimeAndFunction(collection, windowStart,
            windowEnd, relation.getChildId()));
      }
    } catch (Exception e) {
      LOG.error("Exception in getting existing anomalies", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return results;
  }

  private void handleResults(List<AnomalyResult> results) {

      for (AnomalyResult result : results) {
        try {
          // Properties that always come from the function spec
          AnomalyFunctionSpec spec = anomalyFunction.getSpec();

          // make sure score and weight are valid numbers
          result.setScore(normalize(result.getScore()));
          result.setWeight(normalize(result.getWeight()));
          resultDAO.save(result);

          // now save the function entity and update result
          result.setFunction(spec);
          resultDAO.update(result);
        } catch (Exception e) {
          LOG.error("Exception in saving anomaly result : " + result.toString(), e);
        }
      }
  }

  /** Handle any infinite or NaN values by replacing them with +/- max value or 0 */
  private double normalize(double value) {
    if (Double.isInfinite(value)) {
      return (value > 0.0 ? 1 : -1) * Double.MAX_VALUE;
    } else if (Double.isNaN(value)) {
      return 0.0; // default?
    } else {
      return value;
    }
  }
}
