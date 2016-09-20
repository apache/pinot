package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;

@Path("thirdeye/function")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionResource.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();
  private static final TimeSeriesResponseConverter timeSeriesResponseConverter =
      TimeSeriesResponseConverter.getInstance();

  private final Map<String, Object> anomalyFunctionMetadata = new HashMap<>();
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  public AnomalyFunctionResource(ThirdEyeDashboardConfiguration configuration) {
    buildFunctionMetadata(configuration.getFunctionConfigPath());
    this.anomalyFunctionFactory = new AnomalyFunctionFactory(configuration.getFunctionConfigPath());
  }

  private void buildFunctionMetadata(String functionConfigPath) {
    Properties props = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(functionConfigPath);
      props.load(input);
    } catch (IOException e) {
      LOG.error("Function config not found at {}", functionConfigPath);
    } finally {
      IOUtils.closeQuietly(input);
    }
    LOG.info("Loaded functions : " + props.keySet() + " from path : " + functionConfigPath);
    for (Object key : props.keySet()) {
      String functionName = key.toString();
      try {
        Class<AnomalyFunction> clz = (Class<AnomalyFunction>) Class.forName(props.get(functionName).toString());
        Method getFunctionProps = clz.getMethod("getPropertyKeys");
        anomalyFunctionMetadata.put(functionName, getFunctionProps.invoke(null));
      } catch (ClassNotFoundException e) {
        LOG.warn("Unknown class for function : " + functionName);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        LOG.error("Unknown method", e);
      }
    }
  }

  /**
   * @return map of function name vs function property keys
   * <p/>
   * eg. { "WEEK_OVER_WEEK_RULE":["baseline","changeThreshold","averageVolumeThreshold"],
   * "MIN_MAX_THRESHOLD":["min","max"] }
   */
  @GET
  @Path("metadata")
  public Map<String, Object> getAnomalyFunctionMetadata() {
    return anomalyFunctionMetadata;
  }

  /**
   * @return List of metric functions
   * <p/>
   * eg. ["SUM","AVG","COUNT"]
   */
  @GET
  @Path("metric-function")
  public MetricAggFunction[] getMetricFunctions() {
    return MetricAggFunction.values();
  }


  @POST
  @Path("/analyze")
  @Consumes(MediaType.APPLICATION_JSON)
  public List<MergedAnomalyResultDTO> analyze(AnomalyFunctionDTO anomalyFunctionSpec,
      @QueryParam("startTime") Long startTime, @QueryParam("endTime") Long endTime)
      throws Exception {
    // TODO: replace this with Job/Task framework and job tracker page
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    TimeSeriesResponse finalResponse = TimeSeriesUtil
        .getTimeSeriesResponse(anomalyFunctionSpec, anomalyFunction,
            anomalyFunctionSpec.getExploreDimensions(), startTime, endTime);

    List<MergedAnomalyResultDTO> mergedAnomalyResults = new ArrayList<>();
    List<RawAnomalyResultDTO> results = new ArrayList<>();
    CollectionSchema collectionSchema;
    List<String> collectionDimensions;

    try {
      collectionSchema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache()
          .get(anomalyFunctionSpec.getCollection());
      collectionDimensions = collectionSchema.getDimensionNames();
    } catch (Exception e) {
      LOG.error("Exception when reading collection schema cache", e);
      return mergedAnomalyResults;
    }

    Map<DimensionKey, MetricTimeSeries> res =
        timeSeriesResponseConverter.toMap(finalResponse, collectionDimensions);
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
            dimensionKey, startTime, endTime);

        results = anomalyFunction
            .analyze(dimensionKey, metricTimeSeries, new DateTime(startTime), new DateTime(endTime),
                new ArrayList<>());

        LOG.info("{} has {} anomalies in window {} to {}", entry.getKey(), results.size(),
            startTime, endTime);
      } catch (Exception e) {
        LOG.error("Could not compute for {}", entry.getKey(), e);
      }
    }
    if (results.size() > 0) {
      List<RawAnomalyResultDTO> validResults = new ArrayList<>();
      for (RawAnomalyResultDTO anomaly : results) {
        if (!anomaly.isDataMissing()) {
          validResults.add(anomaly);
        }
      }

      mergedAnomalyResults
          .addAll(AnomalyTimeBasedSummarizer.mergeAnomalies(validResults, -1, 2 * 60 * 60 * 1000));
      LOG.info("Merging [{}] anomalies to [{}] for preview", validResults.size(),
          mergedAnomalyResults.size());
    }
    return mergedAnomalyResults;
  }
}
