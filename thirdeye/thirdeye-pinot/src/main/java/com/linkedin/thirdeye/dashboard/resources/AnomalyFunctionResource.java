package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
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
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("thirdeye/function")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionResource.class);

  private final Map<String, Object> anomalyFunctionMetadata = new HashMap<>();
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  public AnomalyFunctionResource(String functionConfigPath) {
    buildFunctionMetadata(functionConfigPath);
    this.anomalyFunctionFactory = new AnomalyFunctionFactory(functionConfigPath);
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
        AnomalyFunction anomalyFunction = clz.newInstance();
        anomalyFunctionMetadata.put(functionName, getFunctionProps.invoke(anomalyFunction));
      } catch (ClassNotFoundException e) {
        LOG.warn("Unknown class for function : " + functionName);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        LOG.error("Unknown method", e);
      } catch (InstantiationException e) {
        LOG.error("Unsupported anomaly function", e);
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
  public Response analyze(AnomalyFunctionDTO anomalyFunctionSpec,
      @QueryParam("startTime") Long startTime, @QueryParam("endTime") Long endTime)
      throws Exception {
    // TODO: replace this with Job/Task framework and job tracker page
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    DateTime windowStart = new DateTime(startTime);
    DateTime windowEnd = new DateTime(endTime);

    AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
        new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
    anomalyDetectionInputContextBuilder
        .setFunction(anomalyFunctionSpec)
        .fetchTimeSeriesData(windowStart, windowEnd)
        .fetchScalingFactors(windowStart, windowEnd);
    AnomalyDetectionInputContext anomalyDetectionInputContext = anomalyDetectionInputContextBuilder.build();

    Map<DimensionMap, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
        anomalyDetectionInputContext.getDimensionMapMetricTimeSeriesMap();

    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();
    List<RawAnomalyResultDTO> results = new ArrayList<>();

    for (Map.Entry<DimensionMap, MetricTimeSeries> entry : dimensionKeyMetricTimeSeriesMap.entrySet()) {
      DimensionMap dimensionMap = entry.getKey();
      if (entry.getValue().getTimeWindowSet().size() < 2) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", dimensionMap);
        continue;
      }
      try {
        // Run algorithm
        MetricTimeSeries metricTimeSeries = entry.getValue();
        LOG.info("Analyzing anomaly function with dimensionKey: {}, windowStart: {}, windowEnd: {}",
            dimensionMap, startTime, endTime);

        List<RawAnomalyResultDTO> resultsOfAnEntry = anomalyFunction
            .analyze(dimensionMap, metricTimeSeries, new DateTime(startTime), new DateTime(endTime),
                new ArrayList<MergedAnomalyResultDTO>());
        if (resultsOfAnEntry.size() != 0) {
          results.addAll(resultsOfAnEntry);
        }
        LOG.info("{} has {} anomalies in window {} to {}", dimensionMap, resultsOfAnEntry.size(),
            new DateTime(startTime), new DateTime(endTime));
      } catch (Exception e) {
        LOG.error("Could not compute for {}", dimensionMap, e);
      }
    }
    if (results.size() > 0) {
      List<RawAnomalyResultDTO> validResults = new ArrayList<>();
      for (RawAnomalyResultDTO anomaly : results) {
        if (!anomaly.isDataMissing()) {
          LOG.info("Found anomaly, sev [{}] start [{}] end [{}]", anomaly.getWeight(),
              new DateTime(anomaly.getStartTime()), new DateTime(anomaly.getEndTime()));
          validResults.add(anomaly);
        }
      }
      anomalyResults.addAll(validResults);
    }
    return Response.ok(anomalyResults).build();
  }
}
