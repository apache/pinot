package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;

import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.*;


@Path("dashboard/anomaly-function")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionResource.class);

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<String, Object> anomalyFunctionMetadata = new HashMap<>();
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private AutotuneConfigManager autotuneConfigDAO;
  private EmailConfigurationManager emailConfigurationDAO;

  private static final String DEFAULT_CRON = "0 0 0 * * ?";
  private static final String UTF8 = "UTF-8";
  private static final String DEFAULT_FUNCTION_TYPE = "WEEK_OVER_WEEK_RULE";

  public AnomalyFunctionResource(String functionConfigPath) {
    buildFunctionMetadata(functionConfigPath);
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.rawAnomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.autotuneConfigDAO = DAO_REGISTRY.getAutotuneConfigDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getEmailConfigurationDAO();
    this.anomalyFunctionFactory = new AnomalyFunctionFactory(functionConfigPath);
  }

  // Partially update anomaly function

  /**
   * Update the properties of the given anomaly function id
   * @param id
   *    The id of the given anomaly function
   * @param propertiesJson
   *    The json string defining the function properties to be updated, ex. {"pValueThreshold":"0.01",...}
   * @return
   *    OK if the properties are successfully updated
   */
  @PUT
  @Path("/update")
  public Response updateAnomalyFunctionProperties (
      @QueryParam("id") @NotNull Long id,
      @QueryParam("config") @NotNull String propertiesJson) {
    if(id == null || anomalyFunctionDAO.findById(id) == null) {
      String msg = "Unable to update function properties. " + id + " doesn't exist";
      LOG.warn(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
    }

    if(StringUtils.isNotBlank(propertiesJson)) {
      Map<String, String> configs = Collections.emptyMap();
      try {
        configs = OBJECT_MAPPER.readValue(propertiesJson, Map.class);
      } catch (IOException e) {
        String msg = "Unable to parse json string " + propertiesJson + " for function " + id;
        LOG.error(msg);
        return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
      }
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
      anomalyFunction.updateProperties(configs);
      anomalyFunctionDAO.update(anomalyFunction);
    }
    String msg = "Successfully update properties for function " + id + " with " + propertiesJson;
    LOG.info(msg);
    return Response.ok(id).build();
  }

  // Activate anomaly function
  @POST
  @Path("/anomaly-function/activate")
  public Response activateAnomalyFunction(@NotNull @QueryParam("functionId") Long id) {
    if (id == null) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    toggleFunctionById(id, true);
    return Response.ok(id).build();
  }

  @DELETE
  @Path("/anomaly-function/activate")
  public Response deactivateAnomalyFunction(@NotNull @QueryParam("functionId") Long id) {
    if (id == null) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    toggleFunctionById(id, false);
    return Response.ok(id).build();
  }

  // batch activate and deactivate anomaly functions
  @POST
  @Path("/activate/batch")
  public String activateFunction(@QueryParam("functionIds") String functionIds) {
    toggleFunctions(functionIds, true);
    return functionIds;
  }

  @POST
  @Path("/deactivate/batch")
  public String deactivateFunction(@QueryParam("functionIds") String functionIds) {
    toggleFunctions(functionIds, false);
    return functionIds;
  }

  /**
   * toggle anomaly functions to active and inactive
   *
   * @param functionIds string comma separated function ids, ALL meaning all functions
   * @param isActive boolean true or false, set function as true or false
   */
  private void toggleFunctions(String functionIds, boolean isActive) {
    List<Long> functionIdsList = new ArrayList<>();

    // can add tokens here to activate and deactivate all functions for example
    // functionIds == {SPECIAL TOKENS} --> functionIdsList = anomalyFunctionDAO.findAll()

    if (StringUtils.isNotBlank(functionIds)) {
      String[] tokens = functionIds.split(",");
      for (String token : tokens) {
        functionIdsList.add(Long.valueOf(token));  // unhandled exception is expected
      }
    }

    for (long id : functionIdsList) {
      toggleFunctionById(id, isActive);
    }
  }


  private void toggleFunctionById(long id, boolean isActive) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    anomalyFunctionSpec.setActive(isActive);
    anomalyFunctionDAO.update(anomalyFunctionSpec);
  }

  // Delete anomaly function
  @DELETE
  @Path("/delete")
  public Response deleteAnomalyFunctions(@NotNull @QueryParam("id") Long id,
      @QueryParam("functionName") String functionName)
      throws IOException {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }

    // call endpoint to shutdown if active
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("No anomalyFunctionSpec with id " + id);
    }

    // delete dependent entities
    // email config mapping
    List<EmailConfigurationDTO> emailConfigurations = emailConfigurationDAO.findByFunctionId(id);
    for (EmailConfigurationDTO emailConfiguration : emailConfigurations) {
      emailConfiguration.getFunctions().remove(anomalyFunctionSpec);
      emailConfigurationDAO.update(emailConfiguration);
    }

    // raw result mapping
    List<RawAnomalyResultDTO> rawResults =
        rawAnomalyResultDAO.findAllByTimeAndFunctionId(0, System.currentTimeMillis(), id);
    for (RawAnomalyResultDTO result : rawResults) {
      rawAnomalyResultDAO.delete(result);
    }

    // merged anomaly mapping
    List<MergedAnomalyResultDTO> mergedResults = mergedAnomalyResultDAO.findByFunctionId(id, true);
    for (MergedAnomalyResultDTO result : mergedResults) {
      mergedAnomalyResultDAO.delete(result);
    }

    // delete from db
    anomalyFunctionDAO.deleteById(id);
    return Response.noContent().build();
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
  @Path("/metadata")
  public Map<String, Object> getAnomalyFunctionMetadata() {
    return anomalyFunctionMetadata;
  }

  /**
   * @return List of metric functions
   * <p/>
   * eg. ["SUM","AVG","COUNT"]
   */
  @GET
  @Path("/metric-function")
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
