package com.linkedin.thirdeye.dashboard.resources;

import io.dropwizard.hibernate.UnitOfWork;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.dashboard.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private QueryCache queryCache;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyResultDAO anomalyResultDAO;
  private EmailConfigurationDAO emailConfigurationDAO;
  private AnomalyDetectionJobManager anomalyDetectionJobManager;

  public AnomalyResource(AnomalyDetectionJobManager anomalyDetectionJobManager,
      AnomalyFunctionSpecDAO anomalyFunctionSpecDAO,
      AnomalyResultDAO anomalyResultDAO,
      EmailConfigurationDAO emailConfigurationDAO) {

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.anomalyDetectionJobManager = anomalyDetectionJobManager;
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.emailConfigurationDAO = emailConfigurationDAO;

  }

  public AnomalyResource() {
  }

  /************** CRUD for anomalies of a collection ********************************************************/

  // View anomalies for collection
  @GET
  @UnitOfWork
  @Path("/anomalies/view")
  public List<AnomalyResult> viewAnomaliesInRange(@QueryParam("dataset") String dataset,
      @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("metric") String metric,
      @QueryParam("dimensions") String dimensions) {

    List<AnomalyResult> anomalyResults = new ArrayList<>();
    List<AnomalyResult> anomalies = null;
    try {
      DateTime endTime = DateTime.now();
      if (StringUtils.isNotEmpty(endTimeIso)) {
        endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
      }
      DateTime startTime = endTime.minusDays(7);
      if (StringUtils.isNotEmpty(startTimeIso)) {
        startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
      }
      System.out.println(dataset + " " + startTimeIso + " " + startTime + " " + endTimeIso + " " + endTime + " " + metric);
      if (StringUtils.isEmpty(metric)) {
        anomalyResults = anomalyResultDAO.findAllByCollectionAndTime(dataset, startTime, endTime);
      } else {
        anomalyResults = anomalyResultDAO.findAllByCollectionTimeAndMetric(dataset, metric, startTime, endTime);
      }
      anomalies = anomalyResults;

      System.out.println(dimensions);
      if (StringUtils.isNotEmpty(dimensions)) {

        dimensions = URLDecoder.decode(dimensions, "UTF-8");
        Multimap<String, String> dimensionsMap = ThirdEyeUtils.convertToMultiMap(dimensions);

        System.out.println(dimensions);
        anomalies = new ArrayList<>();
        for (AnomalyResult anomalyResult : anomalyResults) {
          String filters = anomalyResult.getFilters();
          String dimensionString = anomalyResult.getDimensions();
          if (StringUtils.isNotEmpty(filters) && StringUtils.isNotEmpty(dimensionString)
              && StringUtils.isNotEmpty(dimensionString = dimensionString.replaceAll("\\*|,", ""))) {
            String dimensionName = filters.split("=")[0];
            if (dimensionsMap.containsKey(dimensionName) && dimensionsMap.get(dimensionName).contains(dimensionString)) {
              anomalies.add(anomalyResult);
            }
          }
        }
      }
      System.out.println(anomalies.size());

    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }
    return anomalies;
  }

  /************* CRUD for anomaly functions of collection **********************************************/

  // View all anomaly functions
  @GET
  @UnitOfWork
  @Path("/anomaly-function/view")
  public List<AnomalyFunctionSpec> viewAnomalyFunctions(@QueryParam("dataset") String dataset, @QueryParam("metric") String metric) {

    List<AnomalyFunctionSpec> anomalyFunctionSpecs = anomalyFunctionSpecDAO.findAllByCollection(dataset);

    List<AnomalyFunctionSpec> anomalyFunctions = anomalyFunctionSpecs;

    if (StringUtils.isNotEmpty(metric)) {
      anomalyFunctions = new ArrayList<>();
      for (AnomalyFunctionSpec anomalyFunctionSpec : anomalyFunctionSpecs) {
        if (metric.equals(anomalyFunctionSpec.getMetric())) {
          anomalyFunctions.add(anomalyFunctionSpec);
        }
      }
    }
    return anomalyFunctions;
  }

  // Add anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/create")
  public Response createAnomalyFunction(@QueryParam("dataset") String dataset,
      @QueryParam("functionName") String functionName,
      @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("scheduleStartIso") String scheduleStartIso,
      @QueryParam("repeatEverySize") String repeatEverySize,
      @QueryParam("repeatEveryUnit") String repeatEveryUnit,
      @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("properties") String properties,
      @QueryParam("isActive") boolean isActive)
          throws Exception {

    CollectionSchema schema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(dataset);
    TimeGranularity dataGranularity = schema.getTime().getDataGranularity();

    AnomalyFunctionSpec anomalyFunctionSpec = new AnomalyFunctionSpec();
    anomalyFunctionSpec.setIsActive(isActive);
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setMetric(metric);
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));
    anomalyFunctionSpec.setWindowDelay(Integer.valueOf(windowDelay));
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());
    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setProperties(properties);

    String cron = "";
    if (StringUtils.isNotEmpty(scheduleStartIso)) {
      cron = constructCron(scheduleStartIso, repeatEverySize, repeatEveryUnit);
    }
    anomalyFunctionSpec.setCron(cron);

    Long id = anomalyFunctionSpecDAO.create(anomalyFunctionSpec);

    if (isActive) {
      anomalyDetectionJobManager.start(id);
    }

    return Response.ok(id).build();
  }

  private String constructCron(String scheduleStartIso, String repeatEverySize, String repeatEveryUnit) {

    DateTime scheduleTime = DateTime.now();
    if (StringUtils.isNotEmpty(scheduleStartIso)) {
      scheduleTime = ISODateTimeFormat.dateTimeParser().parseDateTime(scheduleStartIso);
    }
    String minute = "0";
    minute = String.valueOf(scheduleTime.getMinuteOfHour());

    String hour = "*";
    if (repeatEveryUnit.equals(TimeUnit.DAYS)) {
      hour = String.valueOf(scheduleTime.getHourOfDay());
    }

    String cron = String.format("0 %s %s * * ?", minute, hour);
    return cron;
  }

  // Edit anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/update")
  public Response updateAnomalyFunction(@QueryParam("id") Long id, @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric, @QueryParam("type") String type, @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit, @QueryParam("windowDelay") String windowDelay,
      @QueryParam("exploreDimension") String exploreDimension, @QueryParam("properties") String properties) {

    return Response.ok(id).build();
  }

  // Delete anomaly function
  @DELETE
  @UnitOfWork
  @Path("/anomaly-function/delete")
  public Response deleteAnomalyFunctions(@QueryParam("id") Long id, @QueryParam("dataset") String dataset) {

    return Response.noContent().build();
  }

  // Run anomaly function ad hoc
  @POST
  @UnitOfWork
  @Path("/anomaly-function/adhoc")
  public Response runAdhocAnomalyFunctions(@QueryParam("id") Long id, @QueryParam("dataset") String dataset) {

    return Response.noContent().build();
  }

  /*************** CRUD for email functions of collection *********************************************/

  // View all email functions

  // Add email function

  // Edit email function

  // Delete email function

  // Run email function ad hoc

}
