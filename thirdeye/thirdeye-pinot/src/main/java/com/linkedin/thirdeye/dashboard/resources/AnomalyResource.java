package com.linkedin.thirdeye.dashboard.resources;

import io.dropwizard.hibernate.UnitOfWork;

import java.util.ArrayList;
import java.util.List;
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

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;

@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private QueryCache queryCache;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyFunctionRelationDAO anomalyFunctionRelationDAO;
  private AnomalyResultDAO anomalyResultDAO;
  private EmailConfigurationDAO emailConfigurationDAO;

  public AnomalyResource(AnomalyFunctionSpecDAO anomalyFunctionSpecDAO,
      AnomalyFunctionRelationDAO anomalyFunctionRelationDAO, AnomalyResultDAO anomalyResultDAO,
      EmailConfigurationDAO emailConfigurationDAO) {

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
    this.anomalyFunctionRelationDAO = anomalyFunctionRelationDAO;
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
  public List<AnomalyResult> viewAnomaliesInRange(@QueryParam("collection") String collection,
      @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("filters") String filterJson,
      @QueryParam("metrics") String metrics) {

    List<AnomalyResult> anomalyResults = new ArrayList<>();
    DateTime endTime = DateTime.now();
    DateTime startTime = endTime.minusDays(7);
    anomalyResults =  anomalyResultDAO.findAllByCollectionAndTime("thirdeyeAbook", startTime, endTime);

    return anomalyResults;
  }

  // Delete anomalies for a collection
  @DELETE
  @UnitOfWork
  @Path("/anomalies/delete")
  public Response clearAnomaliesInRange(@QueryParam("collection") String collection,
      @QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime) {

    return Response.noContent().build();
  }

  /************* CRUD for anomaly functions of collection **********************************************/

  // View all anomaly functions
  @GET
  @UnitOfWork
  @Path("/anomaly-function/view")
  public List<AnomalyFunctionSpec> viewAnomalyFunctions(@QueryParam("collection") String collection) {

    List<AnomalyFunctionSpec> anomalyFunctionSpec = new ArrayList<>();
    anomalyFunctionSpec = anomalyFunctionSpecDAO.findAllByCollection("thirdeyeAbook");
    return anomalyFunctionSpec;
  }

  // Add anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/create")
  public Response createAnomalyFunction(@QueryParam("collection") String collection,
      @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("scheduleStartIso") String scheduleStartIso,
      @QueryParam("repeatEverySize") String repeatEverySize,
      @QueryParam("repeatEveryUnit") String repeatEveryUnit,
      @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("properties") String properties)
          throws ExecutionException {

    CollectionSchema schema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(collection);
    TimeGranularity dataGranularity = schema.getTime().getDataGranularity();

    AnomalyFunctionSpec anomalyFunctionSpec = new AnomalyFunctionSpec();

    anomalyFunctionSpec.setIsActive(true);
    anomalyFunctionSpec.setCollection(collection);
    anomalyFunctionSpec.setMetric(metric);
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));
    anomalyFunctionSpec.setWindowDelay(Integer.valueOf(windowDelay));
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());
    String cron = constructCron(scheduleStartIso, repeatEverySize, repeatEveryUnit);
    anomalyFunctionSpec.setCron(cron);
    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setProperties(properties);


    Long id = 101L;
    return Response.ok(id).build();
  }

  private String constructCron(String scheduleStartIso, String repeatEverySize, String repeatEveryUnit) {
    String cron = "30 * * * * ?";
    return cron;
  }

  // Edit anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/update")
  public Response updateAnomalyFunction(@QueryParam("id") Long id, @QueryParam("collection") String collection,
      @QueryParam("metric") String metric, @QueryParam("type") String type, @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit, @QueryParam("windowDelay") String windowDelay,
      @QueryParam("exploreDimension") String exploreDimension, @QueryParam("properties") String properties) {

    return Response.ok(id).build();
  }

  // Delete anomaly function
  @DELETE
  @UnitOfWork
  @Path("/anomaly-function/delete")
  public Response deleteAnomalyFunctions(@QueryParam("id") Long id, @QueryParam("collection") String collection) {

    return Response.noContent().build();
  }

  // Run anomaly function ad hoc
  @POST
  @UnitOfWork
  @Path("/anomaly-function/adhoc")
  public Response runAdhocAnomalyFunctions(@QueryParam("id") Long id, @QueryParam("collection") String collection) {

    return Response.noContent().build();
  }

  /*************** CRUD for email functions of collection *********************************************/

  // View all email functions

  // Add email function

  // Edit email function

  // Delete email function

  // Run email function ad hoc

}
