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

package com.linkedin.thirdeye.dashboard.resources;

import com.codahale.metrics.Counter;
import com.google.common.base.CaseFormat;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardResource;
import com.linkedin.thirdeye.anomaly.onboard.tasks.DefaultDetectionOnboardJob;
import com.linkedin.thirdeye.auto.onboard.AutoOnboardUtility;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.onboard.tasks.FunctionCreationOnboardingTask.*;
import static com.linkedin.thirdeye.dashboard.resources.EntityManagerResource.*;


@Path("/onboard")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OnboardResource {
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private MetricConfigManager metricConfigDAO;
  private DatasetConfigManager datasetFunctionDAO;
  private AlertConfigManager emailConfigurationDAO;
  private TaskManager taskDAO;
  private ThirdEyeDashboardConfiguration config;

  private static final String DEFAULT_FUNCTION_PREFIX = "thirdEyeAutoOnboard_";
  private static final String DEFAULT_ALERT_GROUP = "te_bulk_onboard_alerts";
  private static final String DEFAULT_ALERT_GROUP_APPLICATION = "others";
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(OnboardResource.class);

  public OnboardResource(ThirdEyeDashboardConfiguration config) {
    this.config = config;
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.datasetFunctionDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.taskDAO = DAO_REGISTRY.getTaskDAO();
  }

  public OnboardResource(AnomalyFunctionManager anomalyFunctionManager,
                         MergedAnomalyResultManager mergedAnomalyResultManager) {
    this.anomalyFunctionDAO = anomalyFunctionManager;
    this.mergedAnomalyResultDAO = mergedAnomalyResultManager;
  }

  /**
   * Endpoint for bulk onboarding of metrics
   *
   * This endpoint will create anomaly functions for all the metrics under the given tag
   * and also has the ability to auto create alert config groups with dataset owners as
   * recipients and send out email alerts.
   *
   * @param tag the tag belonging to the metrics which you would like to onboard
   * @param dataset the dataset belonging to the metrics which you would like to onboard
   * @param functionPrefix (optional, DEFAULT_FUNCTION_PREFIX) a custom anomaly function prefix
   * @param forceSyncAlertGroup (optional, true) force create alert groups based on dataset owners
   * @param alertGroupName (optional) subscribe to a custom subscription alert group
   * @param application (optional) the application to which this alert belongs to.
   * @return HTTP response containing onboard statistics and warnings
   */
  @POST
  @Path("/bulk-onboard")
  @ApiOperation("Endpoint used for bulk on-boarding alerts leveraging the create-job endpoint.")
  public Response bulkOnboardAlert(
      @QueryParam("tag") String tag,
      @QueryParam("dataset") String dataset,
      @DefaultValue(DEFAULT_FUNCTION_PREFIX) @QueryParam("functionPrefix") String functionPrefix,
      @DefaultValue("true") @QueryParam("forceSyncAlertGroup") boolean forceSyncAlertGroup,
      @QueryParam("alertGroupName") String alertGroupName, @QueryParam("alertGroupCron") String alertGroupCron,
      @QueryParam("application") String application,
      @QueryParam("sleep") Long sleep)
      throws Exception {
    Map<String, String> responseMessage = new HashMap<>();
    Counter counter = new Counter();

    if (StringUtils.isBlank(tag) && StringUtils.isBlank(dataset)) {
      responseMessage.put("message", "Must provide either tag or dataset");
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }

    AlertConfigDTO alertConfigDTO = null;
    if (StringUtils.isNotEmpty(alertGroupName)) {
      alertConfigDTO = emailConfigurationDAO.findWhereNameEquals(alertGroupName);
      if (alertConfigDTO == null) {
        responseMessage.put("message", "cannot find an alert group with name " + alertGroupName + ".");
        return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
      }
    }

    List<MetricConfigDTO> metrics = new ArrayList<>();

    if (StringUtils.isNotBlank(tag)) {
      List<MetricConfigDTO> tagMetrics = fetchMetricsByTag(tag);
      LOG.info("Number of metrics with tag {} fetched is {}.", tag, tagMetrics.size());
      metrics.addAll(tagMetrics);
    }

    if (StringUtils.isNotBlank(dataset)) {
      List<MetricConfigDTO> datasetMetrics = fetchMetricsByDataset(dataset);
      LOG.info("Number of metrics with dataset {} fetched is {}.", dataset, datasetMetrics.size());
      metrics.addAll(datasetMetrics);
    }

    // For each metric create a new anomaly function & replay it
    List<Long> ids = new ArrayList<>();
    for (MetricConfigDTO metric : metrics) {
      String functionName = functionPrefix
          + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, metric.getName()) + "_"
          + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, metric.getDataset());

      LOG.info("[bulk-onboard] Onboarding anomaly function {}.", functionName);

      if (alertConfigDTO == null) {
        alertConfigDTO = getAlertConfigGroupForMetric(metric, forceSyncAlertGroup, application, alertGroupCron);
        if (alertConfigDTO == null) {
          responseMessage.put("message", "cannot find an alert group for metric " + metric.getName() + ".");
          return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
        }
      }

      AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findWhereNameEquals(functionName);
      if (anomalyFunctionDTO != null) {
        LOG.error("[bulk-onboard] Anomaly function {} already exists.", anomalyFunctionDTO.getFunctionName());
        responseMessage.put("metric " + metric.getName(), "skipped! Anomaly function "
            + anomalyFunctionDTO.getFunctionName() + " already exists.");
        continue;
      }

      try {
        Map<String, String> properties = new HashMap<>();
        properties.put(DefaultDetectionOnboardJob.FUNCTION_NAME, functionName);
        properties.put(DefaultDetectionOnboardJob.METRIC_NAME, metric.getName());
        properties.put(DefaultDetectionOnboardJob.COLLECTION_NAME, metric.getDataset());
        properties.put("alertId", alertConfigDTO.getId().toString());
        String propertiesJson = OBJECT_MAPPER.writeValueAsString(properties);
        DetectionOnboardResource detectionOnboardResource = new DetectionOnboardResource(taskDAO, anomalyFunctionDAO);
        detectionOnboardResource.createDetectionOnboardingJob(functionName, propertiesJson);
        long functionId = anomalyFunctionDAO.findWhereNameEquals(functionName).getId();
        ids.add(functionId);
        subscribeAlertGroupToFunction(alertConfigDTO, functionId);
        responseMessage.put("metric " + metric.getName(), "success! onboarded and added function id " + functionId
            + " to subscription alertGroup = " + alertConfigDTO.getName());
        counter.inc();

        if (sleep != null) {
          Thread.sleep(sleep);
        }

      } catch (InterruptedException e) {
        responseMessage.put("message", "Operation interrupted");
        return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();

      } catch (Exception e) {
        LOG.error("[bulk-onboard] There was an exception onboarding metric {} function {}.", metric, functionName, e);
        responseMessage.put("skipped " + metric.getName(), "Exception onboarding metric : " + e);
      }
    }

    responseMessage.put("message", "successfully onboarded " + counter.getCount() + " metrics with function ids " + ids);
    return Response.ok(responseMessage).build();
  }

  private void subscribeAlertGroupToFunction(AlertConfigDTO alertConfigDTO, long functionId) {
    if (alertConfigDTO.getEmailConfig() == null) {
      AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
      List<Long> functionIds = new ArrayList<>();
      functionIds.add(functionId);
      emailConfig.setFunctionIds(functionIds);
      alertConfigDTO.setEmailConfig(emailConfig);
    } else {
      alertConfigDTO.getEmailConfig().getFunctionIds().add(functionId);
    }
    emailConfigurationDAO.update(alertConfigDTO);
  }

  private AlertConfigDTO getAlertConfigGroupForMetric(MetricConfigDTO metric, boolean forceSyncAlertGroup,
      String application, String cron) {
    String alertGroupName = AutoOnboardUtility.getAutoAlertGroupName(metric.getDataset());
    AlertConfigDTO alertConfigDTO = emailConfigurationDAO.findWhereNameEquals(alertGroupName);

    if (forceSyncAlertGroup) {
      syncAlertConfig(alertConfigDTO, alertGroupName, metric, application, cron);
      alertConfigDTO = emailConfigurationDAO.findWhereNameEquals(alertGroupName);
    } else {
      if (alertConfigDTO == null) {
        LOG.warn("Cannot find alert group {} corresponding to dataset {} for metric {}. Loading default alert group {}",
            alertGroupName, metric.getDataset(), metric.getName(), DEFAULT_ALERT_GROUP);
        alertConfigDTO = emailConfigurationDAO.findWhereNameEquals(DEFAULT_ALERT_GROUP);
      }
    }

    return alertConfigDTO;
  }

  private void syncAlertConfig(AlertConfigDTO alertConfigDTO, String alertGroupName, MetricConfigDTO metric,
      String application, String cron) {
    Set<String> metricOwners = getOwners(metric);
    if (alertConfigDTO == null) {
      createAlertConfig(alertGroupName, application, cron, metricOwners);
    } else {
      // Note: Since we support only one subscription group per function in the legacy code, we append
      // dataset owners and interested stakeholders to the same auto created subscription group.
      // Side effect:
      // If a dataset owner is removed at source, which rarely is the case, then we will continue to
      // retain the owner in our subscription group and send him alerts unless manually removed.
      if (alertConfigDTO.getReceiverAddresses() == null) {
        alertConfigDTO.setReceiverAddresses(new DetectionAlertFilterRecipients(metricOwners));
      } else if (alertConfigDTO.getReceiverAddresses().getTo() == null) {
        alertConfigDTO.getReceiverAddresses().setTo(metricOwners);
      } else {
        alertConfigDTO.getReceiverAddresses().getTo().addAll(metricOwners);
      }
      this.emailConfigurationDAO.update(alertConfigDTO);
      LOG.info("Alert config {} with id {} has been updated.", alertConfigDTO.getName(), alertConfigDTO.getId());
    }
  }

  private Set<String> getOwners(MetricConfigDTO metric) {
    Set<String> owners = new HashSet<>();
    if (metric != null && metric.getDataset() != null) {
      DatasetConfigDTO datasetConfigDTO = datasetFunctionDAO.findByDataset(metric.getDataset());
      if (datasetConfigDTO != null && datasetConfigDTO.getOwners() != null) {
        owners.addAll(datasetConfigDTO.getOwners());
      }
    }
    return owners;
  }


  private Long createAlertConfig(String alertGroupName, String application, String cron, Set<String> recipients) {
    if (StringUtils.isEmpty(cron)) {
      cron = DEFAULT_ALERT_CRON;
    } else {
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression : " + cron);
      }
    }

    if (StringUtils.isEmpty(application)) {
      application = DEFAULT_ALERT_GROUP_APPLICATION;
    }

    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName(alertGroupName);
    alertConfigDTO.setApplication(application);
    alertConfigDTO.setActive(true);
    alertConfigDTO.setFromAddress(config.getFailureFromAddress());
    alertConfigDTO.setCronExpression(cron);
    alertConfigDTO.setReceiverAddresses(new DetectionAlertFilterRecipients(recipients));
    return this.emailConfigurationDAO.save(alertConfigDTO);
  }

  private List<MetricConfigDTO> fetchMetricsByTag(String tag) {
    List<MetricConfigDTO> results = new ArrayList<>();
    for (MetricConfigDTO metricConfigDTO : this.metricConfigDAO.findAll()) {
      if (metricConfigDTO.getTags() != null) {
        if (metricConfigDTO.getTags().contains(tag)) {
          results.add(metricConfigDTO);
        }
      }
    }
    return results;
  }

  private List<MetricConfigDTO> fetchMetricsByDataset(String dataset) {
    return this.metricConfigDAO.findByDataset(dataset);
  }

  // endpoint clone function Ids to append a name defined in nameTags

  @GET
  @Path("function/{id}")
  @ApiOperation("GET a single function record by id")
  public AnomalyFunctionDTO getAnomalyFunction(@ApiParam("alert function id\n") @PathParam("id") Long id) {
    return anomalyFunctionDAO.findById(id);
  }

  // clone functions in batch
  @POST
  @Path("function/clone")
  public List<Long> cloneFunctionsGetIds(@QueryParam("functionId") String functionIds,
                                         @QueryParam("nameTags") String nameTags,
                                         @QueryParam("cloneAnomaly")@DefaultValue("false") String cloneAnomaly)
      throws Exception {
    ArrayList<Long> cloneFunctionIds = new ArrayList<>();
    try {
      Map<Long, String> idNameTagMap = parseFunctionIdsAndNameTags(functionIds, nameTags);

      for (Map.Entry<Long, String> entry : idNameTagMap.entrySet()) {
        cloneFunctionIds.add(cloneAnomalyFunctionById(entry.getKey(), entry.getValue(), Boolean.valueOf(cloneAnomaly)));
      }
    } catch (Exception e) {
      LOG.error("Can not clone function Ids {}, with name tags {}", functionIds, nameTags);
      throw new WebApplicationException(e);
    }
    return cloneFunctionIds;
  }

  // clone function 1 by 1
  @POST
  @Path("function/{id}/clone/{tag}")
  public Long cloneFunctionGetId(@PathParam("id") Long id,
                                 @PathParam("tag") String tag,
                                 @QueryParam("cloneAnomaly")@DefaultValue("false") String cloneAnomaly)
      throws Exception {
    try {
      return cloneAnomalyFunctionById(id, tag, Boolean.valueOf(cloneAnomaly));
    } catch (Exception e) {
      LOG.error("Can not clone function: Id {}, with name tag {}", id, tag);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Copy total configurations of source anomaly function (with srcId) to destination anomaly function (with destId)
   * Explicit representation: denote source anomaly function with (srcId, srcFunctionName, srcConfigs)
   *                          denote destination anomaly function with (destId, destFunctionName, destConfigs)
   * "copyConfigFromSrcToDest" will update destination anomaly function's configurations by source anomaly function's configurations
   * After "copyConfigFromSrcToDest", the two functions will become:
   *        (srcId, srcFunctionName, srcConfigs)
   *        (destId, destFunctionName, srcConfigs)
   * This in fact updates source anomaly function's properties into destination function's properties
   * @param srcId : the source function with configurations to be copied to
   * @param destId : the destination function Id that will have its configurations being overwritten by source function
   * @return OK is success
   */
  @POST
  @Path("function/{srcId}/copyTo/{destId}")
  public Response copyConfigFromSrcToDest(@PathParam("srcId") @NotNull Long srcId,
      @PathParam("destId") @NotNull Long destId) {
    AnomalyFunctionDTO srcAnomalyFunction = anomalyFunctionDAO.findById(srcId);
    AnomalyFunctionDTO destAnomalyFunction = anomalyFunctionDAO.findById(destId);
    if (srcAnomalyFunction == null) {
      // LOG and exit
      LOG.error("Anomaly Function With id [{}] does not found", srcId);
      return Response.status(Response.Status.BAD_REQUEST).entity("Cannot find function with id: " + srcId).build();
    }

    if (destAnomalyFunction == null) {
      // LOG and exit
      LOG.error("Anomaly Function With id [{}] does not found", destId);
      return Response.status(Response.Status.BAD_REQUEST).entity("Cannot find function with id: " + srcId).build();
    }
    // Thirdeye database uses (functionId, functionName) as an identity for each anomaly function,
    // here by updating the identity of source anomaly function into destination anomaly function's Id and function name,
    // source anomaly function will inherit destination anomaly function's total configurations
    srcAnomalyFunction.setId(destId);
    srcAnomalyFunction.setFunctionName(destAnomalyFunction.getFunctionName());
    anomalyFunctionDAO.update(srcAnomalyFunction); // update configurations
    return Response.ok().build();
  }

  /**
   * Clone anomalies from source anomaly function to destination anomaly function in time range
   * @param srcId : function Id of source anomaly function
   * @param destId : function Id of destination anomaly function
   * @param startTimeIso : start time of anomalies to be cloned, time in ISO format ex: 2016-5-23T00:00:00Z
   * @param endTimeIso : end time of anomalies to be cloned, time in ISO format
   * @return true if at least one anomaly being cloned
   * @throws Exception
   */
  @POST
  @Path("function/{srcId}/cloneAnomalies/{destId}")
  public Response ClonedAnomalies(@PathParam("srcId") @NotNull long srcId,
      @PathParam("destId") @NotNull long destId,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso) {

    long start = 0;
    long end = DateTime.now().getMillis();
    try {
      if (startTimeIso != null) {
        start = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      }
      if (endTimeIso != null) {
        end = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
      }
    } catch (Exception e) {
      throw new WebApplicationException("Failed to parse time, startTime: " + startTimeIso + ", endTime: " + endTimeIso);
    }
    Boolean isAnyCloned = cloneAnomalyInstances(srcId, destId, start, end);
    return Response.ok(isAnyCloned).build();
  }



  // util functions for clone anomaly functions
  /**
   * Parse function ids to be cloned and clone name tags together
   * the length should be aligned otherwise name tags are all empty
   * or all replace with the first name tag (if length == 1)
   *
   * @param functionIds ',' separated string with numbers representing function ids to be cloned
   * @param nameTags ',' separated string with strings representing name tags for clone functions
   * @return a HashMap from function id to be cloned and the new name tag
   */
  private Map<Long, String> parseFunctionIdsAndNameTags(String functionIds, String nameTags) {
    List<Long> functionIdsList = new ArrayList<>();
    if (StringUtils.isNotBlank(functionIds)) {
      String[] tokens = functionIds.split(",");
      for (String token : tokens) {
        functionIdsList.add(Long.valueOf(token));
      }
    }

    int len = functionIdsList.size();

    List<String> nameTagList = new ArrayList<>();
    if (StringUtils.isNotBlank(nameTags)) {
      String[] tags = nameTags.split(",");
      if (tags.length == 1) {
        for (int i = 0; i < len; i++) {
          nameTagList.add(tags[0]);
          LOG.debug("only 1 tag, use the tag for all function");
        }
      } else {
        if (tags.length == len) {
          for (String tag : tags) {
            nameTagList.add(tag);
          }
        } else {
          LOG.debug("tag list and function id list does not mach, use empty strings as tags");
          for (int i = 0; i < len; i++) {
            nameTagList.add("");
          }
        }
      }
    }

    LOG.info("function ids set: {}", functionIds);
    LOG.info("name tag set: {}", nameTagList);
    // Construct Return results
    HashMap<Long, String> IdNameTagMap = new HashMap<>();
    for (int i=0; i < len; i++) {
      IdNameTagMap.put(functionIdsList.get(i), nameTagList.get(i));
    }
    return IdNameTagMap;
  }

  /**
   * clone Anomaly Function by Id to a function name appended with a name tag
   *
   * @param id function id to be cloned
   * @param cloneNameTag an name tag that will append to original function name as serve as the cloned function name
   * @param doCloneAnomaly does the associated anomaly instances will be clone to new function
   * @return id of the clone function
   */
  public Long cloneAnomalyFunctionById(long id, String cloneNameTag, boolean doCloneAnomaly) throws Exception {
    // get anomaly function definition
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    // if cannot find then return
    if (anomalyFunction == null) {
      // LOG and exit
      LOG.error("Anomaly Function With id [{}] does not found", id);
      return null;
    }
    String functionName = anomalyFunction.getFunctionName();
    String newFunctionName = functionName;
    if (cloneNameTag != null && cloneNameTag.length() > 0) {
      newFunctionName = newFunctionName + "_" + cloneNameTag;  // update with input clone function name
    } else {
      newFunctionName = newFunctionName + "_" + "clone_0"; // set default clone function name
    }
    LOG.info("Try to clone anomaly Function Id: {}, Name: {}, to new Name: {}", id, functionName, newFunctionName);
    anomalyFunction.setFunctionName(newFunctionName);
    anomalyFunction.setActive(false);  // deactivate the function
    anomalyFunction.setId(null);
    long newId = anomalyFunctionDAO.save(anomalyFunction);  // exception should be handled when have duplicate name
    LOG.info("clone function id: {}, name: {}  to id: {}, name: {}", id, functionName, newId, newFunctionName);

    if (doCloneAnomaly) {
      cloneAnomalyInstances(id, newId, 0, DateTime.now().getMillis());
    }
    return newId;
  }

  /**
   * Clone merged anomaly instances of one function to another
   *   1. get all merged anomaly instances with AnomalyFunctionId = srcId
   *   2. set the associated anomalyFunctionId = destId
   *   3. save the modified anomaly instances
   * @param srcId the source function Id with anomalies to be cloned.
   * @param destId the destination function Id which source anomalies to be cloned to.
   * @param start the start time of anomalies from source function Id to be cloned.
   * @param end the end time of anomalies from source function Id to be cloned.
   * @return boolean to indicate if the clone is success or not
   */
  public Boolean cloneAnomalyInstances(Long srcId, Long destId, long start, long end) {

    // make sure both function can be identified by IDs

    AnomalyFunctionDTO srcAnomalyFunction = anomalyFunctionDAO.findById(srcId);
    // if cannot find then return
    if (srcAnomalyFunction == null) {
      // LOG and exit
      LOG.error("Source Anomaly Function With id [{}] does not found", srcId);
      return false;
    }

    AnomalyFunctionDTO destAnomalyFunction = anomalyFunctionDAO.findById(destId);
    // if cannot find then return
    if (destAnomalyFunction == null) {
      // LOG and exit
      LOG.error("Destination Anomaly Function With id [{}] does not found", destId);
      return false;
    }

    LOG.info("clone merged anomaly results from source anomaly function id {} to id {}", srcId, destId);

    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOs = mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(start, end, srcId);
    if (mergedAnomalyResultDTOs == null || mergedAnomalyResultDTOs.isEmpty()) {
      LOG.error("No merged anomaly results found for anomaly function Id: {}", srcId);
      return false;
    }

    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : mergedAnomalyResultDTOs) {
      long oldId = mergedAnomalyResultDTO.getId();
      mergedAnomalyResultDTO.setId(null);  // clean the Id, then will create a new Id when save
      mergedAnomalyResultDTO.setFunctionId(destId);
      mergedAnomalyResultDTO.setFunction(destAnomalyFunction);
      long newId = mergedAnomalyResultDAO.save(mergedAnomalyResultDTO);
      LOG.debug("clone merged anomaly {} to {}", oldId, newId);
    }
    return true;
  }

  /**
   * Delete raw and merged anomalies whose start time is located in the given time ranges
   * @param startTimeIso The start time of the monitoring window, if null, use smallest time
   * @param endTimeIso The start time of the monitoring window, if null, use current time
   */
  @DELETE
  @Path("function/{id}/anomalies")
  public Map<String, Integer> deleteAnomalies(@PathParam("id") Long functionId,
    @QueryParam("start") String startTimeIso,
    @QueryParam("end") String endTimeIso) {

    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      LOG.info("Anomaly functionId {} is not found", functionId);
      return null;
    }

    long startTime = 0;
    long endTime = DateTime.now().getMillis();
    try {
      if (startTimeIso != null) {
        startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      }
      if (endTimeIso != null) {
        endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
      }
    } catch (Exception e) {
      throw new WebApplicationException("Unable to parse strings, " + startTimeIso + " and " + endTimeIso
          + ", in ISO DateTime format", e);
    }

    LOG.info("Delete anomalies of function {} in the time range: {} -- {}", functionId, startTimeIso, endTimeIso);

    return deleteExistingAnomalies(functionId, startTime, endTime);
  }

  /**
   * Delete raw or merged anomalies whose start time is located in the given time ranges, except
   * the following two cases:
   *
   * 1. If a raw anomaly belongs to a merged anomaly whose start time is not located in the given
   * time ranges, then the raw anomaly will not be deleted.
   *
   * 2. If a raw anomaly belongs to a merged anomaly whose start time is located in the given
   * time ranges, then it is deleted regardless its start time.
   *
   * If monitoringWindowStartTime is not given, then start time is set to 0.
   * If monitoringWindowEndTime is not given, then end time is set to Long.MAX_VALUE.
   * @param functionId function id
   * @param monitoringWindowStartTime The start time of the monitoring window (in milli-second)
   * @param monitoringWindowEndTime The start time of the monitoring window (in milli-second)
   */
  public Map<String, Integer> deleteExistingAnomalies(long functionId,
      long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      LOG.info("Anomaly functionId {} is not found", functionId);
      return null;
    }
    HashMap<String, Integer> returnInfo = new HashMap<>();

    // Find merged anomaly result and delete them first
    LOG.info("Deleting merged anomaly results in the time range: {} -- {}", new DateTime(monitoringWindowStartTime), new
        DateTime(monitoringWindowEndTime));
    LOG.info("Beginning cleanup merged anomaly results of functionId {} collection {} metric {}",
        functionId, anomalyFunction.getCollection(), anomalyFunction.getMetric());
    int mergedAnomaliesDeleted = 0;
    List<MergedAnomalyResultDTO> mergedResults =
        mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(monitoringWindowStartTime, monitoringWindowEndTime, functionId);
    if (CollectionUtils.isNotEmpty(mergedResults)) {
      mergedAnomaliesDeleted = deleteMergedResults(mergedResults);
    }
    returnInfo.put("mergedAnomaliesDeleted", mergedAnomaliesDeleted);
    LOG.info("{} merged anomaly results have been deleted", mergedAnomaliesDeleted);

    return returnInfo;
  }

  // Delete merged anomaly results from mergedAnomalyResultDAO
  private int deleteMergedResults(List<MergedAnomalyResultDTO> mergedResults) {
    LOG.info("Deleting merged results");
    int mergedAnomaliesDeleted = 0;
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      LOG.info("...Deleting merged result id {} for functionId {}", mergedResult.getId(), mergedResult.getFunctionId());
      mergedAnomalyResultDAO.delete(mergedResult);
      mergedAnomaliesDeleted++;
    }
    return mergedAnomaliesDeleted;
  }
}