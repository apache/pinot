package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.primitives.Floats;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;

@Path("/onboard")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OnboardResource {
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final RawAnomalyResultManager rawAnomalyResultDAO;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(OnboardResource.class);

  public OnboardResource() {
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.rawAnomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
  }

  // endpoint clone function Ids to append a name defined in nameTags

  @GET
  @Path("function/{id}")
  public AnomalyFunctionDTO getAnomalyFunction(@PathParam("id") Long id) {
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
  public Long cloneFunctionsGetIds(@PathParam("id") Long id,
      @PathParam("tag") String tag,
      @QueryParam("cloneAnomaly")@DefaultValue("false") String cloneAnomaly)
      throws Exception {
    LOG.info("Prepare to clone functionId {} with tag {} and cloneAnomaly {}", id, tag, cloneAnomaly);
    try {
      return cloneAnomalyFunctionById(id, tag, Boolean.valueOf(cloneAnomaly));
    } catch (Exception e) {
      LOG.error("Can not clone function: Id {}, with name tag {}", id, tag);
      throw new WebApplicationException(e);
    }
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
      cloneAnomalyInstances(id, newId);
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
   * @return boolean to indicate if the clone is success or not
   */
  public Boolean cloneAnomalyInstances(Long srcId, Long destId) {

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

    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOs = mergedAnomalyResultDAO.findByFunctionId(srcId);
    if (mergedAnomalyResultDTOs == null || mergedAnomalyResultDTOs.isEmpty()) {
      LOG.error("No merged anomaly results found for anomaly function Id: {}", srcId);
      return false;
    }

    LOG.info("{} merged anomaly results has found with functionId {}. Prepare to copy to dest. functionId {}",
        mergedAnomalyResultDTOs.size(), srcId, destId);

    // TODO: MergedAnomalyResults aren't copied to the new destination
    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : mergedAnomalyResultDTOs) {
      long oldId = mergedAnomalyResultDTO.getId();
      mergedAnomalyResultDTO.setId(null);  // clean the Id, then will create a new Id when save
      mergedAnomalyResultDTO.setRawAnomalyIdList(null);
      mergedAnomalyResultDTO.setFunctionId(destId);
      mergedAnomalyResultDTO.setFunction(destAnomalyFunction);
      long newId = mergedAnomalyResultDAO.save(mergedAnomalyResultDTO);
      LOG.debug("clone merged anomaly {} to {}", oldId, newId);
    }
    return true;
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
   * @param monitoringWindowStartTime The start time of the monitoring window (in milli-second)
   * @param monitoringWindowEndTime The start time of the monitoring window (in milli-second)
   */
  @POST
  @Path("function/{id}/deleteExistingAnomalies")
  public Map<String, Integer> deleteExistingAnomalies(@PathParam("id") String id,
      @QueryParam("start") long monitoringWindowStartTime,
      @QueryParam("end") long monitoringWindowEndTime) {
    long functionId = Long.valueOf(id);
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunction == null){
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

    // Find raw anomaly results and delete them
    LOG.info("Deleting raw anomaly results in the time range: {} -- {}", new DateTime(monitoringWindowStartTime), new
        DateTime(monitoringWindowEndTime));
    LOG.info("Beginning cleanup merged anomaly results of functionId {} collection {} metric {}",
        functionId, anomalyFunction.getCollection(), anomalyFunction.getMetric());
    int rawAnomaliesDeleted = 0;
    List<RawAnomalyResultDTO> rawResults =
        rawAnomalyResultDAO.findAllByTimeAndFunctionId(monitoringWindowStartTime, monitoringWindowEndTime, functionId);
    if (CollectionUtils.isNotEmpty(rawResults)){
      rawAnomaliesDeleted = deleteRawResults(rawResults);
    }
    returnInfo.put("rawAnomaliesDeleted", rawAnomaliesDeleted);
    LOG.info("{} raw anomaly results have been deleted", rawAnomaliesDeleted);

    return returnInfo;
  }

  /**
   * Show the content of merged anomalies whose start time is located in the given time ranges
   *
   * @param monitoringWindowStartTime The start time of the monitoring window (in milli-second)
   * @param monitoringWindowEndTime The start time of the monitoring window (in milli-second)
   */
  @POST
  @Path("function/{id}/getExistingRawAnomalies")
  public List<RawAnomalyResultDTO> getExistingRawAnomalies(@PathParam("id") String id,
      @QueryParam("start") long monitoringWindowStartTime,
      @QueryParam("end") long monitoringWindowEndTime) {
    LOG.info("Retrieving merged anomaly results in the time range: {} -- {}", new DateTime(monitoringWindowStartTime), new
        DateTime(monitoringWindowEndTime));

    List<RawAnomalyResultDTO> rawResults = null;

    long functionId = Long.valueOf(id);
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunction == null){
      LOG.info("Anomaly functionId {} is not found", functionId);
      return rawResults;
    }
    rawResults = rawAnomalyResultDAO.findAllByTimeAndFunctionId(monitoringWindowStartTime,
        monitoringWindowEndTime, functionId);
    return rawResults;
  }

  /**
   * Show the content of raw anomalies whose start time is located in the given time ranges
   *
   * @param monitoringWindowStartTime The start time of the monitoring window (in milli-second)
   * @param monitoringWindowEndTime The start time of the monitoring window (in milli-second)
   */
  @POST
  @Path("function/{id}/getExistingMergedAnomalies")
  public List<MergedAnomalyResultDTO> getExistingMergedAnomalies(@PathParam("id") String id,
      @QueryParam("start") long monitoringWindowStartTime,
      @QueryParam("end") long monitoringWindowEndTime) {
    LOG.info("Retrieving merged anomaly results in the time range: {} -- {}", new DateTime(monitoringWindowStartTime), new
        DateTime(monitoringWindowEndTime));

    List<MergedAnomalyResultDTO> mergedResults = null;

    long functionId = Long.valueOf(id);
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunction == null){
      LOG.info("Anomaly functionId {} is not found", functionId);
      return mergedResults;
    }
    mergedResults = mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(monitoringWindowStartTime,
        monitoringWindowEndTime, functionId);
    return mergedResults;
  }

  // Delete merged anomaly results from mergedAnomalyResultDAO
  private int deleteMergedResults(List<MergedAnomalyResultDTO> mergedResults) {
    LOG.info("Deleting merged results");
    int mergedAnomaliesDeleted = 0;
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      // Delete raw anomalies of the merged anomaly
      List<RawAnomalyResultDTO> rawAnomalyResultDTOs = mergedResult.getAnomalyResults();
      //deleteRawResults(rawAnomalyResultDTOs);

      LOG.info(".....Deleting merged result id {} for functionId {}", mergedResult.getId(), mergedResult.getFunctionId());
      mergedAnomalyResultDAO.delete(mergedResult);
      mergedAnomaliesDeleted++;
    }
    return mergedAnomaliesDeleted;
  }

  // Delete raw anomaly results from rawResultDAO
  private int deleteRawResults(List<RawAnomalyResultDTO> rawResults){
    LOG.info("Deleting raw anomaly results...");
    int rawAnomaliesDeleted = 0;
    for(RawAnomalyResultDTO rawResult : rawResults){
      LOG.info("...Deleting raw anomaly result id {} for functionId {}", rawResult.getId(), rawResult.getFunctionId());
      rawAnomalyResultDAO.delete(rawResult);
      rawAnomaliesDeleted++;
    }
    return rawAnomaliesDeleted;
  }

  /**
   * Breaks down the given range into consecutive monitoring windows as per function definition
   * Regenerates anomalies for each window separately
   * @param id anomaly function id
   * @param detectionHost the host of anomaly detection function
   * @param detectionPort the port of anomaly detection function
   * @param monitoringWindowStartTime The start time of the monitoring window (in milli-second)
   * @param monitoringWindowEndTime The start time of the monitoring window (in milli-second)
   * @param isForceBackfill false to resume backfill from the latest left off
   * @return HTTP response of this request
   * @throws Exception
   */
  //TODO: Make detection host available
  private final String detectionHost = "localhost";
  private final int detectionPort = 1867;
  @POST
  @Path("function/{id}/regenerateAnomaliesInRange")
  public String regenerateAnomaliesInRange(@PathParam("id") String id,
//      @QueryParam("detectionHost") String detectionHost,
//      @QueryParam("detectionPort") int detectionPort,
      @QueryParam("start") long monitoringWindowStartTime,
      @QueryParam("end") long monitoringWindowEndTime,
      @QueryParam("forceBackfill") @DefaultValue("false") String isForceBackfill) throws Exception {
    long functionId = Long.valueOf(id);
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunction == null){
      LOG.info("Anomaly functionId {} is not found", functionId);
      return String.format("Anomaly Function %s is not found", id);
    }

    // Check if the merged anomaly results has been cleaned up before regeneration
    List<MergedAnomalyResultDTO> mergedResults =
        mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(monitoringWindowStartTime, monitoringWindowEndTime, functionId);
    if(!mergedResults.isEmpty()){
      LOG.info("Merged anomaly results of functionId {} should be cleaned up before regeneration", id);
      return String.format("Merged anomaly results of functionId %s should be cleaned up before regeneration", id);
    }

    DetectionResourceHttpUtils detectionResourceHttpUtils = new DetectionResourceHttpUtils(detectionHost, detectionPort);

    if (!anomalyFunction.getIsActive()) {
      LOG.info("Skipping deactivated function {}", functionId);
      return String.format("Skipping deactivated function %s", id);
    }
    else{
      LOG.info("Sending backfill function {} for range {} to {}", functionId, monitoringWindowStartTime, monitoringWindowEndTime);
      String response = detectionResourceHttpUtils.runBackfillAnomalyFunction(
          String.valueOf(functionId),
          Long.toString(monitoringWindowStartTime),
          Long.toString(monitoringWindowEndTime),
          Boolean.valueOf(isForceBackfill));
      LOG.info("Response {}", response);
      return response;
    }
  }

}
