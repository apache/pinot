package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;

/**
 * Utility class to cleanup all anomalies for input datasets,
 * and regenerate anomalies for time range specified in the input
 * Inputs:
 * config file for config class CleanupAndRegenerateAnomaliesConfig
 */
public class CleanupAndRegenerateAnomaliesTool {

  private static final Logger LOG = LoggerFactory.getLogger(CleanupAndRegenerateAnomaliesTool.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private enum Mode {
    DELETE,
    GENERATE_FOR_RANGE, BACKFILL_FOR_RANGE
  }
  private String monitoringWindowStartTime;
  private String monitoringWindowEndTime;
  private List<Long> functionIds;

  private int rawAnomaliesDeleted = 0;
  private int mergedAnomaliesDeleted = 0;

  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawResultDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private DetectionResourceHttpUtils detectionResourceHttpUtils;

  public CleanupAndRegenerateAnomaliesTool(String startTime, String endTime, String datasets, String functionIds,
      File persistenceFile, String detectionHost, int detectionPort, String token)
      throws Exception {
    init(persistenceFile);
    this.monitoringWindowStartTime = startTime;
    this.monitoringWindowEndTime = endTime;
    this.functionIds = getFunctionIds(datasets, functionIds);
    detectionResourceHttpUtils = new DetectionResourceHttpUtils(detectionHost, detectionPort, token);
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }

  private List<Long> getFunctionIds(String datasets, String functionIds) {
    List<Long> functionIdsList = new ArrayList<>();
    if (StringUtils.isNotBlank(functionIds)) {
      String[] tokens = functionIds.split(",");
      for (String token : tokens) {
        functionIdsList.add(Long.valueOf(token));
      }
    } else if (StringUtils.isNotBlank(datasets)) {
      List<String> datasetsList = Lists.newArrayList(datasets.split(","));
      for (String dataset : datasetsList) {
        List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(dataset);
        for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
          functionIdsList.add(anomalyFunction.getId());
        }
      }
    }
    return functionIdsList;
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
   */
  private void deleteExistingAnomalies() {
    long startTime = 0;
    long endTime = Long.MAX_VALUE;
    if (StringUtils.isNotBlank(monitoringWindowStartTime)) {
      startTime =
          ISODateTimeFormat.dateTimeParser().parseDateTime(monitoringWindowStartTime).getMillis();
    }
    if (StringUtils.isNotBlank(monitoringWindowEndTime)) {
      endTime =
          ISODateTimeFormat.dateTimeParser().parseDateTime(monitoringWindowEndTime).getMillis();
    }
    LOG.info("Deleting anomalies in the time range: {} -- {}", new DateTime(startTime), new
        DateTime(endTime));

    for (Long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      if(anomalyFunction == null){
        LOG.info("Requested functionId {} doesn't exist", functionId);
        continue;
      }

      LOG.info("Beginning cleanup of functionId {} collection {} metric {}",
          functionId, anomalyFunction.getCollection(), anomalyFunction.getMetric());

      // Clean up merged and raw anomaly of functionID
      OnboardResource onboardResource = new OnboardResource(anomalyFunctionDAO, mergedResultDAO, rawResultDAO);
      onboardResource.deleteExistingAnomalies(functionId, startTime, endTime);
    }
  }


  /**
   * Regenerates anomalies for the whole given range as one monitoring window
   * @throws Exception
   */
  @Deprecated
  private void regenerateAnomaliesInRange() throws Exception {
    LOG.info("Begin regenerate anomalies for entire range...");
    for (Long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      boolean isActive = anomalyFunction.getIsActive();
      if (!isActive) {
        LOG.info("Skipping function {}", functionId);
        continue;
      }
      runAdhocFunctionForWindow(functionId, monitoringWindowStartTime, monitoringWindowEndTime);
    }
  }

  /**
   * Breaks down the given range into consecutive monitoring windows as per function definition
   * Regenerates anomalies for each window separately
   * @throws Exception
   */
  private void regenerateAnomaliesForBucketsInRange(boolean forceBackfill) throws Exception {
    for (Long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      if (!anomalyFunction.getIsActive()) {
        LOG.info("Skipping deactivated function {}", functionId);
        continue;
      }

      LOG.info("Sending backfill function {} for range {} to {}", functionId, monitoringWindowStartTime, monitoringWindowEndTime);

      String response =
          detectionResourceHttpUtils.runBackfillAnomalyFunction(String.valueOf(functionId), monitoringWindowStartTime,
              monitoringWindowEndTime, forceBackfill);
      LOG.info("Response {}", response);
    }
  }

  private void runAdhocFunctionForWindow(Long functionId, String monitoringWindowStart, String monitoringWindowEnd)
      throws ClientProtocolException, IOException {
    LOG.info("Running adhoc function {} for range {} to {}", functionId, monitoringWindowStart, monitoringWindowEnd);
    String response = detectionResourceHttpUtils.runAdhocAnomalyFunction(String.valueOf(functionId),
        monitoringWindowStart, monitoringWindowEnd);
    LOG.info("Response {}", response);
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("USAGE CleanupAndRegenerateAnomaliesTool <config_yml_file> <mode> \n "
          + "Please take note: \nDELETE mode will delete all anomalies for that functionId/dataset, "
          + "\nGENERATE mode will generate anomalies in time range you specify");
      System.exit(1);
    }
    File configFile = new File(args[0]);
    CleanupAndRegenerateAnomaliesConfig config =
        OBJECT_MAPPER.readValue(configFile, CleanupAndRegenerateAnomaliesConfig.class);

    String mode = args[1];

    File persistenceFile = new File(config.getPersistenceFile());
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    String detectorHost = config.getDetectorHost();
    int detectorPort = config.getDetectorPort();
    if (StringUtils.isBlank(detectorHost)) {
      LOG.error("Detector host and port must be provided");
      System.exit(1);
    }

    String startTimeIso = config.getStartTimeIso();
    String endTimeIso = config.getEndTimeIso();
    Mode runMode = Mode.valueOf(mode);
    if ((runMode.equals(Mode.GENERATE_FOR_RANGE) || runMode.equals(Mode.BACKFILL_FOR_RANGE))
        && (StringUtils.isBlank(startTimeIso) || StringUtils.isBlank(endTimeIso))) {
      LOG.error("StarteTime and endTime must be provided in generate mode");
      System.exit(1);
    }

    String datasets = config.getDatasets();
    String functionIds = config.getFunctionIds();
    if (StringUtils.isBlank(datasets) && StringUtils.isBlank(functionIds)) {
      LOG.error("Must provide one of datasets or functionIds");
      System.exit(1);
    }

    boolean doForceBackfill = false;
    String forceBackfill = config.getForceBackfill();
    if (StringUtils.isNotBlank(forceBackfill)) {
      doForceBackfill = Boolean.parseBoolean(forceBackfill);
    }

    String authToken = "";

    CleanupAndRegenerateAnomaliesTool tool = new CleanupAndRegenerateAnomaliesTool(startTimeIso,
        endTimeIso, datasets, functionIds, persistenceFile, detectorHost, detectorPort, authToken);

    if (runMode.equals(Mode.DELETE)) {
      // DELETE mode deletes *ALL* anomalies for all functions in functionIds or datasets
      tool.deleteExistingAnomalies();
    } else if (runMode.equals(Mode.GENERATE_FOR_RANGE)) {
      // GENERATE_FOR_RANGE mode regenerates anomalies for all active functions in functionIds or datasets
      tool.regenerateAnomaliesInRange();
    } else if (runMode.equals(Mode.BACKFILL_FOR_RANGE)) {
      // BACKFILL_FOR_RANGE mode regenerates anomalies for all active functions in functionIds or datasets
      // It will honor the monitoring window size of the function, and run for all consecutive windows, one by one,
      // to cover the entire range provided as input
      tool.regenerateAnomaliesForBucketsInRange(doForceBackfill);
    } else {
      LOG.error("Incorrect mode {}", mode);
      System.exit(1);
    }
    // Added this because database connection gets stuck at times and program never terminates
    System.exit(0);
  }

}
