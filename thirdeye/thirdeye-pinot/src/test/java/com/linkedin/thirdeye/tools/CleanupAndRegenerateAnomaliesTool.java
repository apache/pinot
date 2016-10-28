package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

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
    GENERATE_FOR_RANGE,
    GENERATE_FOR_WINDOWS_IN_RANGE
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
      File persistenceFile, String detectionHost, int detectionPort)
      throws Exception {
    init(persistenceFile);
    this.monitoringWindowStartTime = startTime;
    this.monitoringWindowEndTime = endTime;
    this.functionIds = getFunctionIds(datasets, functionIds);
    detectionResourceHttpUtils = new DetectionResourceHttpUtils(detectionHost, detectionPort);
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

  private void deleteExistingAnomalies() {

    for (Long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      LOG.info("Beginning cleanup of functionId {} collection {} metric {}",
          functionId, anomalyFunction.getCollection(), anomalyFunction.getMetric());
      List<RawAnomalyResultDTO> rawResults = rawResultDAO.findByFunctionId(functionId);
      if (CollectionUtils.isNotEmpty(rawResults)) {
        deleteRawResults(rawResults);
      }
      List<MergedAnomalyResultDTO> mergedResults = mergedResultDAO.findByFunctionId(functionId);
      if (CollectionUtils.isNotEmpty(mergedResults)) {
        deleteMergedResults(mergedResults);
      }
    }
    LOG.info("Deleted {} raw anomalies", rawAnomaliesDeleted);
    LOG.info("Deleted {} merged anomalies", mergedAnomaliesDeleted);
  }


  /**
   * Regenerates anomalies for the whole given range as one monitoring window
   * @throws Exception
   */
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
  private void regenerateAnomaliesForBucketsInRange() throws Exception {
    LOG.info("Begin regenerate anomalies for each monitoring window in range...");

    DateTime start = ISODateTimeFormat.dateTimeParser().parseDateTime(monitoringWindowStartTime);
    DateTime end = ISODateTimeFormat.dateTimeParser().parseDateTime(monitoringWindowEndTime);

    for (Long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      boolean isActive = anomalyFunction.getIsActive();
      if (!isActive) {
        LOG.info("Skipping function {}", functionId);
        continue;
      }

      long monitoringWindowSize = TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowSize(),
          anomalyFunction.getWindowUnit());
      CronExpression cronExpression = new CronExpression(anomalyFunction.getCron());

      DateTime currentStart = start;
      DateTime currentEnd = currentStart.plus(monitoringWindowSize);

      // Make the end time inclusive
      DateTime endBoundary = new DateTime(cronExpression.getNextValidTimeAfter(end.toDate()));
      while (currentEnd.isBefore(endBoundary)) {
        String monitoringWindowStart = ISODateTimeFormat.dateHourMinute().print(currentStart);
        String monitoringWindowEnd = ISODateTimeFormat.dateHourMinute().print(currentEnd);

        runAdhocFunctionForWindow(functionId, monitoringWindowStart, monitoringWindowEnd);

        currentStart = new DateTime(cronExpression.getNextValidTimeAfter(currentStart.toDate()));
        currentEnd = currentStart.plus(monitoringWindowSize);
      }
    }
  }

  private void runAdhocFunctionForWindow(Long functionId, String monitoringWindowStart, String monitoringWindowEnd)
      throws ClientProtocolException, IOException {
    LOG.info("Running adhoc function {} for range {} to {}", functionId, monitoringWindowStart, monitoringWindowEnd);
    String response = detectionResourceHttpUtils.runAdhocAnomalyFunction(String.valueOf(functionId),
        monitoringWindowStart, monitoringWindowEnd);
    LOG.info("Response {}", response);
  }

  private void deleteRawResults(List<RawAnomalyResultDTO> rawResults) {
    LOG.info("Deleting raw results");
    for (RawAnomalyResultDTO rawResult : rawResults) {
      LOG.info("......Deleting id {} for functionId {}", rawResult.getId(), rawResult.getFunctionId());
      rawResultDAO.delete(rawResult);
      rawAnomaliesDeleted++;
    }
  }

  private void deleteMergedResults(List<MergedAnomalyResultDTO> mergedResults) {
    LOG.info("Deleting merged results");
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      LOG.info(".....Deleting id {} for functionId {}", mergedResult.getId(), mergedResult.getFunctionId());
      mergedResultDAO.delete(mergedResult);
      mergedAnomaliesDeleted++;
    }
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
    if ((runMode.equals(Mode.GENERATE_FOR_RANGE) || runMode.equals(Mode.GENERATE_FOR_WINDOWS_IN_RANGE))
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

    CleanupAndRegenerateAnomaliesTool tool = new CleanupAndRegenerateAnomaliesTool(startTimeIso,
        endTimeIso, datasets, functionIds, persistenceFile, detectorHost, detectorPort);

    if (runMode.equals(Mode.DELETE)) {
      // DELETE mode deletes *ALL* anomalies for all functions in functionIds or datasets
      tool.deleteExistingAnomalies();
    } else if (runMode.equals(Mode.GENERATE_FOR_RANGE)) {
      // GENERATE_FOR_RANGE mode regenerates anomalies for all active functions in functionIds or datasets
      tool.regenerateAnomaliesInRange();
    } else if (runMode.equals(Mode.GENERATE_FOR_WINDOWS_IN_RANGE)) {
      // GENERATE_FOR_WINDOWS_IN_RANGE mode regenerates anomalies for all active functions in functionIds or datasets
      // It will honor the monitoring window size of the function, and run for all consecutive windows, one by one,
      // to cover the entire range provided as input
      tool.regenerateAnomaliesForBucketsInRange();
    } else {
      LOG.error("Incorrect mode {}", mode);
      System.exit(1);
    }
    // Added this because database connection gets stuck at times and program never terminates
    System.exit(0);
  }

}
