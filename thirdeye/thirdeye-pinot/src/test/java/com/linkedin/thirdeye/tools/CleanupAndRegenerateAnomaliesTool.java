package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;

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
 * persistence file input path -
 * detector host - the detector which will run these functions
 * detector port -
 * startTime - start time for anomalies regeneration in ISO (eg 2016-09-01)
 * endTime - end time for anomalies regeneration in ISO
 * datasets - the datasets for which to regenerate anomalies in csv format
 */
public class CleanupAndRegenerateAnomaliesTool {

  private static final Logger LOG = LoggerFactory.getLogger(CleanupAndRegenerateAnomaliesTool.class);

  private String startTime;
  private String endTime;
  private List<String> datasets;

  private int rawAnomaliesDeleted = 0;
  private int mergedAnomaliesDeleted = 0;

  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawResultDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private DetectionResourceHttpUtils detectionResourceHttpUtils;

  public CleanupAndRegenerateAnomaliesTool(String startTime, String endTime, String datasets, File persistenceFile,
      String detectionHost, int detectionPort)
      throws Exception {
    init(persistenceFile);
    this.startTime = startTime;
    this.endTime = endTime;
    this.datasets = getDatasets(datasets);
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

  /**
   * Creates arraylist of datasets to process, picks all if datasets not specified in input
   */
  private List<String> getDatasets(String datasets) {
    List<String> datasetsList = null;
    if (StringUtils.isBlank(datasets)) {
      Set<String> datasetNames = new HashSet<>();
      List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAll();
      for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctions) {
        datasetNames.add(anomalyFunctionDTO.getCollection());
      }
      datasetsList = Lists.newArrayList(datasetNames);
    } else {
      datasetsList = Lists.newArrayList(datasets.split(","));
    }
    LOG.info("Running for datasets {}", datasetsList);
    return datasetsList;
  }

  private void deleteExistingAnomalies() {

    Map<Long, List<RawAnomalyResultDTO>> rawResultsByFunctionId = getRawResultsByFunctionId();

    for (String dataset : datasets) {
      List<AnomalyFunctionDTO> anomalyFunctionDTOs = anomalyFunctionDAO.findAllByCollection(dataset);
      LOG.info("Beginning cleanup of dataset {}", dataset);
      for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
        Long functionId = anomalyFunctionDTO.getId();
        LOG.info("Beginning cleanup of functionId {}", functionId);
        List<RawAnomalyResultDTO> rawResults = rawResultsByFunctionId.get(functionId);
        if (CollectionUtils.isNotEmpty(rawResults)) {
          deleteRawResults(rawResults);
        }
        List<MergedAnomalyResultDTO> mergedResults = mergedResultDAO.findByFunctionId(functionId);
        if (CollectionUtils.isNotEmpty(mergedResults)) {
          deleteMergedResults(mergedResults);
        }
      }
    }
    LOG.info("Deleted {} raw anomalies", rawAnomaliesDeleted);
    LOG.info("Deleted {} merged anomalies", mergedAnomaliesDeleted);
  }

  private Map<Long, List<RawAnomalyResultDTO>> getRawResultsByFunctionId() {
    Map<Long, List<RawAnomalyResultDTO>> rawResultsByFunctionId = new HashMap<>();
    List<RawAnomalyResultDTO> rawResults = rawResultDAO.findAll();
    for (RawAnomalyResultDTO rawResult : rawResults) {
      Long functionId = rawResult.getFunctionId();
      if (rawResultsByFunctionId.containsKey(functionId)) {
        rawResultsByFunctionId.get(functionId).add(rawResult);
      } else {
        rawResultsByFunctionId.put(functionId, Lists.newArrayList(rawResult));
      }
    }
    return rawResultsByFunctionId;
  }

  private void regenerateAnomalies() throws Exception {
    for (String dataset : datasets) {
      List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(dataset);
      for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
        Long functionId = anomalyFunction.getId();
        LOG.info("Running adhoc function {}", functionId);
        String response = detectionResourceHttpUtils.runAdhocAnomalyFunction(String.valueOf(functionId),
            startTime, endTime);
        LOG.info("Response {}", response);
      }
    }
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

  public void run() throws Exception {
    deleteExistingAnomalies();
    regenerateAnomalies();
  }

  public static void main(String[] args) throws Exception {

    if (args.length < 5) {
      System.err.println("USAGE CleanupAndRegenerateAnomaliesTool <persistence_file_path> "
          + "<detector_host> <detector_port> <start_time_iso> <end_time_iso> <datasets>");
      System.exit(1);
    }
    File persistenceFile = new File(args[0]);
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    String detectionHost = args[1];
    int detectionPort = Integer.valueOf(args[2]);

    String startTime = args[3];
    String endTime = args[4];
    String datasets = null;
    if (args.length > 5) {
      datasets = args[6];
    }

    CleanupAndRegenerateAnomaliesTool tool = new CleanupAndRegenerateAnomaliesTool(startTime, endTime, datasets,
        persistenceFile, detectionHost, detectionPort);
    tool.run();
  }

}
