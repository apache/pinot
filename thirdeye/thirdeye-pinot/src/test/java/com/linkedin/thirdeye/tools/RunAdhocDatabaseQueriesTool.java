package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

/**
 * Run adhoc queries to db
 */
public class RunAdhocDatabaseQueriesTool {

  private static final Logger LOG = LoggerFactory.getLogger(RunAdhocDatabaseQueriesTool.class);

  private AnomalyFunctionManager anomalyFunctionDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private RawAnomalyResultManager rawResultDAO;
  private MergedAnomalyResultManager mergedResultDAO;

  public RunAdhocDatabaseQueriesTool(File persistenceFile)
      throws Exception {
    init(persistenceFile);
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    emailConfigurationDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    rawResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }

  private void toggleAnomalyFunction(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    anomalyFunction.setActive(true);
    anomalyFunctionDAO.update(anomalyFunction);
  }

  private void updateField(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    anomalyFunction.setCron("0/20 * * * * ?");
    anomalyFunctionDAO.update(anomalyFunction);
  }

  private void customFunction() {
    List<AnomalyFunctionDTO> anomalyFunctionDTOs = anomalyFunctionDAO.findAll();
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
      anomalyFunctionDTO.setActive(false);
      anomalyFunctionDAO.update(anomalyFunctionDTO);
    }
  }

  public static void main(String[] args) throws Exception {

    File persistenceFile = new File(args[0]);
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    RunAdhocDatabaseQueriesTool dq = new RunAdhocDatabaseQueriesTool(persistenceFile);
  }

}
