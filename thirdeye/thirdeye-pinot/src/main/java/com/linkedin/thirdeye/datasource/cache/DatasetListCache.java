package com.linkedin.thirdeye.datasource.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class DatasetListCache {

  private AtomicReference<List<String>> datasetListRef;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private static final Logger LOG = LoggerFactory.getLogger(DatasetListCache.class);


  public DatasetListCache(AnomalyFunctionManager anomalyFunctionDAO, DatasetConfigManager datasetConfigDAO,
      ThirdEyeConfiguration config) {
    this.datasetListRef = new AtomicReference<>();
    this.datasetConfigDAO = datasetConfigDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
  }


  public List<String> getDatasets() {
    return datasetListRef.get();
  }


  /**
   * Loads all datasets which have anomaly functions, into dataset cache list
   */
  public void loadDatasets() {

    List<String> datasets = new ArrayList<>();

    List<AnomalyFunctionDTO> findAll = anomalyFunctionDAO.findAllActiveFunctions();
    Set<String> uniqueDatasets = new HashSet<>();
    for (AnomalyFunctionDTO anomalyFunction : findAll) {
      uniqueDatasets.add(anomalyFunction.getCollection());
    }

    for (String dataset : uniqueDatasets) {
      DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
      if (datasetConfig == null || !datasetConfig.isActive()) {
        LOG.info("Skipping dataset {} due to missing dataset config or status inactive", dataset);
        continue;
      }
      datasets.add(dataset);
    }

    LOG.info("Loading collections {}", datasets);
    datasetListRef.set(datasets);
  }

}

