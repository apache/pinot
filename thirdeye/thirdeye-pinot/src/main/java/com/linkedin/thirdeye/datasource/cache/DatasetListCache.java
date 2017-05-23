package com.linkedin.thirdeye.datasource.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class DatasetListCache {

  private AtomicReference<List<String>> datasetListRef;
  private DatasetConfigManager datasetConfigDAO;
  private List<String> whitelistCollections;
  private static final Logger LOG = LoggerFactory.getLogger(DatasetListCache.class);


  public DatasetListCache(DatasetConfigManager datasetConfigDAO, ThirdEyeConfiguration config) {
    this.datasetListRef = new AtomicReference<>();
    this.datasetConfigDAO = datasetConfigDAO;
    this.whitelistCollections = config.getWhitelistCollections();
  }


  public List<String> getDatasets() {
    return datasetListRef.get();
  }

  // TODO: remove concept of whitelist.
  // This of how we will initialize caches, if at all.
  // Because initializing caches for all datasets will make startup very slow
  public void loadDatasets() {
    List<String> datasets = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(whitelistCollections)) {
      for (String dataset : whitelistCollections) {
        DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
        if (datasetConfig == null || !datasetConfig.isActive()) {
          LOG.info("Skipping dataset {} due to missing dataset config or status inactive", dataset);
          continue;
        }
        datasets.add(dataset);
      }
    } else {
      List<DatasetConfigDTO> datasetConfigs = datasetConfigDAO.findActive();
      for (DatasetConfigDTO datasetConfigDTO : datasetConfigs) {
        datasets.add(datasetConfigDTO.getDataset());
      }
    }

    LOG.info("Loading collections {}", datasets);
    datasetListRef.set(datasets);
  }

}

