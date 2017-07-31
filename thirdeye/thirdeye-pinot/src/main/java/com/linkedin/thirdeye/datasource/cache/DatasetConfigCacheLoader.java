package com.linkedin.thirdeye.datasource.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class DatasetConfigCacheLoader extends CacheLoader<String, DatasetConfigDTO> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetConfigCacheLoader.class);
  private DatasetConfigManager datasetConfigDAO;

  public DatasetConfigCacheLoader(DatasetConfigManager datasetConfigDAO) {
    this.datasetConfigDAO = datasetConfigDAO;
  }

  @Override
  public DatasetConfigDTO load(String collection) throws Exception {
    LOGGER.debug("Loading DatasetConfigCache for {}", collection);
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
    return datasetConfig;
  }

}
