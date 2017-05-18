package com.linkedin.thirdeye.datasource.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Lists;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class CollectionsCache {

  private AtomicReference<List<String>> collectionsRef;
  private DatasetConfigManager datasetConfigDAO;
  private List<String> whitelistCollections;
  private static final Logger LOG = LoggerFactory.getLogger(CollectionsCache.class);


  public CollectionsCache(DatasetConfigManager datasetConfigDAO, ThirdEyeConfiguration config) {
    this.collectionsRef = new AtomicReference<>();
    this.datasetConfigDAO = datasetConfigDAO;
    this.whitelistCollections = config.getWhitelistCollections();
  }


  public List<String> getCollections() {
    return collectionsRef.get();
  }

  public void loadCollections() {
    List<String> collections = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(whitelistCollections)) {
      for (String collection : whitelistCollections) {
        DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
        if (datasetConfig == null || !datasetConfig.isActive()) {
          LOG.info("Skipping collection {} due to missing dataset config or status inactive", collection);
          continue;
        }
        collections.add(collection);
      }
    } else {
      List<DatasetConfigDTO> datasetConfigs = datasetConfigDAO.findActive();
      for (DatasetConfigDTO datasetConfigDTO : datasetConfigs) {
        collections.add(datasetConfigDTO.getDataset());
      }
    }

    LOG.info("Loading collections {}", collections);
    collectionsRef.set(collections);
  }

}

