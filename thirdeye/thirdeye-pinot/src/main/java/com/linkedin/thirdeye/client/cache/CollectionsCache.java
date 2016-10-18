package com.linkedin.thirdeye.client.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class CollectionsCache {

  private AtomicReference<List<String>> collectionsRef;
  private DatasetConfigManager datasetConfigDAO;


  public CollectionsCache(DatasetConfigManager datasetConfigDAO) {
    this.collectionsRef = new AtomicReference<>();
    this.datasetConfigDAO = datasetConfigDAO;
  }


  public List<String> getCollections() {
    return collectionsRef.get();
  }

  public void loadCollections() {

    List<DatasetConfigDTO> datasetConfigs = datasetConfigDAO.findActive();
    List<String> collections = new ArrayList<>();
    for (DatasetConfigDTO datasetConfigDTO : datasetConfigs) {
      collections.add(datasetConfigDTO.getDataset());
    }
    collectionsRef.set(collections);
  }


}

