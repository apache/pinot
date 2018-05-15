package com.linkedin.thirdeye.datasource.cache;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetListCache {

  private List<String> datasetListRef;
  private DatasetConfigManager datasetConfigDAO;
  private static final Logger LOG = LoggerFactory.getLogger(DatasetListCache.class);

  private final Object updateLock = new Object();

  private final long refreshInterval;

  private long nextUpdate = Long.MIN_VALUE;

  public DatasetListCache(DatasetConfigManager datasetConfigDAO, long refreshInterval) {
    this.datasetListRef = new ArrayList<>();
    this.datasetConfigDAO = datasetConfigDAO;
    this.refreshInterval = refreshInterval;
  }

  public List<String> getDatasets() {
    // test and test and set
    if (this.nextUpdate <= System.currentTimeMillis()) {
      synchronized (this.updateLock) {
        if (this.nextUpdate <= System.currentTimeMillis()) {
          this.nextUpdate = System.currentTimeMillis() + this.refreshInterval;

          List<String> datasets = new ArrayList<>();
          for (DatasetConfigDTO dataset : this.datasetConfigDAO.findAll()) {
            datasets.add(dataset.getDataset());
          }

          this.datasetListRef = datasets;
        }
      }
    }

    return this.datasetListRef;
  }

  public void expire() {
    this.nextUpdate = Long.MIN_VALUE;
  }
}

