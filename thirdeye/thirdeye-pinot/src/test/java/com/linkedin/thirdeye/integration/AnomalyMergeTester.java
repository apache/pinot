package com.linkedin.thirdeye.integration;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeExecutor;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import java.io.File;
import java.io.IOException;

public class AnomalyMergeTester {

  private static final String dbConfig = "/opt/Code/thirdeye-configs/sandbox-configs/persistence.yml";
  public static void main(String[] args) throws IOException, InterruptedException {
    PersistenceUtil.init(new File(dbConfig));
    AnomalyMergeConfig config = new AnomalyMergeConfig();
    config.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
    config.setMergeDuration(-1);
    config.setSequentialAllowedGap(2 * 60 * 60_000); // 2 hours

    AnomalyResultDAO resultDAO = PersistenceUtil.getInstance(AnomalyResultDAO.class);
    AnomalyMergedResultDAO mergedResultDAO = PersistenceUtil.getInstance(AnomalyMergedResultDAO.class);
    AnomalyFunctionDAO functionDAO = PersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    AnomalyMergeExecutor executor = new AnomalyMergeExecutor(mergedResultDAO, functionDAO, resultDAO, config);
    while (true) {
      executor.updateMergedResults();
      System.out.println("Sleeping for 1 min");
      Thread.sleep(60_000);
    }
  }
}
