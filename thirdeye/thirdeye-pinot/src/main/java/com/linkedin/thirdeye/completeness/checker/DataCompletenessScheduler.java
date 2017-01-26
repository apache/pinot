package com.linkedin.thirdeye.completeness.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * The scheduler which will run periodically and schedule jobs and tasks for data completeness
 */
public class DataCompletenessScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessScheduler.class);

  private ScheduledExecutorService scheduledExecutorService;
  private DataCompletenessJobRunner dataCompletenessJobRunner;
  private DataCompletenessJobContext dataCompletenessJobContext;
  private List<String> datasetsToCheck = new ArrayList<>();

  public DataCompletenessScheduler(String datasetsToCheck) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    if (StringUtils.isNotBlank(datasetsToCheck)) {
      this.datasetsToCheck = Lists.newArrayList(datasetsToCheck.split(","));
    }
  }


  public void start() {

    LOG.info("Starting data completeness checker service");

    dataCompletenessJobContext = new DataCompletenessJobContext();
    dataCompletenessJobContext.setDatasetsToCheck(datasetsToCheck);
    dataCompletenessJobRunner = new DataCompletenessJobRunner(dataCompletenessJobContext);

    scheduledExecutorService.scheduleWithFixedDelay(dataCompletenessJobRunner, 0, 15, TimeUnit.MINUTES);
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

}
