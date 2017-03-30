package com.linkedin.thirdeye.anomaly.classification;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassificationJobScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ClassificationJobScheduler.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  public void start() {
    LOG.info("Starting anomaly classification service");
    this.scheduledExecutorService.scheduleWithFixedDelay(this, 0, 15, TimeUnit.MINUTES);
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  @Override
  public void run() {
    ClassificationConfigManager classificationConfigDAO = DAO_REGISTRY.getClassificationConfigDAO();

    List<ClassificationConfigDTO> classificationConfigs = classificationConfigDAO.findActives();

    for (ClassificationConfigDTO classificationConfig : classificationConfigs) {
      LOG.info("Running classifier: {}", classificationConfig);
      mainMetricTimeBasedGrouping(classificationConfig);
    }
  }

  private void mainMetricTimeBasedGrouping(ClassificationConfigDTO classificationConfig) {
    AnomalyFunctionManager anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    JobManager jobDAO = DAO_REGISTRY.getJobDAO();

    // TODO: Modularize this grouping logic and make it swappable
    // Get all involved anomaly functions that are activated
    List<AnomalyFunctionDTO> involvedAnomalyFunctions = new ArrayList<>();
    List<Long> functionIdList = classificationConfig.getFunctionIdList();
    for (long functionId : functionIdList) {
      AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
      if (anomalyFunctionDTO.getIsActive()) {
        involvedAnomalyFunctions.add(anomalyFunctionDTO);
      }
    }
    // TODO: Determine if funnel effect has main metric. If it does not, then remove the block below
    if (!functionIdList.contains(classificationConfig.getMainFunctionId())) {
      AnomalyFunctionDTO mainAnomalyFunction = anomalyFunctionDAO.findById(classificationConfig.getMainFunctionId());
      if (mainAnomalyFunction.getIsActive()) {
        involvedAnomalyFunctions.add(mainAnomalyFunction);
      } else {
        LOG.info("Main anomaly function is not activated. Classification job: {} is skipped.", classificationConfig);
        return;
      }
    }

    // Check the latest detection time among all anomaly functions in this classification config
    long minEndTime = Long.MAX_VALUE;
    for (AnomalyFunctionDTO anomalyFunctionDTO : involvedAnomalyFunctions) {
      JobDTO job = jobDAO.findLatestCompletedAnomalyJobByFunctionId(anomalyFunctionDTO.getId());
      if (job != null) {
        minEndTime = Math.min(minEndTime, job.getWindowEndTime());
      } else {
        minEndTime = Long.MAX_VALUE;
        LOG.warn("Could not find most recent executed job for function {}; aborting job for classification config {}",
            anomalyFunctionDTO.getId(), classificationConfig);
        break;
      }
    }
    if (minEndTime == Long.MAX_VALUE) {
      return;
    }

    // Get the most recent classification job
    JobDTO classificationJobDTO = jobDAO.findLatestCompletedGroupingJobById(classificationConfig.getId());
    if (classificationJobDTO != null) {
      long recentEndTime = classificationJobDTO.getWindowEndTime();
      // skip this job if we have processed the latest available window
      if (recentEndTime >= minEndTime) {
        return;
      }

      // create classification job
      long startTime = recentEndTime;
      long endTime = minEndTime;
      ClassificationJobContext jobContext = new ClassificationJobContext();
      jobContext.setWindowStartTime(startTime);
      jobContext.setWindowEndTime(endTime);
      jobContext.setConfigDTO(classificationConfig);
      ClassificationJobRunner jobRunner = new ClassificationJobRunner(jobContext);
      jobRunner.run();
    }
  }
}



















