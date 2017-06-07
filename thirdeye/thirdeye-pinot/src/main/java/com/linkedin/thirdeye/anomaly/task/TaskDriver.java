package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);
  private static final Random RANDOM = new Random();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private ExecutorService taskExecutorService;

  private final TaskManager anomalyTaskDAO;
  private TaskContext taskContext;
  private long workerId;
  private final Set<TaskStatus> allowedOldTaskStatus = new HashSet<>();
  private TaskDriverConfiguration driverConfiguration;

  private volatile boolean shutdown = false;

  public TaskDriver(ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration,
      AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory,
      AnomalyClassifierFactory anomalyClassifierFactory) {
    driverConfiguration = thirdEyeAnomalyConfiguration.getTaskDriverConfiguration();
    workerId = thirdEyeAnomalyConfiguration.getId();
    anomalyTaskDAO = DAO_REGISTRY.getTaskDAO();
    taskExecutorService = Executors.newFixedThreadPool(driverConfiguration.getMaxParallelTasks());
    taskContext = new TaskContext();
    taskContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeAnomalyConfiguration);
    taskContext.setAlertFilterFactory(alertFilterFactory);
    taskContext.setAnomalyClassifierFactory(anomalyClassifierFactory);
    allowedOldTaskStatus.add(TaskStatus.FAILED);
    allowedOldTaskStatus.add(TaskStatus.WAITING);
  }

  public void start() throws Exception {
    for (int i = 0; i < driverConfiguration.getMaxParallelTasks(); i++) {
      Callable callable = new Callable() {
        @Override public Object call() throws Exception {
          while (!shutdown) {
            LOG.info("Thread {} : Finding next task to execute.", Thread.currentThread().getId());

            // select a task to execute, and update it to RUNNING
            TaskDTO anomalyTaskSpec = TaskDriver.this.acquireTask();

            if (anomalyTaskSpec != null) { // a task has acquired and we must finish executing it before termination
              try {
                LOG.info("Thread {} : Executing task: {} {}", Thread.currentThread().getId(), anomalyTaskSpec.getId(),
                    anomalyTaskSpec.getTaskInfo());

                // execute the selected task
                TaskType taskType = anomalyTaskSpec.getTaskType();
                TaskRunner taskRunner = TaskRunnerFactory.getTaskRunnerFromTaskType(taskType);
                TaskInfo taskInfo = TaskInfoFactory.getTaskInfoFromTaskType(taskType, anomalyTaskSpec.getTaskInfo());
                LOG.info("Thread {} : Task Info {}", Thread.currentThread().getId(), taskInfo);
                List<TaskResult> taskResults = taskRunner.execute(taskInfo, taskContext);
                LOG.info("Thread {} : DONE Executing task: {}", Thread.currentThread().getId(), anomalyTaskSpec.getId());
                // update status to COMPLETED
                TaskDriver.this.updateStatusAndTaskEndTime(anomalyTaskSpec.getId(), TaskStatus.RUNNING, TaskStatus.COMPLETED);
              } catch (Exception e) {
                LOG.error("Exception in electing and executing task", e);
                try {
                  // update task status failed
                  TaskDriver.this.updateStatusAndTaskEndTime(anomalyTaskSpec.getId(), TaskStatus.RUNNING, TaskStatus.FAILED);
                } catch (Exception e1) {
                  LOG.error("Error in updating failed status", e1);
                }
              }
            }
          }
          return 0;
        }
      };
      taskExecutorService.submit(callable);
      LOG.info("Thread {} : Started task driver", Thread.currentThread().getId());
    }
  }

  public void shutdown() {
    shutdown = true;
    AnomalyUtils.safelyShutdownExecutionService(taskExecutorService, this.getClass());
  }

  /**
   * Returns a TaskDTO if a task is successfully acquired; returns null if system is shutting down.
   *
   * @return null if system is shutting down.
   */
  private TaskDTO acquireTask() {
    LOG.info("Thread {} : Trying to find a task to execute", Thread.currentThread().getId());
    while (!shutdown) {
      List<TaskDTO> anomalyTasks = new ArrayList<>();
      boolean hasFetchError = false;
      try {
        boolean orderAscending = System.currentTimeMillis() % 2 == 0;
        anomalyTasks = anomalyTaskDAO
            .findByStatusOrderByCreateTime(TaskStatus.WAITING, driverConfiguration.getTaskFetchSizeCap(),
                orderAscending);
      } catch (Exception e) {
        hasFetchError = true;
        anomalyTasks.clear();
        LOG.warn("Exception found in fetching new tasks", e);
      }

      if (CollectionUtils.isNotEmpty(anomalyTasks)) {
        LOG.info("Thread {} : Found {} tasks in waiting state", Thread.currentThread().getId(), anomalyTasks.size());

        for (int i = 0; i < anomalyTasks.size() && !shutdown; i++) {
          TaskDTO anomalyTaskSpec = anomalyTasks.get(i);

          boolean success = false;
          try {
            success = anomalyTaskDAO
                .updateStatusAndWorkerId(workerId, anomalyTaskSpec.getId(), allowedOldTaskStatus,
                    TaskStatus.RUNNING, anomalyTaskSpec.getVersion());
            LOG.info("Thread {} : Trying to acquire task id [{}], success status: [{}] with version [{}]",
                Thread.currentThread().getId(), anomalyTaskSpec.getId(), success, anomalyTaskSpec.getVersion());
          } catch (Exception e) {
            LOG.warn("Thread {} : Got exception when acquiring task. (Worker Id: {})", Thread.currentThread().getId(),
                workerId, e);
          }
          if (success) {
            LOG.info("Thread {} has acquired task: {}", Thread.currentThread().getId(), anomalyTaskSpec);
            return anomalyTaskSpec;
          }
        }
      }

      if (shutdown) {
        return null;
      } else {
        long sleepTime = driverConfiguration.getTaskFailureDelayInMillis();
        if (!hasFetchError) {
          // sleep for few seconds if not tasks found - avoid cpu thrashing
          // also add some extra random number of milli seconds to allow threads to start at different times
          sleepTime = driverConfiguration.getNoTaskDelayInMillis() + RANDOM
              .nextInt(driverConfiguration.getRandomDelayCapInMillis());
        }
        try {
          LOG.debug("No tasks found to execute, sleeping for {} MS", sleepTime);
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          if (!shutdown) {
            LOG.warn(e.getMessage(), e);
          }
        }
      }
    }
    return null;
  }

  private void updateStatusAndTaskEndTime(long taskId, TaskStatus oldStatus, TaskStatus newStatus)
      throws Exception {
    LOG.info("Thread {} : Starting updateStatus for task id {}", Thread.currentThread().getId(), taskId);
    try {
      anomalyTaskDAO
          .updateStatusAndTaskEndTime(taskId, oldStatus, newStatus, System.currentTimeMillis());
      LOG.info("Thread {} : Updated status {}", Thread.currentThread().getId(), newStatus);
    } catch (Exception e) {
      LOG.error("Exception in updating status and task end time", e);
    }
  }
}
