package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

import org.hibernate.StaleObjectStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);

  private ExecutorService taskExecutorService;

  private final AnomalyTaskDAO anomalyTaskDAO;
  private final AnomalyResultDAO anomalyResultDAO;
  private final AnomalyMergedResultDAO mergedResultDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private TaskContext taskContext;
  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;
  private long workerId;

  volatile boolean shutdown = false;
  private static int MAX_PARALLEL_TASK = 3;

  public TaskDriver(ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration, AnomalyJobDAO anomalyJobDAO,
      AnomalyTaskDAO anomalyTaskDAO, AnomalyResultDAO anomalyResultDAO, AnomalyMergedResultDAO mergedResultDAO,
      AnomalyFunctionFactory anomalyFunctionFactory) {
    this.workerId = thirdEyeAnomalyConfiguration.getId();
    this.anomalyTaskDAO = anomalyTaskDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.mergedResultDAO = mergedResultDAO;
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    taskExecutorService = Executors.newFixedThreadPool(MAX_PARALLEL_TASK);

    taskContext = new TaskContext();
    taskContext.setAnomalyJobDAO(anomalyJobDAO);
    taskContext.setAnomalyTaskDAO(anomalyTaskDAO);
    taskContext.setResultDAO(anomalyResultDAO);
    taskContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    taskContext.setMergedResultDAO(mergedResultDAO);
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeAnomalyConfiguration);
  }

  public void start() throws Exception {
    List<Callable<Void>> callables = new ArrayList<>();
    for (int i = 0; i < MAX_PARALLEL_TASK; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          while (!shutdown) {

            LOG.info(Thread.currentThread().getId() + " : Finding next task to execute for threadId:{}",
                Thread.currentThread().getId());

            try {
              // select a task to execute, and update it to RUNNING
              AnomalyTaskSpec anomalyTaskSpec = selectAndUpdate();
              LOG.info(Thread.currentThread().getId() + " : Executing task: {} {}", anomalyTaskSpec.getId(),
                  anomalyTaskSpec.getTaskInfo());

              // execute the selected task
              TaskType taskType = anomalyTaskSpec.getTaskType();
              TaskRunner taskRunner = TaskRunnerFactory.getTaskRunnerFromTaskType(taskType);
              TaskInfo taskInfo = TaskInfoFactory.getTaskInfoFromTaskType(taskType, anomalyTaskSpec.getTaskInfo());
              LOG.info(Thread.currentThread().getId() + " : Task Info {}", taskInfo);
              List<TaskResult> taskResults = taskRunner.execute(taskInfo, taskContext);
              LOG.info(Thread.currentThread().getId() + " : DONE Executing task: {}", anomalyTaskSpec.getId());

              // update status to COMPLETED
              updateStatusAndTaskEndTime(anomalyTaskSpec.getId(), TaskStatus.RUNNING, TaskStatus.COMPLETED);
            } catch (Exception e) {
              LOG.error("Exception in electing and executing task", e);
            }
          }
          return null;
        }
      };
      callables.add(callable);
    }
    for (Callable<Void> callable : callables) {
      taskExecutorService.submit(callable);
    }
    LOG.info(Thread.currentThread().getId() + " : Started task driver");
  }

  public void stop() {
    taskExecutorService.shutdown();
  }

  private AnomalyTaskSpec selectAndUpdate() {
    LOG.info(Thread.currentThread().getId() + " : Starting selectAndUpdate {}", Thread.currentThread().getId());
    AnomalyTaskSpec acquiredTask = null;
    LOG.info(Thread.currentThread().getId() + " : Trying to find a task to execute");
    do {

      List<AnomalyTaskSpec> anomalyTasks = new ArrayList<>();
      anomalyTasks = anomalyTaskDAO.findByStatusOrderByCreateTimeAscending(TaskStatus.WAITING);
      if (anomalyTasks.size() > 0)
        LOG.info(Thread.currentThread().getId() + " : Found {} tasks in waiting state", anomalyTasks.size());

      for (AnomalyTaskSpec anomalyTaskSpec : anomalyTasks) {
        LOG.info(Thread.currentThread().getId() + " : Trying to acquire task : {}", anomalyTaskSpec.getId());

        boolean success = false;
        try {
          success = anomalyTaskDAO.updateStatusAndWorkerId(workerId, anomalyTaskSpec.getId(),TaskStatus.WAITING,
              TaskStatus.RUNNING);
          LOG.info(Thread.currentThread().getId() + " : Task acquired success: {}", success);
        } catch (OptimisticLockException | RollbackException | StaleObjectStateException e) {
          LOG.warn("Optimistic lock exception in acquiring task by threadId {} and workerId {}",
              Thread.currentThread().getId(), workerId);
        }
        if (success) {
          acquiredTask = anomalyTaskSpec;
          break;
        }
      }
    } while (acquiredTask == null);
    LOG.info(Thread.currentThread().getId() + " : Acquired task ======" + acquiredTask);

    return acquiredTask;
  }

  private void updateStatusAndTaskEndTime(long taskId, TaskStatus oldStatus, TaskStatus newStatus) throws Exception {
    LOG.info("{} : Starting updateStatus {}", Thread.currentThread().getId(), Thread.currentThread().getId());

    try {
      anomalyTaskDAO.updateStatusAndTaskEndTime(taskId, oldStatus, newStatus, System.currentTimeMillis());
      LOG.info("{} : updated status {}", Thread.currentThread().getId(), newStatus);
    } catch (Exception e) {
      LOG.error("Exception in updating status and task end time", e);
    }
  }

}
