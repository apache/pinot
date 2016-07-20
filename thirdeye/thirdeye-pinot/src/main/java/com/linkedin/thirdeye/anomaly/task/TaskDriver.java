package com.linkedin.thirdeye.anomaly.task;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.detector.db.entity.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ExecutorService taskExecutorService;

  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyResultDAO anomalyResultDAO;
  private AnomalyFunctionRelationDAO anomalyFunctionRelationDAO;
  private SessionFactory sessionFactory;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private TaskContext taskContext;
  private long workerId;

  volatile boolean shutdown = false;
  private static int MAX_PARALLEL_TASK = 3;

  public TaskDriver(long workerId, AnomalyTaskSpecDAO anomalyTaskSpecDAO, AnomalyResultDAO anomalyResultDAO,
      AnomalyFunctionRelationDAO anomalyFunctionRelationDAO, SessionFactory sessionFactory,
      AnomalyFunctionFactory anomalyFunctionFactory) {
    this.workerId = workerId;
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.anomalyFunctionRelationDAO = anomalyFunctionRelationDAO;
    this.sessionFactory = sessionFactory;
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    taskExecutorService = Executors.newFixedThreadPool(MAX_PARALLEL_TASK);

    taskContext = new TaskContext();
    taskContext.setRelationDAO(anomalyFunctionRelationDAO);
    taskContext.setResultDAO(anomalyResultDAO);
    taskContext.setSessionFactory(sessionFactory);
    taskContext.setAnomalyFunctionFactory(anomalyFunctionFactory);

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
              Constructor<?> taskRunnerConstructor = TaskExecutorFactory.getTaskExecutorClassFromTaskType(taskType)
                  .getConstructor();
              TaskRunner taskRunner = (TaskRunner) taskRunnerConstructor.newInstance();

              TaskInfo taskInfo = OBJECT_MAPPER.readValue(anomalyTaskSpec.getTaskInfo(), TaskInfo.class);
              LOG.info(Thread.currentThread().getId() + " : Task Info {}", taskInfo);
              List<TaskResult> taskResults = taskRunner.execute(taskInfo, taskContext);
              LOG.info(Thread.currentThread().getId() + " : DONE Executing task: {}", anomalyTaskSpec.getId());

              // update status to COMPLETED
              updateStatus(anomalyTaskSpec.getId(), TaskStatus.RUNNING, TaskStatus.COMPLETED);
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

  private AnomalyTaskSpec selectAndUpdate() throws Exception {
    LOG.info(Thread.currentThread().getId() + " : Starting selectAndUpdate {}", Thread.currentThread().getId());
    AnomalyTaskSpec acquiredTask = null;
    LOG.info(Thread.currentThread().getId() + " : Trying to find a task to execute");
    do {
      Session session = sessionFactory.openSession();
      ManagedSessionContext.bind(session);
      Transaction transaction = null;
      try {

        List<AnomalyTaskSpec> anomalyTasks =
            anomalyTaskSpecDAO.findByStatusOrderByCreateTimeAscending(TaskStatus.WAITING);
        if (anomalyTasks.size() > 0)
          LOG.info(Thread.currentThread().getId() + " : Found {} tasks in waiting state", anomalyTasks.size());

        for (AnomalyTaskSpec anomalyTaskSpec : anomalyTasks) {
          transaction = session.beginTransaction();
          LOG.info(Thread.currentThread().getId() + " : Trying to acquire task : {}", anomalyTaskSpec.getId());
          boolean success = anomalyTaskSpecDAO.updateStatusAndWorkerId(workerId, anomalyTaskSpec.getId(),
              TaskStatus.WAITING, TaskStatus.RUNNING);
          LOG.info(Thread.currentThread().getId() + " : Task acquired success: {}", success);
          if (success) {
            acquiredTask = anomalyTaskSpec;
            break;
          }
        }
        if (!transaction.wasCommitted()) {
          transaction.commit();
        }
        Thread.sleep(1000);
      } catch (Exception e) {
        if (transaction != null) {
          transaction.rollback();
        }
      } finally {
        session.close();
        ManagedSessionContext.unbind(sessionFactory);
      }
    } while (acquiredTask == null);
    LOG.info(Thread.currentThread().getId() + " : Acquired task ======" + acquiredTask);

    return acquiredTask;
  }

  private void updateStatus(long taskId, TaskStatus oldStatus, TaskStatus newStatus) throws Exception {
    LOG.info(Thread.currentThread().getId() + " : Starting updateStatus {}", Thread.currentThread().getId());

    Session session = sessionFactory.openSession();
    ManagedSessionContext.bind(session);
    Transaction transaction = null;
    try {
      transaction = session.beginTransaction();

      boolean updateStatus = anomalyTaskSpecDAO.updateStatus(taskId, oldStatus, newStatus);
      LOG.info(Thread.currentThread().getId() + " : update status {}", updateStatus);

      if (!transaction.wasCommitted()) {
        transaction.commit();
      }
    } catch (Exception e) {
      if (transaction != null) {
        transaction.rollback();
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
  }

}
