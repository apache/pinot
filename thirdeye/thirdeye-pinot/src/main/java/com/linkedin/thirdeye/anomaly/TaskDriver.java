package com.linkedin.thirdeye.anomaly;

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

import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;

public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);

  private ExecutorService taskExecutorService;

  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private SessionFactory sessionFactory;
  volatile boolean shutdown = false;
  private static int MAX_PARALLEL_TASK = 3;

  public TaskDriver(AnomalyTaskSpecDAO anomalyTaskSpecDAO, SessionFactory sessionFactory) {
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
    this.sessionFactory = sessionFactory;
    taskExecutorService = Executors.newFixedThreadPool(MAX_PARALLEL_TASK);

  }

  public void start() throws Exception {
    List<Callable<Void>> callables = new ArrayList<>();
    for (int i = 0; i < MAX_PARALLEL_TASK; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          while (!shutdown) {
            LOG.info("Finding next task to execute for threadId:{}",
                Thread.currentThread().getId());
            AnomalyTaskSpec anomalyTaskSpec = selectAndUpdate();
            LOG.info("Executing task: {}", anomalyTaskSpec.getTaskId());
            TaskRunner taskRunner = new TaskRunner();
            // return taskRunner.execute(taskInfo, taskContext);
            LOG.info("DONE Executing task: {}", anomalyTaskSpec.getTaskId());
          }
          return null;
        }
      };
      callables.add(callable);
    }
    for (Callable<Void> callable : callables) {
      taskExecutorService.submit(callable);
    }
    System.out.println("Started task driver");
  }

  public void stop() {
    taskExecutorService.shutdown();
  }

  private AnomalyTaskSpec selectAndUpdate() throws Exception {
    LOG.info("Starting selectAndUpdate {}", Thread.currentThread().getId());
    AnomalyTaskSpec acquiredTask = null;
    LOG.info("Trying to find a task to execute");
    do {
      Session session = sessionFactory.openSession();
      ManagedSessionContext.bind(session);
      Transaction transaction = null;
      try {

        List<AnomalyTaskSpec> anomalyTasks =
            anomalyTaskSpecDAO.findByStatusOrderByCreateTimeAscending(JobStatus.WAITING);
        LOG.info("Found {} tasks in waiting state", anomalyTasks.size());
        for (AnomalyTaskSpec anomalyTaskSpec : anomalyTasks) {
          transaction = session.beginTransaction();
          LOG.info("Trying to acquire task : {}", anomalyTaskSpec.getTaskId());
          boolean success = anomalyTaskSpecDAO.updateStatus(anomalyTaskSpec.getTaskId());
          LOG.info("Task acquired success: {}", success);
          if (success) {
            acquiredTask = anomalyTaskSpec;
            if (!transaction.wasCommitted()) {
              transaction.commit();
            }
            break;
          }
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
    LOG.info("Acquired task ======" + acquiredTask);

    return acquiredTask;
  }

}
