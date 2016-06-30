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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
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
  private TaskContext taskContext;

  volatile boolean shutdown = false;
  private static int MAX_PARALLEL_TASK = 3;
  private AnomalyFunctionFactory anomalyFunctionFactory;

  public TaskDriver(AnomalyTaskSpecDAO anomalyTaskSpecDAO, AnomalyResultDAO anomalyResultDAO,
      AnomalyFunctionRelationDAO anomalyFunctionRelationDAO, SessionFactory sessionFactory,
      AnomalyFunctionFactory anomalyFunctionFactory) {
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
            LOG.info("Executing task: {} {}", anomalyTaskSpec.getTaskId(),
                anomalyTaskSpec.getTaskInfo());
            TaskRunner taskRunner = new TaskRunner(anomalyFunctionFactory);
            TaskInfo taskInfo = null;
            try {
              taskInfo = OBJECT_MAPPER.readValue(anomalyTaskSpec.getTaskInfo(), TaskInfo.class);
            } catch (Exception e) {
              LOG.error("Exception in converting taskInfo string to TaskInfo {}",
                  anomalyTaskSpec.getTaskInfo(), e);
            }
            LOG.info("Task Info {}", taskInfo);
            List<AnomalyResult> anomalyResults = taskRunner.execute(taskInfo, taskContext);
            LOG.info("DONE Executing task: {}", anomalyTaskSpec.getTaskId());
            Session session = sessionFactory.openSession();
            ManagedSessionContext.bind(session);
            Transaction transaction = null;
            try {
              transaction = session.beginTransaction();
              anomalyTaskSpecDAO.updateStatus(anomalyTaskSpec.getTaskId(), JobStatus.RUNNING,
                  JobStatus.COMPLETED);
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
        LOG.debug("Found {} tasks in waiting state", anomalyTasks.size());
        for (AnomalyTaskSpec anomalyTaskSpec : anomalyTasks) {
          transaction = session.beginTransaction();
          LOG.info("Trying to acquire task : {}", anomalyTaskSpec.getTaskId());
          boolean success = anomalyTaskSpecDAO.updateStatus(anomalyTaskSpec.getTaskId(),
              JobStatus.WAITING, JobStatus.RUNNING);
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
