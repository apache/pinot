package com.linkedin.thirdeye.anomaly.monitor;

import io.dropwizard.hibernate.UnitOfWork;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;

public class MonitorTaskRunner implements TaskRunner {


  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private SessionFactory sessionFactory;

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {

    anomalyJobSpecDAO = taskContext.getAnomalyJobSpecDAO();
    anomalyTaskSpecDAO = taskContext.getAnomalyTaskSpecDAO();
    sessionFactory = taskContext.getSessionFactory();

    MonitorTaskInfo monitorTaskInfo = (MonitorTaskInfo) taskInfo;
    MonitorType monitorType = monitorTaskInfo.getMonitorType();
    if (monitorType.equals(MonitorType.UPDATE)) {
      executeMonitorUpdate(monitorTaskInfo);
    } else if (monitorType.equals(MonitorType.EXPIRE)) {
      executeMonitorExpire(monitorTaskInfo);
    } else {
      throw new UnsupportedOperationException("Monitor task must be of type UPDATE/EXPIRE, found " + monitorType);
    }
    return null;
  }

  @UnitOfWork
  private void executeMonitorUpdate(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor udpate {}", monitorTaskInfo);
    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);

      Long jobExecutionId = monitorTaskInfo.getJobExecutionId();
      List<AnomalyTaskSpec> anomalyTaskSpecs = anomalyTaskSpecDAO
          .findByJobIdAndStatusNotIn(jobExecutionId, TaskStatus.COMPLETED);
      if (anomalyTaskSpecs.isEmpty()) {
        anomalyJobSpecDAO.updateStatusAndJobEndTime(jobExecutionId, JobStatus.COMPLETED, System.currentTimeMillis());
        LOG.info("COMPLETED anomaly job {}", jobExecutionId);
      }
    } catch (Exception e) {
      LOG.error("Exception in monitor update task", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
  }

  @UnitOfWork
  private void executeMonitorExpire(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor expire {}", monitorTaskInfo);
    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);

      int expireDaysAgo = monitorTaskInfo.getExpireDaysAgo();
      int numAnomalyJobsDeleted = anomalyJobSpecDAO.deleteRecordsOlderThanDaysWithStatus(expireDaysAgo, JobStatus.COMPLETED);
      int numAnomalyTasksDeleted = anomalyTaskSpecDAO.deleteRecordsOlderThanDaysWithStatus(expireDaysAgo, TaskStatus.COMPLETED);
      LOG.info("Deleted {} anomaly jobs and {} anomaly tasks", numAnomalyJobsDeleted, numAnomalyTasksDeleted);
    } catch (Exception e) {
      LOG.error("Exception in monitor expire task", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
  }

}
