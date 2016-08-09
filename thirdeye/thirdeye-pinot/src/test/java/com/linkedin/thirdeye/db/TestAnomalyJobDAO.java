package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.db.dao.AbstractDbTestBase;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;

import java.util.List;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyJobDAO extends AbstractDbTestBase {

  private Long anomalyJobId1;
  private Long anomalyJobId2;

  @Test
  public void testCreate() {
    anomalyJobId1 = anomalyJobDAO.save(getTestFunctionSpec());
    Assert.assertNotNull(anomalyJobId1);
    anomalyJobId2 = anomalyJobDAO.save(getTestFunctionSpec());
    Assert.assertNotNull(anomalyJobId2);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAll() {
    List<AnomalyJobSpec> anomalyJobs = anomalyJobDAO.findAll();
    Assert.assertEquals(anomalyJobs.size(), 2);
  }

  @Test(dependsOnMethods = { "testFindAll" })
  public void testUpdateStatusAndJobEndTime() {
    JobStatus status = JobStatus.COMPLETED;
    long jobEndTime = System.currentTimeMillis();
    anomalyJobDAO.updateStatusAndJobEndTime(anomalyJobId1, status, jobEndTime);
    AnomalyJobSpec anomalyJob = anomalyJobDAO.findById(anomalyJobId1);
    Assert.assertEquals(anomalyJob.getStatus(), status);
    Assert.assertEquals(anomalyJob.getScheduleEndTime(), jobEndTime);
  }

  @Test(dependsOnMethods = {"testUpdateStatusAndJobEndTime"})
  public void testFindByStatus() {
    JobStatus status = JobStatus.COMPLETED;
    List<AnomalyJobSpec> anomalyJobs = anomalyJobDAO.findByStatus(status);
    Assert.assertEquals(anomalyJobs.size(), 1);
    Assert.assertEquals(anomalyJobs.get(0).getStatus(), status);
  }

  @Test(dependsOnMethods = { "testFindByStatus" })
  public void testDeleteRecordsOlderThanDaysWithStatus() {
    JobStatus status = JobStatus.COMPLETED;
    int numRecordsDeleted = anomalyJobDAO.deleteRecordsOlderThanDaysWithStatus(0, status);
    Assert.assertEquals(numRecordsDeleted, 1);
    List<AnomalyJobSpec> anomalyJobs = anomalyJobDAO.findByStatus(status);
    Assert.assertEquals(anomalyJobs.size(), 0);
  }

  static AnomalyJobSpec getTestFunctionSpec() {
    AnomalyJobSpec jobSpec = new AnomalyJobSpec();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobStatus.SCHEDULED);
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setWindowStartTime(new DateTime().minusHours(20).getMillis());
    jobSpec.setWindowEndTime(new DateTime().minusHours(10).getMillis());
    return jobSpec;
  }
}
