package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;

public class TestAnomalyJobManager extends AbstractManagerTestBase {

  private Long anomalyJobId1;
  private Long anomalyJobId2;

  @Test
  public void testCreate() {
    anomalyJobId1 = anomalyJobDAO.save(getTestJobSpec());
    Assert.assertNotNull(anomalyJobId1);
    anomalyJobId2 = anomalyJobDAO.save(getTestJobSpec());
    Assert.assertNotNull(anomalyJobId2);
    printAll("After insert");
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAll() {
    List<JobDTO> anomalyJobs = anomalyJobDAO.findAll();
    Assert.assertEquals(anomalyJobs.size(), 2);
  }

  @Test(dependsOnMethods = { "testFindAll" })
  public void testUpdateStatusAndJobEndTime() {
    JobStatus status = JobStatus.COMPLETED;
    long jobEndTime = System.currentTimeMillis();
    anomalyJobDAO.updateStatusAndJobEndTime(anomalyJobId1, status, jobEndTime);
    JobDTO anomalyJob = anomalyJobDAO.findById(anomalyJobId1);
    Assert.assertEquals(anomalyJob.getStatus(), status);
    Assert.assertEquals(anomalyJob.getScheduleEndTime(), jobEndTime);
    printAll("After testUpdateStatusAndJobEndTime");
  }

  @Test(dependsOnMethods = {"testUpdateStatusAndJobEndTime"})
  public void testFindByStatus() {
    JobStatus status = JobStatus.COMPLETED;
    List<JobDTO> anomalyJobs = anomalyJobDAO.findByStatus(status);
    Assert.assertEquals(anomalyJobs.size(), 1);
    Assert.assertEquals(anomalyJobs.get(0).getStatus(), status);
  }

  private void printAll(String msg) {
    List<JobDTO> allAnomalyJobs = anomalyJobDAO.findAll();
    System.out.println("START:ALL JOB after:"+ msg);
    for(JobDTO jobDTO:allAnomalyJobs){
      System.out.println(jobDTO);
    }
    System.out.println("END:ALL JOB after:"+ msg);
  }

  @Test(dependsOnMethods = { "testFindByStatus" })
  public void testDeleteRecordsOlderThanDaysWithStatus() {
    JobStatus status = JobStatus.COMPLETED;
    int numRecordsDeleted = anomalyJobDAO.deleteRecordsOlderThanDaysWithStatus(0, status);
    Assert.assertEquals(numRecordsDeleted, 1);
    List<JobDTO> anomalyJobs = anomalyJobDAO.findByStatus(status);
    Assert.assertEquals(anomalyJobs.size(), 0);
  }
}
