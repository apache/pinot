package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;

public class TestAnomalyJobManager {

  private Long anomalyJobId1;
  private Long anomalyJobId2;
  private Long anomalyJobId3;

  private DAOTestBase testDAOProvider;
  private JobManager jobDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    jobDAO = DAORegistry.getInstance().getJobDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {
    anomalyJobId1 = jobDAO.save(DaoTestUtils.getTestJobSpec());
    Assert.assertNotNull(anomalyJobId1);
    anomalyJobId2 = jobDAO.save(DaoTestUtils.getTestJobSpec());
    Assert.assertNotNull(anomalyJobId2);
    anomalyJobId3 = jobDAO.save(DaoTestUtils.getTestJobSpec());
    Assert.assertNotNull(anomalyJobId3);
    printAll("After insert");
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAll() {
    List<JobDTO> anomalyJobs = jobDAO.findAll();
    Assert.assertEquals(anomalyJobs.size(), 3);
  }

  @Test(dependsOnMethods = { "testFindAll" })
  public void testUpdateStatusAndJobEndTime() {
    JobStatus status = JobStatus.COMPLETED;
    long jobEndTime = System.currentTimeMillis();
    jobDAO.updateStatusAndJobEndTimeForJobIds(Sets.newHashSet(anomalyJobId1, anomalyJobId3), status, jobEndTime);
    JobDTO anomalyJob = jobDAO.findById(anomalyJobId1);
    Assert.assertEquals(anomalyJob.getStatus(), status);
    Assert.assertEquals(anomalyJob.getScheduleEndTime(), jobEndTime);
    printAll("After testUpdateStatusAndJobEndTime");
  }

  @Test(dependsOnMethods = {"testUpdateStatusAndJobEndTime"})
  public void testFindByStatus() {
    JobStatus status = JobStatus.COMPLETED;
    List<JobDTO> anomalyJobs = jobDAO.findByStatus(status);
    Assert.assertEquals(anomalyJobs.size(), 2);
    Assert.assertEquals(anomalyJobs.get(0).getStatus(), status);
  }

  private void printAll(String msg) {
    List<JobDTO> allAnomalyJobs = jobDAO.findAll();
    System.out.println("START:ALL JOB after:"+ msg);
    for(JobDTO jobDTO:allAnomalyJobs){
      System.out.println(jobDTO);
    }
    System.out.println("END:ALL JOB after:"+ msg);
  }

  @Test(dependsOnMethods = { "testFindByStatus" })
  public void testDeleteRecordsOlderThanDaysWithStatus() {
    JobStatus status = JobStatus.COMPLETED;
    int numRecordsDeleted = jobDAO.deleteRecordsOlderThanDaysWithStatus(0, status);
    Assert.assertEquals(numRecordsDeleted, 2);
    List<JobDTO> anomalyJobs = jobDAO.findByStatus(status);
    Assert.assertEquals(anomalyJobs.size(), 0);
  }
}
