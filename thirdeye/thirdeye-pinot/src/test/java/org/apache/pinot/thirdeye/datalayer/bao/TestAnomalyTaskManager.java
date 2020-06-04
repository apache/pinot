/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.datalayer.bao;

import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;

public class TestAnomalyTaskManager {

  private Long anomalyTaskId1;
  private Long anomalyTaskId2;
  private Long anomalyJobId;
  private static final Set<TaskStatus> allowedOldTaskStatus = new HashSet<>();
  static  {
    allowedOldTaskStatus.add(TaskStatus.FAILED);
    allowedOldTaskStatus.add(TaskStatus.WAITING);
  }

  private DAOTestBase testDAOProvider;
  private JobManager jobDAO;
  private TaskManager taskDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    jobDAO = daoRegistry.getJobDAO();
    taskDAO = daoRegistry.getTaskDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() throws JsonProcessingException {
    JobDTO testAnomalyJobSpec = DaoTestUtils.getTestJobSpec();
    anomalyJobId = jobDAO.save(testAnomalyJobSpec);
    anomalyTaskId1 = taskDAO.save(getTestTaskSpec(testAnomalyJobSpec));
    Assert.assertNotNull(anomalyTaskId1);
    anomalyTaskId2 = taskDAO.save(getTestTaskSpec(testAnomalyJobSpec));
    Assert.assertNotNull(anomalyTaskId2);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAll() throws Exception {
    List<TaskDTO> anomalyTasks = taskDAO.findAll();
    Assert.assertEquals(anomalyTasks.size(), 2);
  }

  @Test(dependsOnMethods = { "testFindAll" })
  public void testUpdateStatusAndWorkerId() {
    Long workerId = 1L;
    TaskDTO taskDTO = taskDAO.findById(anomalyTaskId1);
    boolean status =
        taskDAO.updateStatusAndWorkerId(workerId, anomalyTaskId1, allowedOldTaskStatus, taskDTO.getVersion());
    TaskDTO anomalyTask = taskDAO.findById(anomalyTaskId1);
    Assert.assertTrue(status);
    Assert.assertEquals(anomalyTask.getStatus(), TaskStatus.RUNNING);
    Assert.assertEquals(anomalyTask.getWorkerId(), workerId);
    Assert.assertEquals(anomalyTask.getVersion(), taskDTO.getVersion() + 1);

  }

  @Test(dependsOnMethods = {"testUpdateStatusAndWorkerId"})
  public void testFindByStatusOrderByCreationTimeAsc() {
    List<TaskDTO> anomalyTasks =
        taskDAO.findByStatusOrderByCreateTime(TaskStatus.WAITING, Integer.MAX_VALUE, true);
    Assert.assertEquals(anomalyTasks.size(), 1);
  }

  @Test(dependsOnMethods = {"testFindByStatusOrderByCreationTimeAsc"})
  public void testUpdateStatusAndTaskEndTime() {
    TaskStatus oldStatus = TaskStatus.RUNNING;
    TaskStatus newStatus = TaskStatus.COMPLETED;
    long taskEndTime = System.currentTimeMillis();
    taskDAO.updateStatusAndTaskEndTime(anomalyTaskId1, oldStatus, newStatus, taskEndTime, "testMessage");
    TaskDTO anomalyTask = taskDAO.findById(anomalyTaskId1);
    Assert.assertEquals(anomalyTask.getStatus(), newStatus);
    Assert.assertEquals(anomalyTask.getEndTime(), taskEndTime);
    Assert.assertEquals(anomalyTask.getMessage(), "testMessage");
  }

  @Test(dependsOnMethods = {"testUpdateStatusAndTaskEndTime"})
  public void testFindByJobIdStatusNotIn() {
    TaskStatus status = TaskStatus.COMPLETED;
    List<TaskDTO> anomalyTaskSpecs = taskDAO.findByJobIdStatusNotIn(anomalyJobId, status);
    Assert.assertEquals(anomalyTaskSpecs.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testUpdateTaskStartTime() {
    long taskStartTime = System.currentTimeMillis();
    taskDAO.updateTaskStartTime(anomalyTaskId1, taskStartTime);
    TaskDTO anomalyTask = taskDAO.findById(anomalyTaskId1);
    Assert.assertEquals(anomalyTask.getStartTime(), taskStartTime);
  }

  @Test(dependsOnMethods = {"testFindByJobIdStatusNotIn"})
  public void testDeleteRecordOlderThanDaysWithStatus() {
    TaskStatus status = TaskStatus.COMPLETED;
    int numRecordsDeleted = taskDAO.deleteRecordsOlderThanDaysWithStatus(0, status);
    Assert.assertEquals(numRecordsDeleted, 1);
  }

  @Test(dependsOnMethods = {"testDeleteRecordOlderThanDaysWithStatus"})
  public void testFindByStatusWithinDays() throws JsonProcessingException, InterruptedException {
    JobDTO testAnomalyJobSpec = DaoTestUtils.getTestJobSpec();
    anomalyJobId = jobDAO.save(testAnomalyJobSpec);
    anomalyTaskId1 = taskDAO.save(getTestTaskSpec(testAnomalyJobSpec));
    Assert.assertNotNull(anomalyTaskId1);
    anomalyTaskId2 = taskDAO.save(getTestTaskSpec(testAnomalyJobSpec));
    Assert.assertNotNull(anomalyTaskId2);

    Thread.sleep(100); // To ensure every task has been created more than 1 ms ago

    List<TaskDTO> tasksWithZeroDays = taskDAO.findByStatusWithinDays(TaskStatus.WAITING, 0);
    Assert.assertEquals(tasksWithZeroDays.size(), 0);

    List<TaskDTO> tasksWithOneDays = taskDAO.findByStatusWithinDays(TaskStatus.WAITING, 1);
    Assert.assertTrue(tasksWithOneDays.size() > 0);
  }

  @Test(dependsOnMethods = {"testDeleteRecordOlderThanDaysWithStatus"})
  public void testFindTimeoutTasksWithinDays() throws JsonProcessingException, InterruptedException {
    TaskDTO task1 = taskDAO.findById(anomalyTaskId1);
    task1.setStatus(TaskStatus.RUNNING);
    taskDAO.update(task1);

    Thread.sleep(100); // To ensure every task has been updated more than 50 ms ago

    List<TaskDTO> all = taskDAO.findByStatusWithinDays(TaskStatus.RUNNING, 7);

    List<TaskDTO> timeoutTasksWithinOneDays = taskDAO.findTimeoutTasksWithinDays(7, 50);
    Assert.assertTrue(timeoutTasksWithinOneDays.size() > 0);
  }

  TaskDTO getTestTaskSpec(JobDTO anomalyJobSpec) throws JsonProcessingException {
    TaskDTO jobSpec = new TaskDTO();
    jobSpec.setJobName("Test_Anomaly_Task");
    jobSpec.setStatus(TaskStatus.WAITING);
    jobSpec.setTaskType(TaskType.DETECTION);
    jobSpec.setStartTime(new DateTime().minusDays(20).getMillis());
    jobSpec.setEndTime(new DateTime().minusDays(10).getMillis());
    jobSpec.setTaskInfo(new ObjectMapper().writeValueAsString(getTestDetectionTaskInfo()));
    jobSpec.setJobId(anomalyJobSpec.getId());
    return jobSpec;
  }

  static DetectionPipelineTaskInfo getTestDetectionTaskInfo() {
    DetectionPipelineTaskInfo taskInfo = new DetectionPipelineTaskInfo();
    taskInfo.setStart((new DateTime().minusHours(1)).getMillis());
    taskInfo.setEnd((new DateTime().minusHours(1)).getMillis());
    return taskInfo;
  }
}
