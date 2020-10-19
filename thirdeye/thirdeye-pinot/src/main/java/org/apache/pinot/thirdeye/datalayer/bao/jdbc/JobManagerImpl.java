/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.persist.Transactional;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.anomaly.job.JobConstants.JobStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.bao.JobManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.JobBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.joda.time.DateTime;

@Singleton
public class JobManagerImpl extends AbstractManagerImpl<JobDTO> implements JobManager {

  private static final String FIND_RECENT_SCHEDULED_JOB_BY_TYPE_AND_CONFIG_ID =
      "where type=:type and configId=:configId and status!=:status and scheduleStartTime>=:scheduleStartTime order by scheduleStartTime desc";

  @Inject
  public JobManagerImpl(GenericPojoDao genericPojoDao) {
    super(JobDTO.class, JobBean.class, genericPojoDao);
  }

  @Override
  @Transactional
  public List<JobDTO> findByStatus(JobStatus status) {
    return super.findByParams(ImmutableMap.<String, Object>of("status", status.toString()));
  }

  @Override
  public List<JobDTO> findByStatusWithinDays(JobStatus status, int days) {
    DateTime activeDate = new DateTime().minusDays(days);
    Timestamp activeTimestamp = new Timestamp(activeDate.getMillis());
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    Predicate timestampPredicate = Predicate.GE("createTime", activeTimestamp);
    return findByPredicate(Predicate.AND(statusPredicate, timestampPredicate));
  }

  @Override
  @Transactional
  public void updateJobStatusAndEndTime(List<JobDTO> jobsToUpdate, JobStatus newStatus, long newEndTime) {
    Preconditions.checkNotNull(newStatus);
    if (CollectionUtils.isNotEmpty(jobsToUpdate)) {
      for (JobDTO jobDTO : jobsToUpdate) {
        jobDTO.setStatus(newStatus);
        jobDTO.setScheduleEndTime(newEndTime);
      }
      update(jobsToUpdate);
    }
  }

  @Override
  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    Predicate timestampPredicate = Predicate.LT("updateTime", expireTimestamp);
    return deleteByPredicate(Predicate.AND(statusPredicate, timestampPredicate));
  }

  @Override
  public List<JobDTO> findNRecentJobs(int n) {
    String parameterizedSQL = "order by scheduleStartTime desc limit "+n;
    HashMap<String, Object> parameterMap = new HashMap<>();
    List<JobBean> list = genericPojoDao.executeParameterizedSQL(parameterizedSQL, parameterMap, JobBean.class);
    return convertBeanListToDTOList(list);
  }

  @Override
  public String getJobNameByJobId(long id) {
    JobDTO anomalyJobSpec = findById(id);
    if (anomalyJobSpec != null) {
      return anomalyJobSpec.getJobName();
    } else {
      return null;
    }
  }

  @Override
  public List<JobDTO> findRecentScheduledJobByTypeAndConfigId(TaskConstants.TaskType taskType, long configId,
      long minScheduledTime) {
    HashMap<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("type", taskType);
    parameterMap.put("configId", configId);
    parameterMap.put("status", JobStatus.FAILED);
    parameterMap.put("scheduleStartTime", minScheduledTime);
    List<JobBean> list = genericPojoDao
        .executeParameterizedSQL(FIND_RECENT_SCHEDULED_JOB_BY_TYPE_AND_CONFIG_ID, parameterMap, JobBean.class);

    if (CollectionUtils.isNotEmpty(list)) {
      // Sort by scheduleStartTime; most recent scheduled job at the beginning
      Collections.sort(list, Collections.reverseOrder(new Comparator<JobBean>() {
        @Override
        public int compare(JobBean o1, JobBean o2) {
          return Long.compare(o1.getScheduleStartTime(), o2.getScheduleStartTime());
        }
      }));
      return convertBeanListToDTOList(list);
    } else {
      return Collections.emptyList();
    }
  }
}
