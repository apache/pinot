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

package org.apache.pinot.thirdeye.datasource;

import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AlertSnapshotManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.ClassificationConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.ConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionStatusManager;
import org.apache.pinot.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.GroupedAnomalyResultsManager;
import org.apache.pinot.thirdeye.datalayer.bao.JobManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.OnboardDatasetMetricManager;
import org.apache.pinot.thirdeye.datalayer.bao.OnlineDetectionDataManager;
import org.apache.pinot.thirdeye.datalayer.bao.OverrideConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.RawAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseSessionManager;
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseTemplateManager;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AlertSnapshotManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ApplicationManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ClassificationConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionAlertConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EntityToEntityMappingManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EvaluationManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.GroupedAnomalyResultsManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OnboardDatasetMetricManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OnlineDetectionDataManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RootcauseSessionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RootcauseTemplateManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.SessionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.TaskManagerImpl;
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseTemplateDTO;
import org.apache.pinot.thirdeye.datalayer.util.DaoProviderUtil;

/**
 * Singleton service registry for Data Access Objects (DAOs)
 */
public class DAORegistry {

  private static final DAORegistry INSTANCE = new DAORegistry();

  /****************************************************************************
   * SINGLETON
   */

  public static DAORegistry getInstance() {
    return INSTANCE;
  }


  /**
   * internal constructor.
   */
  private DAORegistry() {}

  /****************************************************************************
   * GETTERS/SETTERS
   */

  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return DaoProviderUtil.getInstance(AnomalyFunctionManagerImpl.class);
  }

  public AlertConfigManager getAlertConfigDAO() {
    return DaoProviderUtil.getInstance(AlertConfigManagerImpl.class);
  }

  public RawAnomalyResultManager getRawAnomalyResultDAO() {
    return DaoProviderUtil.getInstance(RawAnomalyResultManagerImpl.class);
  }

  public MergedAnomalyResultManager getMergedAnomalyResultDAO() {
    return DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
  }

  public JobManager getJobDAO() {
    return DaoProviderUtil.getInstance(JobManagerImpl.class);
  }

  public TaskManager getTaskDAO() {
    return DaoProviderUtil.getInstance(TaskManagerImpl.class);
  }

  public DatasetConfigManager getDatasetConfigDAO() {
    return DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
  }

  public MetricConfigManager getMetricConfigDAO() {
    return DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);
  }

  public OverrideConfigManager getOverrideConfigDAO() {
    return DaoProviderUtil.getInstance(OverrideConfigManagerImpl.class);
  }

  public DataCompletenessConfigManager getDataCompletenessConfigDAO() {
    return DaoProviderUtil.getInstance(DataCompletenessConfigManagerImpl.class);
  }

  public EventManager getEventDAO() {
    return DaoProviderUtil.getInstance(EventManagerImpl.class);
  }

  public DetectionStatusManager getDetectionStatusDAO() {
    return DaoProviderUtil.getInstance(DetectionStatusManagerImpl.class);
  }

  public ClassificationConfigManager getClassificationConfigDAO() {
    return DaoProviderUtil.getInstance(ClassificationConfigManagerImpl.class);
  }

  public EntityToEntityMappingManager getEntityToEntityMappingDAO() {
    return DaoProviderUtil.getInstance(EntityToEntityMappingManagerImpl.class);
  }

  public GroupedAnomalyResultsManager getGroupedAnomalyResultsDAO() {
    return DaoProviderUtil.getInstance(GroupedAnomalyResultsManagerImpl.class);
  }

  public OnboardDatasetMetricManager getOnboardDatasetMetricDAO() {
    return DaoProviderUtil.getInstance(OnboardDatasetMetricManagerImpl.class);
  }

  public ConfigManager getConfigDAO() {
    return DaoProviderUtil.getInstance(ConfigManagerImpl.class);
  }

  public ApplicationManager getApplicationDAO() {
    return DaoProviderUtil.getInstance(ApplicationManagerImpl.class);
  }

  public AlertSnapshotManager getAlertSnapshotDAO() {
    return DaoProviderUtil.getInstance(AlertSnapshotManagerImpl.class);
  }

  public RootcauseSessionManager getRootcauseSessionDAO() {
    return DaoProviderUtil.getInstance(RootcauseSessionManagerImpl.class);
  }

  public RootcauseTemplateManager getRootcauseTemplateDao() {
    return DaoProviderUtil.getInstance(RootcauseTemplateManagerImpl.class);
  }

  public SessionManager getSessionDAO() {
    return DaoProviderUtil.getInstance(SessionManagerImpl.class);
  }

  public DetectionConfigManager getDetectionConfigManager() {
    return DaoProviderUtil.getInstance(DetectionConfigManagerImpl.class);
  }

  public DetectionAlertConfigManager getDetectionAlertConfigManager() {
    return DaoProviderUtil.getInstance(DetectionAlertConfigManagerImpl.class);
  }

  public EvaluationManager getEvaluationManager() {
    return DaoProviderUtil.getInstance(EvaluationManagerImpl.class);
  }

  public OnlineDetectionDataManager getOnlineDetectionDataManager() {
    return DaoProviderUtil.getInstance(OnlineDetectionDataManagerImpl.class);
  }
}
