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
 *
 */

package org.apache.pinot.thirdeye.dashboard.resources;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import javax.ws.rs.Path;
import org.apache.pinot.thirdeye.api.application.ApplicationResource;
import org.apache.pinot.thirdeye.api.detection.AnomalyDetectionResource;
import org.apache.pinot.thirdeye.api.user.dashboard.UserDashboardResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AnomaliesResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ConfigResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.DataResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.DetectionAlertResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.alerts.AlertResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies.AnomalySearchResource;
import org.apache.pinot.thirdeye.dataset.DatasetAutoOnboardResource;
import org.apache.pinot.thirdeye.datasource.sql.resources.SqlDataSourceResource;
import org.apache.pinot.thirdeye.detection.DetectionResource;
import org.apache.pinot.thirdeye.detection.yaml.YamlResource;

/**
 * This is the Jersey root endpoint "/"
 * All child endpoints should be registered here with method level @Path annotations.
 *
 */
@Path("/")
@Api(tags = { "Root" })
@Singleton
public class RootResource {

  private final AlertResource alertResource;
  private final AdminResource adminResource;
  private final AnomaliesResource anomaliesResource;
  private final AnomalySearchResource anomalySearchResource;
  private final AnomalyDetectionResource anomalyDetectionResource;
  private final ApplicationResource applicationResource;
  private final AutoOnboardResource autoOnboardResource;
  private final CacheResource cacheResource;
  private final ConfigResource configResource;
  private final CustomizedEventResource customizedEventResource;
  private final DataResource dataResource;
  private final DatasetAutoOnboardResource datasetAutoOnboardResource;
  private final DetectionResource detectionResource;
  private final DetectionAlertResource detectionAlertResource;
  private final EntityMappingResource entityMappingResource;
  private final OnboardDatasetMetricResource onboardDatasetMetricResource;
  private final SqlDataSourceResource sqlDataSourceResource;
  private final ThirdEyeResource thirdEyeResource;
  private final UserDashboardResource userDashboardResource;
  private final YamlResource yamlResource;

  @Inject
  public RootResource(
      final AlertResource alertResource,
      final AdminResource adminResource,
      final AnomaliesResource anomaliesResource,
      final AnomalySearchResource anomalySearchResource,
      final AnomalyDetectionResource anomalyDetectionResource,
      final ApplicationResource applicationResource,
      final AutoOnboardResource autoOnboardResource,
      final CacheResource cacheResource,
      final ConfigResource configResource,
      final CustomizedEventResource customizedEventResource,
      final DataResource dataResource,
      final DatasetAutoOnboardResource datasetAutoOnboardResource,
      final DetectionResource detectionResource,
      final DetectionAlertResource detectionAlertResource,
      final EntityMappingResource entityMappingResource,
      final OnboardDatasetMetricResource onboardDatasetMetricResource,
      final SqlDataSourceResource sqlDataSourceResource,
      final ThirdEyeResource thirdEyeResource,
      final UserDashboardResource userDashboardResource,
      final YamlResource yamlResource) {
    this.alertResource = alertResource;
    this.adminResource = adminResource;
    this.anomaliesResource = anomaliesResource;
    this.anomalySearchResource = anomalySearchResource;
    this.anomalyDetectionResource = anomalyDetectionResource;
    this.applicationResource = applicationResource;
    this.autoOnboardResource = autoOnboardResource;
    this.cacheResource = cacheResource;
    this.configResource = configResource;
    this.customizedEventResource = customizedEventResource;
    this.dataResource = dataResource;
    this.datasetAutoOnboardResource = datasetAutoOnboardResource;
    this.detectionResource = detectionResource;
    this.detectionAlertResource = detectionAlertResource;
    this.entityMappingResource = entityMappingResource;
    this.onboardDatasetMetricResource = onboardDatasetMetricResource;
    this.sqlDataSourceResource = sqlDataSourceResource;
    this.thirdEyeResource = thirdEyeResource;
    this.userDashboardResource = userDashboardResource;
    this.yamlResource = yamlResource;
  }


  @Path("alerts")
  public AlertResource getAlertResource() {
    return alertResource;
  }

  @Path("anomalies")
  public AnomaliesResource getAnomaliesResource() {
    return anomaliesResource;
  }

  @Path("anomaly-search")
  public AnomalySearchResource getAnomalySearchResource() {
    return anomalySearchResource;
  }

  @Path("anomaly-detection")
  public AnomalyDetectionResource getAnomalyDetectionResource() {
    return anomalyDetectionResource;
  }

  @Path("application")
  public ApplicationResource getApplicationResource() {
    return applicationResource;
  }

  @Path("events")
  public CustomizedEventResource getCustomizedEventResource() {
    return customizedEventResource;
  }

  @Path(value = "autoOnboard")
  public AutoOnboardResource getAutoOnboardResource() {
    return autoOnboardResource;
  }

  @Path("cache")
  public CacheResource getCacheResource() {
    return cacheResource;
  }

  @Path("config")
  public ConfigResource getConfigResource() {
    return configResource;
  }

  @Path("data")
  public DataResource getDataResource() {
    return dataResource;
  }

  @Path("dataset-auto-onboard")
  public DatasetAutoOnboardResource getDatasetAutoOnboardResource() {
    return datasetAutoOnboardResource;
  }

  @Path("detection")
  public DetectionResource getDetectionResource() {
    return detectionResource;
  }

  @Path(value = "entityMapping")
  public EntityMappingResource getEntityMappingResource() {
    return entityMappingResource;
  }

  @Path("groups")
  public DetectionAlertResource getDetectionAlertResource() {
    return detectionAlertResource;
  }

  @Path("onboard")
  public OnboardDatasetMetricResource getOnboardDatasetMetricResource() {
    return onboardDatasetMetricResource;
  }

  @Path("thirdeye-admin")
  public AdminResource getAdminResource() {
    return adminResource;
  }

  @Path("sql-data-source")
  public SqlDataSourceResource getSqlDataSourceResource() {
    return sqlDataSourceResource;
  }

  @Path("thirdeye")
  public ThirdEyeResource getThirdEyeResource() {
    return thirdEyeResource;
  }

  @Path("userdashboard")
  public UserDashboardResource getUserDashboardResource() {
    return userDashboardResource;
  }

  @Path("yaml")
  public YamlResource getYamlResource() {
    return yamlResource;
  }
}
