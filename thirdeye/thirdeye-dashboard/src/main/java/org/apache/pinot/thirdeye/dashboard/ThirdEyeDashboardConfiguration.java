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

package org.apache.pinot.thirdeye.dashboard;

import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.dashboard.configs.AuthConfiguration;
import org.apache.pinot.thirdeye.dashboard.configs.ResourceConfiguration;
import java.util.List;


public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {
  private static final double PROP_DEFAULT_ALERT_ONBOARDING_PERMIT_PER_SECOND = 2;

  AuthConfiguration authConfig;
  RootCauseConfiguration rootCause;
  List<ResourceConfiguration> resourceConfig;
  DetectionPreviewConfiguration detectionPreviewConfig = new DetectionPreviewConfiguration();

  // the maximum number of onboarding requests allowed per second
  double alertOnboardingPermitPerSecond = PROP_DEFAULT_ALERT_ONBOARDING_PERMIT_PER_SECOND;

  public DetectionPreviewConfiguration getDetectionPreviewConfig() {
    return detectionPreviewConfig;
  }

  public void setDetectionPreviewConfig(DetectionPreviewConfiguration detectionPreviewConfig) {
    this.detectionPreviewConfig = detectionPreviewConfig;
  }

  public double getAlertOnboardingPermitPerSecond() {
    return alertOnboardingPermitPerSecond;
  }

  public void setAlertOnboardingPermitPerSecond(double alertOnoardingPermitPerSecond) {
    this.alertOnboardingPermitPerSecond = alertOnoardingPermitPerSecond;
  }

  public List<ResourceConfiguration> getResourceConfig() {
    return resourceConfig;
  }

  public void setResourceConfig(List<ResourceConfiguration> resourceConfig) {
    this.resourceConfig = resourceConfig;
  }

  public RootCauseConfiguration getRootCause() {
    return rootCause;
  }

  public void setRootCause(RootCauseConfiguration rootCause) {
    this.rootCause = rootCause;
  }

  public AuthConfiguration getAuthConfig() {
    return authConfig;
  }

  public void setAuthConfig(AuthConfiguration authConfig) {
    this.authConfig = authConfig;
  }
}
