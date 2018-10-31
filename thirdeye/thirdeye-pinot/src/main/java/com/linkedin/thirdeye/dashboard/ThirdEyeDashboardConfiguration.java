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

package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.RootCauseConfiguration;
import com.linkedin.thirdeye.dashboard.configs.AuthConfiguration;
import com.linkedin.thirdeye.dashboard.configs.ResourceConfiguration;
import java.util.List;


public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {
  AuthConfiguration authConfig;
  RootCauseConfiguration rootCause;
  List<ResourceConfiguration> resourceConfig;

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
