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

package com.linkedin.thirdeye.auto.onboard;

import com.linkedin.thirdeye.datasource.DataSourceConfig;

/**
 * This is the abstract parent class for all auto onboard services for various datasources
 */
public abstract class AutoOnboard {
  private DataSourceConfig dataSourceConfig;

  public AutoOnboard(DataSourceConfig dataSourceConfig) {
    this.dataSourceConfig = dataSourceConfig;
  }

  public DataSourceConfig getDataSourceConfig() {
    return dataSourceConfig;
  }

  /**
   * Method which contains implementation of what needs to be done as part of onboarding for this data source
   */
  public abstract void run();

  /**
   * Method for triggering adhoc run of the onboard logic
   */
  public abstract void runAdhoc();
}
