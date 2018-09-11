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

package com.linkedin.thirdeye.datasource;

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This class defines the config of a single datasource used in thirdeye
 * Eg: PinotThirdeyeDataSource
 */
public class DataSourceConfig {

  private String className;
  private Map<String, Object> properties;
  private String autoLoadClassName = null;


  public DataSourceConfig() {

  }
  public DataSourceConfig(String className, Map<String, Object> properties, String autoLoadClassName) {
    this.className = className;
    this.properties = properties;
    this.autoLoadClassName = autoLoadClassName;
  }

  public String getClassName() {
    return className;
  }
  public void setClassName(String className) {
    this.className = className;
  }
  public Map<String, Object> getProperties() {
    return properties;
  }
  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
  public String getAutoLoadClassName() {
    return autoLoadClassName;
  }
  public void setAutoLoadClassName(String autoLoadClassName) {
    this.autoLoadClassName = autoLoadClassName;
  }
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

}
