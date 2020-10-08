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

package org.apache.pinot.thirdeye.datalayer.pojo;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OverrideConfigBean extends AbstractBean {

  private long startTime;

  private long endTime;

  private Map<String, List<String>> targetLevel;

  private String targetEntity;

  private Map<String, String> overrideProperties;

  private boolean active;


  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public Map<String, List<String>> getTargetLevel() {
    return targetLevel;
  }

  public void setTargetLevel(Map<String, List<String>> targetLevel) {
    this.targetLevel = targetLevel;
  }

  public String getTargetEntity() {
    return targetEntity;
  }

  public void setTargetEntity(String targetEntity) {
    this.targetEntity = targetEntity;
  }

  public Map<String, String> getOverrideProperties() {
    return overrideProperties;
  }

  public void setOverrideProperties(Map<String, String> overrideProperties) {
    this.overrideProperties = overrideProperties;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean isActive) {
    this.active = isActive;
  }

  public boolean getActive() {
    return active;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof OverrideConfigBean)) {
      return false;
    }
    OverrideConfigBean that = (OverrideConfigBean) o;
    return Objects.equals(startTime, that.getStartTime())
        && Objects.equals(endTime, that.getEndTime())
        && Objects.equals(targetLevel, that.getTargetLevel())
        && Objects.equals(targetEntity, that.getTargetEntity())
        && Objects.equals(overrideProperties, that.getOverrideProperties())
        && Objects.equals(active, that.getActive());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStartTime(), getEndTime(), getTargetLevel(), getTargetEntity(),
        getOverrideProperties(), getActive());
  }
}
