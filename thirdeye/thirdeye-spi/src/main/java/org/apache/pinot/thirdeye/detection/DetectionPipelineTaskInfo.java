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

package org.apache.pinot.thirdeye.detection;

import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;

public class DetectionPipelineTaskInfo implements TaskInfo {
  long configId;
  long start;
  long end;
  boolean online;
  DetectionConfigBean detectionConfigBean;

  public DetectionPipelineTaskInfo(long configId, long start, long end) {
    this.configId = configId;
    this.start = start;
    this.end = end;
  }

  public DetectionPipelineTaskInfo(){
    // dummy constructor for deserialization
  }

  public long getConfigId() {
    return configId;
  }

  public void setConfigId(long configId) {
    this.configId = configId;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public boolean isOnline() {
    return online;
  }

  public void setOnline(boolean online) {
    this.online = online;
  }

  public DetectionConfigBean getDetectionConfigBean() {
    return detectionConfigBean;
  }

  public void setDetectionConfigBean(DetectionConfigBean detectionConfigBean) {
    this.detectionConfigBean = detectionConfigBean;
  }
}
