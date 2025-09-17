/**
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
package org.apache.pinot.spi.config.table;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class PauseState extends BaseJsonConfig {
  private boolean _paused;
  private ReasonCode _reasonCode;
  private String _comment;
  private String _timestamp;
  // List of inactive topic indices. Index is the index of the topic in the streamConfigMaps.
  private List<Integer> _indexOfInactiveTopics = new ArrayList<>();

  public PauseState() {
  }

  public PauseState(boolean paused, ReasonCode reasonCode, String comment, String timestamp,
      List<Integer> indexOfInactiveTopics) {
    _paused = paused;
    _reasonCode = reasonCode;
    _comment = comment;
    _timestamp = timestamp;
    setIndexOfInactiveTopics(indexOfInactiveTopics);
  }

  public boolean isPaused() {
    return _paused;
  }

  public ReasonCode getReasonCode() {
    return _reasonCode;
  }

  public String getComment() {
    return _comment;
  }

  public String getTimeInMillis() {
    return _timestamp;
  }

  public List<Integer> getIndexOfInactiveTopics() {
    return _indexOfInactiveTopics;
  }

  public void setPaused(boolean paused) {
    _paused = paused;
  }

  public void setReasonCode(ReasonCode reasonCode) {
    _reasonCode = reasonCode;
  }

  public void setComment(String comment) {
    _comment = comment;
  }

  public void setTimeInMillis(String timestamp) {
    _timestamp = timestamp;
  }

  public void setIndexOfInactiveTopics(List<Integer> indexOfInactiveTopics) {
    _indexOfInactiveTopics.clear();
    if (indexOfInactiveTopics != null) {
      _indexOfInactiveTopics.addAll(indexOfInactiveTopics);
    }
  }

  public enum ReasonCode {
    ADMINISTRATIVE, STORAGE_QUOTA_EXCEEDED, RESOURCE_UTILIZATION_LIMIT_EXCEEDED
  }
}
