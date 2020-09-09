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

package org.apache.pinot.thirdeye.datalayer.dto;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.alert.commons.AnomalyNotifiedStatus;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertSnapshotBean;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AlertSnapshotDTO extends AlertSnapshotBean {
  private static final Logger LOG = LoggerFactory.getLogger(AlertSnapshotDTO.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public Multimap<String, AnomalyNotifiedStatus> getSnapshot() {
    Multimap<String, AnomalyNotifiedStatus> snapshot = HashMultimap.create();
    if (snapshotString == null || snapshotString.size() == 0) {
      LOG.info("SnapshotString in AlertSnapshotBean {} is empty, return an empty map instead", getId());
    } else {
      for (Map.Entry<String, List<String>> entry : snapshotString.entrySet()) {
        for (String statusString : entry.getValue()) {
          try {
            AnomalyNotifiedStatus status = OBJECT_MAPPER.readValue(statusString, AnomalyNotifiedStatus.class);
            snapshot.put(entry.getKey(), status);
          } catch (IOException e) {
            LOG.error("Unable to parse String {} to Status", statusString);
          }
        }
      }
    }
    return snapshot;
  }

  public void setSnapshot(Multimap<String, AnomalyNotifiedStatus> snapshot) {
    this.snapshotString = new HashMap<>();
    for (Map.Entry<String, AnomalyNotifiedStatus> entry : snapshot.entries()) {
      String valueString;
      try {
        valueString = OBJECT_MAPPER.writeValueAsString(entry.getValue());
      } catch (IOException e) {
        LOG.error("Unable to parse Status {} to String", entry.getValue());
        continue;
      }
      if(!snapshotString.containsKey(entry.getKey())) {
        snapshotString.put(entry.getKey(), new ArrayList<String>());
      }
      snapshotString.get(entry.getKey()).add(valueString);
    }
  }

  public AnomalyNotifiedStatus getLatestStatus(Multimap<String, AnomalyNotifiedStatus> snapshot, String statusKey) {
    if (!snapshot.containsKey(statusKey)) {
      return new AnomalyNotifiedStatus(0, 0);
    }
    Collection<AnomalyNotifiedStatus> notifyStatus = snapshot.get(statusKey);
    AnomalyNotifiedStatus recentStatus = null;

    for (AnomalyNotifiedStatus status : notifyStatus) {
      if (recentStatus == null) {
        recentStatus = status;
      } else if (recentStatus.getLastNotifyTime() < status.getLastNotifyTime()) {
        recentStatus = status;
      }
    }

    return recentStatus;
  }
}
