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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertSnapshotBean extends AbstractBean {
  protected long lastNotifyTime;
  protected long alertConfigId;

  // key: metric::dimension
  protected Map<String, List<String>> snapshotString;

  public long getLastNotifyTime() {
    return lastNotifyTime;
  }

  public void setLastNotifyTime(long lastNotifyTime) {
    this.lastNotifyTime = lastNotifyTime;
  }

  public Map<String, List<String>> getSnapshotString() {
    return snapshotString;
  }

  public void setSnapshotString(Map<String, List<String>> snapshot) {
    this.snapshotString = snapshot;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AlertSnapshotBean)) {
      return false;
    }
    AlertSnapshotBean as = (AlertSnapshotBean) o;
    return Objects.equals(snapshotString, as.getSnapshotString()) && Objects.equals(lastNotifyTime, as.getLastNotifyTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotString, lastNotifyTime);
  }
}
