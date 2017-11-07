package com.linkedin.thirdeye.datalayer.pojo;

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
