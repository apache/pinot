package org.apache.pinot.thirdeye.datalayer.pojo;

import java.util.Objects;

public class OnlineDetectionDataBean extends AbstractBean {
  private String dataset;

  private String metric;

  private String onlineDetectionData;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getOnlineDetectionData() {
    return onlineDetectionData;
  }

  public void setOnlineDetectionData(String onlineDetectionData) {
    this.onlineDetectionData = onlineDetectionData;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof OnlineDetectionDataBean)) {
      return false;
    }

    OnlineDetectionDataBean onlineDetectionDataBean = (OnlineDetectionDataBean) o;

    return Objects.equals(getId(), onlineDetectionDataBean.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }
}
