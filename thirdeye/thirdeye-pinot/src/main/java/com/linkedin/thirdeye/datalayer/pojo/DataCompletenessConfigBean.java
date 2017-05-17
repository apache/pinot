package com.linkedin.thirdeye.datalayer.pojo;

import java.util.Objects;

/**
 * This class corresponds to data completeness config.
 */
public class DataCompletenessConfigBean extends AbstractBean {

  private String dataset;
  private long dateToCheckInMS;
  private String dateToCheckInSDF;
  private long countStar = 0;
  private boolean dataComplete = false;
  private double percentComplete = 0;
  private int numAttempts = 0;
  private boolean delayNotified = false;
  private boolean timedOut = false;


  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public long getDateToCheckInMS() {
    return dateToCheckInMS;
  }

  public void setDateToCheckInMS(long dateToCheckInMS) {
    this.dateToCheckInMS = dateToCheckInMS;
  }

  public String getDateToCheckInSDF() {
    return dateToCheckInSDF;
  }

  public void setDateToCheckInSDF(String dateToCheckInSDF) {
    this.dateToCheckInSDF = dateToCheckInSDF;
  }

  public long getCountStar() {
    return countStar;
  }

  public void setCountStar(long countStar) {
    this.countStar = countStar;
  }

  public boolean isDataComplete() {
    return dataComplete;
  }

  public void setDataComplete(boolean dataComplete) {
    this.dataComplete = dataComplete;
  }

  public double getPercentComplete() {
    return percentComplete;
  }

  public void setPercentComplete(double percentComplete) {
    this.percentComplete = percentComplete;
  }

  public int getNumAttempts() {
    return numAttempts;
  }

  public void setNumAttempts(int numAttempts) {
    this.numAttempts = numAttempts;
  }

  public boolean isDelayNotified() {
    return delayNotified;
  }

  public void setDelayNotified(boolean delayNotified) {
    this.delayNotified = delayNotified;
  }

  public boolean isTimedOut() {
    return timedOut;
  }

  public void setTimedOut(boolean timedOut) {
    this.timedOut = timedOut;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DataCompletenessConfigBean)) {
      return false;
    }
    DataCompletenessConfigBean dc = (DataCompletenessConfigBean) o;
    return Objects.equals(getId(), dc.getId()) && Objects.equals(dataset, dc.getDataset())
        && Objects.equals(dateToCheckInMS, dc.getDateToCheckInMS())
        && Objects.equals(dateToCheckInSDF, dc.getDateToCheckInSDF())
        && Objects.equals(countStar, dc.getCountStar()) && Objects.equals(dataComplete, dc.isDataComplete())
        && Objects.equals(percentComplete, dc.getPercentComplete()) && Objects.equals(numAttempts, dc.getNumAttempts())
        && Objects.equals(delayNotified, dc.isDelayNotified()) && Objects.equals(timedOut, dc.isTimedOut());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, dateToCheckInMS, dateToCheckInSDF, countStar, dataComplete, percentComplete,
        numAttempts, delayNotified, timedOut);
  }
}
