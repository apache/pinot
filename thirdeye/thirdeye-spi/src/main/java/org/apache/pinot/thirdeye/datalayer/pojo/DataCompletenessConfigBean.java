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
