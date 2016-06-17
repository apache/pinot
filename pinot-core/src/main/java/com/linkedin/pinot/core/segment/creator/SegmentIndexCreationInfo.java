/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.creator;

public class SegmentIndexCreationInfo {
  private int totalDocs;
  private int totalRawDocs;
  private int totalAggDocs;
  boolean starTreeEnabled;
  private int totalErrors;
  private int totalNulls;
  private int totalConversions;
  private int totalNullCols;

  public int getTotalDocs() {
    return totalDocs;
  }

  public void setTotalDocs(int totalDocs) {
    this.totalDocs = totalDocs;
  }

  public int getTotalRawDocs() {
    return totalRawDocs;
  }

  public void setTotalRawDocs(int totalRawDocs) {
    this.totalRawDocs = totalRawDocs;
  }

  public int getTotalAggDocs() {
    return totalAggDocs;
  }

  public void setTotalAggDocs(int totalAggDocs) {
    this.totalAggDocs = totalAggDocs;
  }

  public boolean isStarTreeEnabled() {
    return starTreeEnabled;
  }

  public void setStarTreeEnabled(boolean starTreeEnabled) {
    this.starTreeEnabled = starTreeEnabled;
  }

  public int getTotalConversions() {
    return totalConversions;
  }

  public void setTotalConversions(int totalConversions) {
    this.totalConversions = totalConversions;
  }

  public int getTotalErrors() {
    return totalErrors;
  }

  public void setTotalErrors(int totalErrors) {
    this.totalErrors = totalErrors;
  }

  public int getTotalNullCols() {
    return totalNullCols;
  }

  public void setTotalNullCols(int totalNullCols) {
    this.totalNullCols = totalNullCols;
  }

  public int getTotalNulls() {
    return totalNulls;
  }

  public void setTotalNulls(int totalNulls) {
    this.totalNulls = totalNulls;
  }
}
