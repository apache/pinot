/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
}
