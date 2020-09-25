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
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;


@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionAnalysisModuleConfig extends AbstractRCAModuleConfig {
  private List<String> includedDimension;
  private List<String> excludedDimension;
  private boolean manualOrder;
  private boolean oneSideError;
  private int summarySize;
  private int dimensionDepth;

  public List<String> getIncludedDimension() {
    return includedDimension;
  }

  public void setIncludedDimension(List<String> includedDimension) {
    this.includedDimension = includedDimension;
  }

  public List<String> getExcludedDimension() {
    return excludedDimension;
  }

  public void setExcludedDimension(List<String> excludedDimension) {
    this.excludedDimension = excludedDimension;
  }

  public boolean isManualOrder() {
    return manualOrder;
  }

  public void setManualOrder(boolean manualOrder) {
    this.manualOrder = manualOrder;
  }

  public boolean isOneSideError() {
    return oneSideError;
  }

  public void setOneSideError(boolean oneSideError) {
    this.oneSideError = oneSideError;
  }

  public int getSummarySize() {
    return summarySize;
  }

  public void setSummarySize(int summarySize) {
    this.summarySize = summarySize;
  }

  public int getDimensionDepth() {
    return dimensionDepth;
  }

  public void setDimensionDepth(int dimensionDepth) {
    this.dimensionDepth = dimensionDepth;
  }
}
