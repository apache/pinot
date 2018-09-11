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

package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import java.util.ArrayList;
import java.util.List;

public class AnomalyDataCompare {

  public static class Response {
    long currentStart;
    long currenEnd;
    double currentVal;

    List<CompareResult> compareResults = new ArrayList<>();

    public List<CompareResult> getCompareResults() {
      return compareResults;
    }

    public void setCompareResults(List<CompareResult> compareResults) {
      this.compareResults = compareResults;
    }

    public double getCurrentVal() {
      return currentVal;
    }

    public void setCurrentVal(double currentVal) {
      this.currentVal = currentVal;
    }

    public long getCurrenEnd() {
      return currenEnd;
    }

    public void setCurrenEnd(long currenEnd) {
      this.currenEnd = currenEnd;
    }

    public long getCurrentStart() {
      return currentStart;
    }

    public void setCurrentStart(long currentStart) {
      this.currentStart = currentStart;
    }
  }

  public static class CompareResult {
    AlertConfigBean.COMPARE_MODE compareMode;
    double baselineValue;
    double change = 1;

    public double getBaselineValue() {
      return baselineValue;
    }

    public void setBaselineValue(double baselineValue) {
      this.baselineValue = baselineValue;
    }

    public double getChange() {
      return change;
    }

    public void setChange(double change) {
      this.change = change;
    }

    public AlertConfigBean.COMPARE_MODE getCompareMode() {
      return compareMode;
    }

    public void setCompareMode(AlertConfigBean.COMPARE_MODE compareMode) {
      this.compareMode = compareMode;
    }
  }
}
