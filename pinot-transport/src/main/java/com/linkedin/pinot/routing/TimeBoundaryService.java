/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.routing;

public interface TimeBoundaryService {

  /**
   * For a given federated resource name, return its time boundary info.
   * 
   * @param resource
   * @return
   */
  TimeBoundaryInfo getTimeBoundaryInfoFor(String resource);

  /**
   * Remove a resource from TimeBoundaryService
   * @param resourceName
   */
  void remove(String resourceName);

  public class TimeBoundaryInfo {
    private String _timeColumn;
    private String _timeValue;

    public String getTimeColumn() {
      return _timeColumn;
    }

    public String getTimeValue() {
      return _timeValue;
    }

    public void setTimeColumn(String timeColumn) {
      _timeColumn = timeColumn;
    }

    public void setTimeValue(String timeValue) {
      _timeValue = timeValue;
    }
  }

}
