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
package com.linkedin.pinot.broker.routing;

import org.json.JSONObject;


public interface TimeBoundaryService {

  /**
   * For a given federated table name, return its time boundary info.
   *
   * @param table
   * @return
   */
  TimeBoundaryInfo getTimeBoundaryInfoFor(String table);

  /**
   * Remove a table from TimeBoundaryService
   * @param tableName
   */
  void remove(String tableName);

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

    public String toJsonString() throws Exception {
      JSONObject obj = new JSONObject();
      obj.put("timeColumnName", _timeColumn);
      obj.put("timeColumnValue",_timeValue);
      return obj.toString(2);
    }
  }
}
