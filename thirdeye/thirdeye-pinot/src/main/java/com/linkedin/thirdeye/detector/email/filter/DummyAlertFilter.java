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

package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DummyAlertFilter extends BaseAlertFilter {
  @Override
  public List<String> getPropertyNames() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void setParameters(Map<String, String> props) {
    // Does nothing
  }

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    return true;
  }

  @Override
  public String toString() {
    return "DummyFilter";
  }
}
