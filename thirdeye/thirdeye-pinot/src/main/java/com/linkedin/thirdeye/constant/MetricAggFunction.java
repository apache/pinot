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

package com.linkedin.thirdeye.constant;

public enum MetricAggFunction {
  SUM, AVG, COUNT, MAX,
  percentileTDigest5, percentileTDigest10, percentileTDigest20, percentileTDigest25, percentileTDigest30,
  percentileTDigest40, percentileTDigest50, percentileTDigest60, percentileTDigest70, percentileTDigest75,
  percentileTDigest80, percentileTDigest90, percentileTDigest95, percentileTDigest99;

  public boolean isTDigest() {
    return this.toString().startsWith("percentileTDigest");
  }
}
