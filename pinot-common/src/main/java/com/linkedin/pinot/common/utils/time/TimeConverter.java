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
package com.linkedin.pinot.common.utils.time;

import com.linkedin.pinot.common.data.TimeGranularitySpec;

/**
 * TimeConverter to convert inputTimeValue whose spec is defined by incomingGranularitySpec to
 * outgoingGranularitySpec
 */
public interface TimeConverter {
  /**
   * @param incomingGranularitySpec
   * @param outgoingGranularitySpec
   */
  void init(TimeGranularitySpec incomingGranularitySpec,
      TimeGranularitySpec outgoingGranularitySpec);

  /**
   * @param incoming time value based on incoming time spec
   * @return time value based on outgoing time spec
   */
  Object convert(Object inputTimeValue);

}
