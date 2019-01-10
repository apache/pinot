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

package com.linkedin.thirdeye.hadoop.join;

import org.apache.avro.generic.GenericRecord;

/**
 * Simple interface to extract the joinKey from a Generic Record
 */
public interface JoinKeyExtractor {
  /**
   * @param sourceName name of the source
   * @param record record from which the join Key is extracted. join key value is expected to be a
   *          string.
   * @return
   */
  String extractJoinKey(String sourceName, GenericRecord record);
}
