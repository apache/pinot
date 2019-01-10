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

import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultJoinKeyExtractor implements JoinKeyExtractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJoinKeyExtractor.class);

  private Map<String, String> joinKeyMap;
  private String defaultJoinKey;

  public DefaultJoinKeyExtractor(Map<String, String> params) {
    this.joinKeyMap = params;
    this.defaultJoinKey = params.get("defaultJoinKey");
  }

  @Override
  public String extractJoinKey(String sourceName, GenericRecord record) {

    String joinKey = defaultJoinKey;
    if (joinKeyMap != null && joinKeyMap.containsKey(sourceName)) {
      joinKey = joinKeyMap.get(sourceName);
    }
    String ret = "INVALID";
    if (joinKey != null) {
      Object object = record.get(joinKey);
      if (object != null) {
        ret = object.toString();
      }
    }
    LOGGER.info("source:{} JoinKey:{} value:{}", sourceName, joinKey, ret);
    return ret;
  }

}
