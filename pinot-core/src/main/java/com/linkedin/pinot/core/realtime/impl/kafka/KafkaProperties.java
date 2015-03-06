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
package com.linkedin.pinot.core.realtime.impl.kafka;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;


public class KafkaProperties {

  public static final String TOPIC_NAME = "kafka.topic.name";
  public static final String DECODER_CLASS = "kafka.decoder.class.name";
  public static final String DECODER_PROPS_PREFIX = "kafka.decoder.prop";

  public static String getDecoderPropertyKeyFor(String key) {
    return StringUtils.join(Lists.newArrayList(DECODER_PROPS_PREFIX, key).toArray(), ".");
  }

  public static String getDecoderPropertyKey(String incoming) {
    return incoming.split(DECODER_PROPS_PREFIX + ".")[1];
  }

  public static class HighLevelConsumer {
    public static final String ZK_CONNECTION_STRING = "kafka.hlc.zk.connect.string";
    public static final String GROUP_ID = "kafka.hlc.group.id";
  }
}
