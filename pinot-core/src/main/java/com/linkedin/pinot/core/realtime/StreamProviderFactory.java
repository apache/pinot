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
package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamLevelConsumer;


/**
 * Factory for StreamProviders, so that they can be overridden for unit tests.
 */
public class StreamProviderFactory {
  private static Class<? extends StreamLevelConsumer> streamProviderClass = KafkaStreamLevelConsumer.class;

  public static StreamLevelConsumer buildStreamProvider() {
    try {
      return streamProviderClass.newInstance();
    } catch (Exception e) {
      Utils.rethrowException(e);
      return null;
    }
  }

  public static Class<? extends StreamLevelConsumer> getStreamProviderClass() {
    return streamProviderClass;
  }

  public static void setStreamProviderClass(Class<? extends StreamLevelConsumer> clazz) {
    streamProviderClass = clazz;
  }
}
