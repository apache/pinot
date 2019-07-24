/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.realtime.stream;

import java.util.Properties;


/**
 * StreamDataProvider provides StreamDataServerStartable and StreamDataProducer based on
 * given implementation class name.
 * E.g. KafkaDataServerStartable, KafkaDataProducer.
 *
 */
public class StreamDataProvider {
  public static StreamDataServerStartable getServerDataStartable(String clazz, Properties props)
      throws Exception {
    final StreamDataServerStartable streamDataServerStartable =
        (StreamDataServerStartable) Class.forName(clazz).newInstance();
    streamDataServerStartable.init(props);
    return streamDataServerStartable;
  }

  public static StreamDataProducer getStreamDataProducer(String clazz, Properties props)
      throws Exception {

    final StreamDataProducer streamDataProducer = (StreamDataProducer) Class.forName(clazz).newInstance();
    streamDataProducer.init(props);
    return streamDataProducer;
  }
}
