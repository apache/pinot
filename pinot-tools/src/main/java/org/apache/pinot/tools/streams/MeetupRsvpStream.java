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
package org.apache.pinot.tools.streams;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MeetupRsvpStream {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MeetupRsvpStream.class);
  private static final String DEFAULT_TOPIC_NAME = "meetupRSVPEvents";
  protected String _topicName;

  protected PinotRealtimeSource _pinotRealtimeSource;

  public MeetupRsvpStream()
      throws Exception {
    this(DEFAULT_TOPIC_NAME, RsvpSourceGenerator.KeyColumn.NONE);
  }

  public MeetupRsvpStream(boolean partitionByKey)
      throws Exception {
    // calling this constructor means that we wish to use EVENT_ID as key. RsvpId is used by MeetupRsvpJsonStream
    this(DEFAULT_TOPIC_NAME,
        partitionByKey ? RsvpSourceGenerator.KeyColumn.EVENT_ID : RsvpSourceGenerator.KeyColumn.NONE);
  }

  public MeetupRsvpStream(String topicName)
      throws Exception {
    this(topicName, RsvpSourceGenerator.KeyColumn.NONE);
  }

  public MeetupRsvpStream(RsvpSourceGenerator.KeyColumn keyColumn)
      throws Exception {
    this(DEFAULT_TOPIC_NAME, keyColumn);
  }

  public MeetupRsvpStream(String topicName, RsvpSourceGenerator.KeyColumn keyColumn)
      throws Exception {
    this(topicName, keyColumn, 0);
  }

  public MeetupRsvpStream(String topicName, RsvpSourceGenerator.KeyColumn keyColumn, int nullProbability)
      throws Exception {
    _topicName = topicName;
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    StreamDataProducer producer =
        StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    _pinotRealtimeSource =
        PinotRealtimeSource.builder().setGenerator(new RsvpSourceGenerator(keyColumn, nullProbability))
            .setProducer(producer)
            .setRateLimiter(permits -> {
              int delay = (int) (Math.log(ThreadLocalRandom.current().nextDouble()) / Math.log(0.999)) + 1;
              try {
                Thread.sleep(delay);
              } catch (InterruptedException ex) {
                LOGGER.warn("Interrupted from sleep but will continue", ex);
              }
            })
            .setTopic(_topicName)
            .build();
  }

  public void run()
      throws Exception {
    _pinotRealtimeSource.run();
  }

  public void stopPublishing() {
    try {
      _pinotRealtimeSource.close();
    } catch (Exception ex) {
      LOGGER.error("Failed to close real time source. ignored and continue", ex);
    }
  }
}
