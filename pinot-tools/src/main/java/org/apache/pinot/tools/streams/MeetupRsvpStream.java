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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeetupRsvpStream {
  private static final Logger LOGGER = LoggerFactory.getLogger(MeetupRsvpStream.class);

  private StreamDataProducer _producer;
  private boolean _partitionByKey;
  private boolean _keepPublishing = true;
  private ExecutorService _service;
  private File _dataFile;

  public MeetupRsvpStream(File dataFile) throws Exception {
    this(dataFile, false);
  }

  public MeetupRsvpStream(File dataFile, boolean partitionByKey)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _partitionByKey = partitionByKey;
    _producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    _dataFile = dataFile;
    _service = Executors.newFixedThreadPool(1);
  }

  public void publishStream() {
    String jsonString;
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(_dataFile), "UTF8"))) {
      while ((jsonString = bufferedReader.readLine()) != null) {
        if (!_keepPublishing) {
          return;
        }
        if (jsonString.isEmpty()) {
          LOGGER.error("Json string is empty");
          continue;
        }
        JsonNode messageJSON = JsonUtils.stringToJsonNode(jsonString);
        ObjectNode extracted = JsonUtils.newObjectNode();

        JsonNode venue = messageJSON.get("venue");
        if (venue != null) {
          extracted.set("venue_name", venue.get("venue_name"));
        }

        JsonNode event = messageJSON.get("event");
        if (event != null) {
          extracted.set("event_name", event.get("event_name"));
          extracted.set("event_id", event.get("event_id"));
          extracted.set("event_time", event.get("time"));
        }

        JsonNode group = messageJSON.get("group");
        if (group != null) {
          extracted.set("group_city", group.get("group_city"));
          extracted.set("group_country", group.get("group_country"));
          extracted.set("group_id", group.get("group_id"));
          extracted.set("group_name", group.get("group_name"));
          extracted.set("group_lat", group.get("group_lat"));
          extracted.set("group_lon", group.get("group_lon"));
        }

        extracted.set("mtime", messageJSON.get("mtime"));
        extracted.put("rsvp_count", 1);

        if (_keepPublishing) {
          if(_partitionByKey) {
            _producer.produce("meetupRSVPEvents",
                event.get("event_id").toString().getBytes(StandardCharsets.UTF_8),
                extracted.toString().getBytes(StandardCharsets.UTF_8));
          } else {
            _producer.produce("meetupRSVPEvents", extracted.toString().getBytes(StandardCharsets.UTF_8));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("encountered an error running the meetupRSVPEvents stream", e);
    }
  }

  public void stopPublishing() {
    _keepPublishing = false;
    _service.shutdown();
    _producer.close();
  }

  public void run() {
    _service.submit((Runnable) () -> {
      while (true) {
        try {
          if (!_keepPublishing) {
            break;
          }
          publishStream();
          Thread.sleep(1_000L);
        } catch (Exception e) {
          LOGGER.error(e.getMessage());
          break;
        }
      }
    });
  }
}
