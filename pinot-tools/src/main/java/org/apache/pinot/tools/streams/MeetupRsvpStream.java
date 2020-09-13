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
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.websocket.ClientEndpointConfig;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.glassfish.tyrus.client.ClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeetupRsvpStream {
  private static final Logger LOGGER = LoggerFactory.getLogger(MeetupRsvpStream.class);
  private StreamDataProducer producer;
  private boolean keepPublishing = true;
  private boolean _partitionByKey;
  private ClientManager client;

  public MeetupRsvpStream() throws Exception {
    this(false);
  }

  public MeetupRsvpStream(boolean partitionByKey)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _partitionByKey = partitionByKey;
    producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
  }

  public void stopPublishing() {
    keepPublishing = false;
    producer.close();
    client.shutdown();
  }

  public void run() {
    try {
      ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
      client = ClientManager.createClient();
      client.connectToServer(new Endpoint() {

        @Override
        public void onOpen(Session session, EndpointConfig config) {
          try {
            session.addMessageHandler(new MessageHandler.Whole<String>() {

              @Override
              public void onMessage(String message) {
                try {
                  JsonNode messageJSON = JsonUtils.stringToJsonNode(message);
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

                  if (keepPublishing) {
                    if(_partitionByKey) {
                      producer.produce("meetupRSVPEvents",
                          event.get("event_id").toString().getBytes(StandardCharsets.UTF_8),
                          extracted.toString().getBytes(StandardCharsets.UTF_8));
                    } else {
                      producer.produce("meetupRSVPEvents", extracted.toString().getBytes(StandardCharsets.UTF_8));
                    }
                  }
                } catch (Exception e) {
                  LOGGER.error("error processing raw event ", e);
                }
              }
            });
            session.getBasicRemote().sendText("");
          } catch (IOException e) {
            LOGGER.error("found an event where data did not have all the fields, don't care about for quickstart", e);
          }
        }
      }, cec, new URI("wss://stream.meetup.com/2/rsvps"));
    } catch (Exception e) {
      LOGGER.error("encountered an error running the meetupRSVPEvents stream", e);
    }
  }
}
