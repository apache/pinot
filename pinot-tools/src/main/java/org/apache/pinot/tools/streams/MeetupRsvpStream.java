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
import java.net.URI;
import java.util.Properties;
import javax.websocket.ClientEndpointConfig;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.glassfish.tyrus.client.ClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MeetupRsvpStream {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MeetupRsvpStream.class);

  protected final boolean _partitionByKey;
  protected final StreamDataProducer _producer;

  protected ClientManager _client;
  protected volatile boolean _keepPublishing;

  public MeetupRsvpStream()
      throws Exception {
    this(false);
  }

  public MeetupRsvpStream(boolean partitionByKey)
      throws Exception {
    _partitionByKey = partitionByKey;

    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
  }

  public void run()
      throws Exception {
    _client = ClientManager.createClient();
    _keepPublishing = true;

    _client.connectToServer(new Endpoint() {
      @Override
      public void onOpen(Session session, EndpointConfig config) {
        session.addMessageHandler(String.class, getMessageHandler());
      }
    }, ClientEndpointConfig.Builder.create().build(), new URI("wss://stream.meetup.com/2/rsvps"));
  }

  public void stopPublishing() {
    _keepPublishing = false;
    _client.shutdown();
    _producer.close();
  }

  protected MessageHandler.Whole<String> getMessageHandler() {
    return message -> {
      try {
        JsonNode messageJson = JsonUtils.stringToJsonNode(message);
        ObjectNode extractedJson = JsonUtils.newObjectNode();

        JsonNode venue = messageJson.get("venue");
        if (venue != null) {
          extractedJson.set("venue_name", venue.get("venue_name"));
        }

        JsonNode event = messageJson.get("event");
        String eventId = "";
        if (event != null) {
          extractedJson.set("event_name", event.get("event_name"));
          eventId = event.get("event_id").asText();
          extractedJson.put("event_id", eventId);
          extractedJson.set("event_time", event.get("time"));
        }

        JsonNode group = messageJson.get("group");
        if (group != null) {
          extractedJson.set("group_city", group.get("group_city"));
          extractedJson.set("group_country", group.get("group_country"));
          extractedJson.set("group_id", group.get("group_id"));
          extractedJson.set("group_name", group.get("group_name"));
          extractedJson.set("group_lat", group.get("group_lat"));
          extractedJson.set("group_lon", group.get("group_lon"));
        }

        extractedJson.set("mtime", messageJson.get("mtime"));
        extractedJson.put("rsvp_count", 1);

        if (_keepPublishing) {
          if (_partitionByKey) {
            _producer.produce("meetupRSVPEvents", StringUtil.encodeUtf8(eventId),
                StringUtil.encodeUtf8(extractedJson.toString()));
          } else {
            _producer.produce("meetupRSVPEvents", StringUtil.encodeUtf8(extractedJson.toString()));
          }
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing the message: {}", message, e);
      }
    };
  }
}
