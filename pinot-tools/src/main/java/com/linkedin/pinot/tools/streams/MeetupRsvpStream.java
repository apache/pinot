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
package com.linkedin.pinot.tools.streams;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.glassfish.tyrus.client.ClientManager;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaJSONMessageDecoder;


public class MeetupRsvpStream {
  private Schema schema;
  private Producer<String, byte[]> producer;
  private boolean keepPublishing = true;
  private ClientManager client;

  public MeetupRsvpStream(File schemaFile) throws IOException, URISyntaxException {
    schema = Schema.fromFile(schemaFile);

    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    producer = new Producer<String, byte[]>(producerConfig);
  }

  public void stopPublishing() {
    keepPublishing = false;
    client.shutdown();
  }

  public void run() {
    try {

      final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
      final KafkaJSONMessageDecoder decoder = new KafkaJSONMessageDecoder();
      decoder.init(null, schema, null);
      client = ClientManager.createClient();
      client.connectToServer(new Endpoint() {

        @Override
        public void onOpen(Session session, EndpointConfig config) {
          try {
            session.addMessageHandler(new MessageHandler.Whole<String>() {

              @Override
              public void onMessage(String message) {
                try {

                  JSONObject messageJSON = new JSONObject(message);
                  JSONObject extracted = new JSONObject();

                  if (messageJSON.has("venue")) {
                    JSONObject venue = messageJSON.getJSONObject("venue");
                    extracted.put("venue_name", venue.getString("venue_name"));
                  }

                  if (messageJSON.has("event")) {
                    JSONObject event = messageJSON.getJSONObject("event");
                    extracted.put("event_name", event.getString("event_name"));
                    extracted.put("event_id", event.getString("event_id"));
                    extracted.put("event_time", event.getLong("time"));
                  }

                  if (messageJSON.has("group")) {
                    JSONObject group = messageJSON.getJSONObject("group");
                    extracted.put("group_city", group.getString("group_city"));
                    extracted.put("group_country", group.getString("group_country"));
                    extracted.put("group_id", group.getLong("group_id"));
                    extracted.put("group_name", group.getString("group_name"));
                  }

                  extracted.put("mtime", messageJSON.getLong("mtime"));
                  extracted.put("rsvp_count", 1);

                  if (keepPublishing) {
                    KeyedMessage<String, byte[]> data =
                        new KeyedMessage<String, byte[]>("meetupRSVPEvents", extracted.toString().getBytes("UTF-8"));
                    producer.send(data);
                  }
                } catch (Exception e) {
                  //LOGGER.error("error processing raw event ", e);
                }
              }
            });
            session.getBasicRemote().sendText("");
          } catch (IOException e) {
            //LOGGER.error("found an event where data did not have all the fields, don't care about for quickstart");
          }
        }
      }, cec, new URI("ws://stream.meetup.com/2/rsvps"));
    } catch (Exception e) {
      //e.printStackTrace();
    }
  }
}
