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
import javax.websocket.MessageHandler;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtils;


public class MeetupRsvpJsonStream extends MeetupRsvpStream {

  public MeetupRsvpJsonStream()
      throws Exception {
    super();
  }

  @Override
  protected MessageHandler.Whole<String> getMessageHandler() {
    return message -> {
      try {
        // Replace nested json with serialized json string
        ObjectNode messageJson = (ObjectNode) JsonUtils.stringToJsonNode(message);
        serializeJsonField(messageJson, "venue");
        serializeJsonField(messageJson, "member");
        serializeJsonField(messageJson, "event");
        serializeJsonField(messageJson, "group");

        if (_keepPublishing) {
          _producer.produce("meetupRSVPEvents", StringUtils.encodeUtf8(messageJson.toString()));
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing the message: {}", message, e);
      }
    };
  }

  private static void serializeJsonField(ObjectNode messageJson, String fieldName) {
    JsonNode jsonNode = messageJson.get(fieldName);
    if (jsonNode != null && jsonNode.isObject()) {
      messageJson.put(fieldName, jsonNode.toString());
    } else {
      messageJson.put(fieldName, "{}");
    }
  }
}
