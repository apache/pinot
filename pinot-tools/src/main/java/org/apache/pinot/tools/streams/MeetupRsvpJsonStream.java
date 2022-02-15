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

import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;


public class MeetupRsvpJsonStream extends MeetupRsvpStream {

  public MeetupRsvpJsonStream()
      throws Exception {
    super();
  }

  public MeetupRsvpJsonStream(boolean partitionByKey)
      throws Exception {
    super(partitionByKey);
  }

  @Override
  protected Consumer<RSVP> createConsumer() {
    return message -> {
      if (_partitionByKey) {
        try {
          _producer.produce(_topicName, message.getRsvpId().getBytes(UTF_8), message.getPayload().toString()
              .getBytes(UTF_8));
        } catch (Exception e) {
          LOGGER.error("Caught exception while processing the message: {}", message, e);
        }
      } else {
        _producer.produce(_topicName, message.getPayload().toString().getBytes(UTF_8));
      }
    };
  }
}
