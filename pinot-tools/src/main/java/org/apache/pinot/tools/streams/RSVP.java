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


class RSVP {
  private final String _eventId;
  private final String _rsvpId;
  private final JsonNode _payload;

  RSVP(String eventId, String rsvpId, JsonNode payload) {
    _eventId = eventId;
    _rsvpId = rsvpId;
    _payload = payload;
  }

  public String getEventId() {
    return _eventId;
  }

  public String getRsvpId() {
    return _rsvpId;
  }

  public JsonNode getPayload() {
    return _payload;
  }
}
