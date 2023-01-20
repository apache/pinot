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
package org.apache.pinot.query.mailbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.apache.pinot.query.routing.VirtualServerAddress;


public class JsonMailboxIdentifier implements MailboxIdentifier {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String _jobId;
  private final String _from;
  private final String _to;

  private final VirtualServerAddress _fromAddress;
  private final VirtualServerAddress _toAddress;

  @JsonCreator
  public JsonMailboxIdentifier(
      @JsonProperty(value = "jobId") String jobId,
      @JsonProperty(value = "from") String from,
      @JsonProperty(value = "to") String to
  ) {
    _jobId = jobId;
    _from = from;
    _to = to;
    _fromAddress = VirtualServerAddress.parse(_from);
    _toAddress = VirtualServerAddress.parse(_to);
  }

  public JsonMailboxIdentifier(
      String jobId,
      VirtualServerAddress from,
      VirtualServerAddress to
  ) {
    _jobId = jobId;
    _from = from.toString();
    _to = to.toString();
    _fromAddress = from;
    _toAddress = to;
  }

  public static JsonMailboxIdentifier parse(final String mailboxId) {
    try {
      return MAPPER.readValue(mailboxId, JsonMailboxIdentifier.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid mailboxID: '" + mailboxId + "'. If you see this exception it may "
          + "be because you are doing a rolling upgrade from an old version of Pinot that is not backwards "
          + "compatible with the current V2 engine.", e);
    }
  }

  @Override
  public String getJobId() {
    return _jobId;
  }

  public String getFrom() {
    return _from;
  }

  @JsonIgnore
  @Override
  public VirtualServerAddress getFromHost() {
    return _fromAddress;
  }

  public String getTo() {
    return _to;
  }

  @JsonIgnore
  @Override
  public VirtualServerAddress getToHost() {
    return _toAddress;
  }

  @JsonIgnore
  @Override
  public boolean isLocal() {
    return _fromAddress.equals(_toAddress);
  }

  @Override
  public String toString() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JsonMailboxIdentifier that = (JsonMailboxIdentifier) o;
    return Objects.equals(_jobId, that._jobId) && Objects.equals(_from, that._from) && Objects.equals(_to, that._to)
        && Objects.equals(_fromAddress, that._fromAddress) && Objects.equals(_toAddress, that._toAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_jobId, _from, _to, _fromAddress, _toAddress);
  }
}
