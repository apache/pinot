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
package org.apache.pinot.query.routing;

import java.util.Map;
import java.util.Objects;


/**
 * {@code MailboxMetadata} wraps around the mailbox information of connected stages.
 *
 */
public class MailboxMetadata {
  private final String _mailBoxId;
  private final VirtualServerAddress _virtualAddress;
  private final Map<String, String> _customProperties;

  public MailboxMetadata(String mailBoxId, String virtualAddress, Map<String, String> customProperties) {
    _mailBoxId = mailBoxId;
    _virtualAddress = VirtualServerAddress.parse(virtualAddress);
    _customProperties = customProperties;
  }

  public String getMailBoxId() {
    return _mailBoxId;
  }

  public VirtualServerAddress getVirtualAddress() {
    return _virtualAddress;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  @Override
  public String toString() {
    return _mailBoxId + "@" + _virtualAddress.toString() + "#" + _customProperties.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(_mailBoxId, _virtualAddress, _customProperties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MailboxMetadata that = (MailboxMetadata) o;
    return Objects.equals(_mailBoxId, that._mailBoxId)
        && Objects.equals(_virtualAddress, that._virtualAddress)
        && _customProperties.equals(that._customProperties);
  }
}
