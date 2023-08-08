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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * {@code MailboxMetadata} wraps around a list of mailboxes information from/to one connected stage.
 *  It contains the following information:
 *  <ul>
 *    <li>MailboxId: the unique id of the mailbox</li>
 *    <li>VirtualAddress: the virtual address of the mailbox</li>
 *    <li>CustomProperties: the custom properties of the mailbox</li>
 *  </ul>
 */
public class MailboxMetadata {
  private final List<String> _mailBoxIdList;
  private final List<VirtualServerAddress> _virtualAddressList;
  private final Map<String, String> _customProperties;

  public MailboxMetadata() {
    _mailBoxIdList = new ArrayList<>();
    _virtualAddressList = new ArrayList<>();
    _customProperties = new HashMap<>();
  }

  public MailboxMetadata(List<String> mailBoxIdList, List<VirtualServerAddress> virtualAddressList,
      Map<String, String> customProperties) {
    _mailBoxIdList = mailBoxIdList;
    _virtualAddressList = virtualAddressList;
    _customProperties = customProperties;
  }

  public List<String> getMailBoxIdList() {
    return _mailBoxIdList;
  }

  public String getMailBoxId(int index) {
    return _mailBoxIdList.get(index);
  }

  public List<VirtualServerAddress> getVirtualAddressList() {
    return _virtualAddressList;
  }

  public VirtualServerAddress getVirtualAddress(int index) {
    return _virtualAddressList.get(index);
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  @Override
  public String toString() {
    return _mailBoxIdList + "@" + _virtualAddressList.toString() + "#" + _customProperties.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(_mailBoxIdList, _virtualAddressList, _customProperties);
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
    return Objects.equals(_mailBoxIdList, that._mailBoxIdList)
        && Objects.equals(_virtualAddressList, that._virtualAddressList)
        && _customProperties.equals(that._customProperties);
  }
}
