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
import java.util.List;


/**
 * {@code MailboxMetadata} wraps around a list of mailboxes information from/to one connected stage.
 *  It contains the following information:
 *  <ul>
 *    <li>MailboxId: the unique id of the mailbox</li>
 *    <li>VirtualAddress: the virtual address of the mailbox</li>
 *  </ul>
 */
public class MailboxMetadata {
  private final List<String> _mailboxIds;
  private final List<VirtualServerAddress> _virtualAddresses;

  public MailboxMetadata() {
    _mailboxIds = new ArrayList<>();
    _virtualAddresses = new ArrayList<>();
  }

  public MailboxMetadata(List<String> mailboxIds, List<VirtualServerAddress> virtualAddresses) {
    _mailboxIds = mailboxIds;
    _virtualAddresses = virtualAddresses;
  }

  public List<String> getMailboxIds() {
    return _mailboxIds;
  }

  public List<VirtualServerAddress> getVirtualAddresses() {
    return _virtualAddresses;
  }

  @Override
  public String toString() {
    return _mailboxIds + "@" + _virtualAddresses;
  }
}
