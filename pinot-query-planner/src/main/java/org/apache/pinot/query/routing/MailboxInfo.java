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


/**
 * {@code MailboxInfo} wraps around the mailbox information of connected stages.
 *
 */
public class MailboxInfo {
  private final String _mailBoxId;
  private final String _mailBoxType;
  private final String _mailBoxHost;
  private final int _mailBoxPort;
  private final Map<String, String> _customProperties;

  public MailboxInfo(String mailBoxId, String mailBoxType, String mailBoxHost, int mailBoxPort,
      Map<String, String> customProperties) {
    _mailBoxId = mailBoxId;
    _mailBoxType = mailBoxType;
    _mailBoxHost = mailBoxHost;
    _mailBoxPort = mailBoxPort;
    _customProperties = customProperties;
  }

  public String getMailBoxId() {
    return _mailBoxId;
  }

  public String getMailBoxType() {
    return _mailBoxType;
  }

  public String getMailBoxHost() {
    return _mailBoxHost;
  }

  public int getMailBoxPort() {
    return _mailBoxPort;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }
}
