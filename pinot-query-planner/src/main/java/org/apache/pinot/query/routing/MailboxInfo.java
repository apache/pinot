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

import java.util.List;


/**
 * {@code MailboxInfo} wraps the mailbox information from/to one connected server.
 */
public class MailboxInfo {
  private final String _hostname;
  private final int _port;
  private final List<Integer> _workerIds;

  public MailboxInfo(String hostname, int port, List<Integer> workerIds) {
    _hostname = hostname;
    _port = port;
    _workerIds = workerIds;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public List<Integer> getWorkerIds() {
    return _workerIds;
  }

  @Override
  public String toString() {
    return _hostname + ":" + _port + "|" + _workerIds;
  }
}
