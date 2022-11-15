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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;


public class StringMailboxIdentifier implements MailboxIdentifier {
  private static final Joiner JOINER = Joiner.on(':');

  private final String _mailboxIdString;
  private final String _jobId;
  private final String _fromHost;
  private final int _fromPort;
  private final String _toHost;
  private final int _toPort;

  public StringMailboxIdentifier(String jobId, String fromHost, int fromPort, String toHost,
      int toPort) {
    _jobId = jobId;
    _fromHost = fromHost;
    _fromPort = fromPort;
    _toHost = toHost;
    _toPort = toPort;
    _mailboxIdString = JOINER.join(_jobId, _fromHost, _fromPort, _toHost, _toPort);
  }

  public StringMailboxIdentifier(String mailboxId) {
    _mailboxIdString = mailboxId;
    String[] splits = mailboxId.split(":");
    Preconditions.checkState(splits.length == 5);
    _jobId = splits[0];
    _fromHost = splits[1];
    _fromPort = Integer.parseInt(splits[2]);
    _toHost = splits[3];
    _toPort = Integer.parseInt(splits[4]);

    // assert that resulting string are identical.
    Preconditions.checkState(
        JOINER.join(_jobId, _fromHost, _fromPort, _toHost, _toPort).equals(_mailboxIdString));
  }

  @Override
  public String getJobId() {
    return _jobId;
  }

  @Override
  public String getFromHost() {
    return _fromHost;
  }

  @Override
  public int getFromPort() {
    return _fromPort;
  }

  @Override
  public String getToHost() {
    return _toHost;
  }

  @Override
  public int getToPort() {
    return _toPort;
  }

  @Override
  public boolean isLocal() {
    return _fromHost.equals(_toHost) && _fromPort == _toPort;
  }

  @Override
  public String toString() {
    return _mailboxIdString;
  }

  @Override
  public int hashCode() {
    return _mailboxIdString.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    return (that instanceof StringMailboxIdentifier) && _mailboxIdString.equals(
        ((StringMailboxIdentifier) that)._mailboxIdString);
  }
}
