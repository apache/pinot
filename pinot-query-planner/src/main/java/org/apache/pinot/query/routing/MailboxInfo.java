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


/// `MailboxInfo` wraps the mailbox information from/to one connected server.
public class MailboxInfo {
  private final String _hostname;
  private final int _port;
  private final List<Integer> _workerIds;
  private final long _instanceIdHash;

  public MailboxInfo(String hostname, int port, List<Integer> workerIds) {
    this(hostname, port, workerIds, 0);
  }

  /// @param instanceIdHash hash of the instance id of the server (see [QueryServerInstance#hashInstanceId(String)]),
  ///                       or 0 if unknown
  public MailboxInfo(String hostname, int port, List<Integer> workerIds, long instanceIdHash) {
    _hostname = hostname;
    _port = port;
    _workerIds = workerIds;
    _instanceIdHash = instanceIdHash;
  }

  /// Creates the mailbox information of receiver workers on `server`, for the senders that send to them. It carries
  /// the hash of the instance id of the server, so that another instance that a sender reaches through a stale
  /// connection rejects the stream.
  public static MailboxInfo ofReceivers(QueryServerInstance server, List<Integer> workerIds) {
    return new MailboxInfo(server.getHostname(), server.getQueryMailboxPort(), workerIds, server.getInstanceIdHash());
  }

  /// Creates the mailbox information of sender workers on `server`, for the receivers that read from them. Receivers
  /// never connect to senders, so it does not carry the identity of the server.
  public static MailboxInfo ofSenders(QueryServerInstance server, List<Integer> workerIds) {
    return new MailboxInfo(server.getHostname(), server.getQueryMailboxPort(), workerIds);
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

  /// Returns the hash of the instance id of the server at [#getHostname()]:[#getPort()], or 0 if unknown. Only the
  /// mailbox information of receivers carries it, see [#ofReceivers].
  public long getInstanceIdHash() {
    return _instanceIdHash;
  }

  @Override
  public String toString() {
    return _hostname + ":" + _port + "|" + _workerIds;
  }
}
