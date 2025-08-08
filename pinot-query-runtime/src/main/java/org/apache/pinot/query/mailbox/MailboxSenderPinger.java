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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A scheduled service that periodically pings mailbox servers to ensure their connections don't reach the idle state,
/// which would mean we would need to re-establish the connection (including TLS negotiation) before sending any,
/// message, which increases the latency of the first query sent after a period of inactivity.
public class MailboxSenderPinger extends AbstractScheduledService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSenderPinger.class);
  private final String _hostname;
  private final int _port;
  private final AbstractScheduledService.Scheduler _scheduler;
  private final ChannelManager _channelManager;
  private final Map<Pair<String, Integer>, PinotMailboxGrpc.PinotMailboxStub> _stubs = new HashMap<>();
  private final Observer _observer = new Observer();

  public MailboxSenderPinger(String hostname, int port, ChannelManager channelManager, Duration period) {
    _hostname = hostname;
    _port = port;
    _channelManager = channelManager;
    _scheduler = AbstractScheduledService.Scheduler.newFixedRateSchedule(Duration.of(2, ChronoUnit.SECONDS), period);
  }

  @Override
  protected String serviceName() {
    return "MailboxSenderPinger-" + _hostname + ":" + _port;
  }

  @Override
  protected void runOneIteration() {
    Set<Pair<String, Integer>> currentKeys = _channelManager.getKnownChannels();

    // Clean up stubs that are no longer in use.
    _stubs.keySet().retainAll(currentKeys);

    // Create stubs for new channels.
    Sets.difference(currentKeys, _stubs.keySet()).forEach(key -> {
      String hostname = key.getKey();
      Integer port = key.getValue();
      PinotMailboxGrpc.PinotMailboxStub stub = PinotMailboxGrpc.newStub(_channelManager.getChannel(hostname, port));
      LOGGER.debug("Pinging mailbox server at {}:{}", hostname, port);
      _stubs.put(key, stub);
    });

    // For each stub, send a ping request.
    Mailbox.Ping ping = Mailbox.Ping.newBuilder()
        .setHostname(_hostname)
        .setPort(_port)
        .build();
    for (PinotMailboxGrpc.PinotMailboxStub stub : _stubs.values()) {
      stub.ping(ping, _observer);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return _scheduler;
  }

  private static class Observer implements StreamObserver<Mailbox.Pong> {
    @Override
    public void onNext(Mailbox.Pong value) {
      LOGGER.debug("Received Pong from mailbox server: {}", value);
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.warn("Error while pinging mailbox server: {}", t.getMessage());
    }

    @Override
    public void onCompleted() {
    }
  }
}
