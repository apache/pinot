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
package org.apache.pinot.query.mailbox.channel;

import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.Test;


/// Negative-path tests for the fail-fast startup gates added to {@link GrpcMailboxServer} in commit
/// 1d29438dc0 ("Fail fast on invalid gRPC mailbox transport configuration"). Each test wires the
/// corresponding bad configuration value through a {@link MailboxService}; the underlying
/// {@code Preconditions.checkArgument} fires inside the {@code GrpcMailboxServer} constructor that
/// {@link MailboxService#start()} invokes, so the failure surfaces during {@code start()} — which is
/// the whole point of fail-at-startup.
public class GrpcMailboxServerValidationTest {

  /// Pins the gate {@code _inboundMessageCredit > 0} in
  /// {@link GrpcMailboxServer}: a zero credit value makes the manual inbound flow-control prefetch a no-op
  /// (the server never calls {@code request(...)} with a positive amount) so the stream would stall
  /// immediately. The startup gate ensures this misconfiguration is rejected at boot rather than producing
  /// silent hangs on the first query.
  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*inbound.message.credit.*must be positive.*")
  public void testStartRejectsZeroInboundMessageCredit() {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_GRPC_INBOUND_MESSAGE_CREDIT, 0));
    MailboxService mailboxService = new MailboxService(
        "localhost", QueryTestUtils.getAvailablePort(), InstanceType.BROKER, config);
    // start() builds the GrpcMailboxServer; the precondition fires inside that constructor and propagates
    // out — no shutdown needed because the server never finished starting.
    mailboxService.start();
  }

  /// Pins the gate {@code _flowControlWindowBytes >= maxInboundMessageSize} in
  /// {@link GrpcMailboxServer}: a window smaller than the largest possible single message produces a
  /// pathological stream where {@code isReady()} flaps on every message because no single message can
  /// ever fit in the available credit. Verifies that a 1 KiB flow-control window combined with the
  /// default 16 MiB max inbound message size is rejected at startup.
  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*flow.control.window.*must be >=.*max.msg.size.*")
  public void testStartRejectsFlowControlWindowSmallerThanMaxMessageSize() {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_GRPC_FLOW_CONTROL_WINDOW_BYTES, 1024));
    // Leave KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES at its default (16 MiB) so the gate is
    // exercised purely by the too-small flow-control window.
    MailboxService mailboxService = new MailboxService(
        "localhost", QueryTestUtils.getAvailablePort(), InstanceType.BROKER, config);
    mailboxService.start();
  }
}
