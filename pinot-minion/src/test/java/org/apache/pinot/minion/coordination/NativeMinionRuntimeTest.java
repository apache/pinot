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
package org.apache.pinot.minion.coordination;

import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// A consumed semantic heartbeat rejection must advance, and failed startup must preserve the live instance.
public class NativeMinionRuntimeTest {
  @Test
  public void testHeartbeatRecoveryAndFailedDirectoryOwnership() throws Exception {
    Path directory = Files.createTempDirectory("native-minion-lifecycle-");
    String history = UUID.randomUUID().toString();
    String instanceId = "Minion_" + UUID.randomUUID();
    try {
      Path certificate = Files.writeString(directory.resolve("test.pem"), "injected transport");
      PinotConfiguration config = new PinotConfiguration();
      String prefix = NativeMetadataConfig.PREFIX;
      config.setProperty(prefix + "historyId", history);
      config.setProperty(prefix + "participantId", instanceId);
      config.setProperty(prefix + "metadata.endpoints", "localhost:12345");
      config.setProperty(prefix + "tls.certChain", certificate.toString());
      config.setProperty(prefix + "tls.privateKey", certificate.toString());
      config.setProperty(prefix + "tls.trustCerts", certificate.toString());
      config.setProperty(prefix + "heartbeatMillis", 20L);
      config.setProperty(NativeMinionRuntime.PREFIX + "pollMillis", 5L);
      config.setProperty(NativeMinionRuntime.PREFIX + "sourceRoot", directory.toString());
      config.setProperty(NativeMinionRuntime.PREFIX + "outputRoot", directory.resolve("outputs").toString());
      config.setProperty(CommonConstants.Minion.CONFIG_OF_MINION_ID, instanceId);
      config.setProperty(CommonConstants.Helix.Instance.DATA_DIR_KEY, directory.resolve("data").toString());
      NativeMinionRuntime.Rpc membership = mock(NativeMinionRuntime.Rpc.class);
      NativeMinionRuntime.Rpc work = mock(NativeMinionRuntime.Rpc.class);
      MetadataRpc.Revision revision = MetadataRpc.Revision.newBuilder().setHistoryId(history).setIndex(10).build();
      AtomicReference<MetadataRpc.ParticipantToken> participant = new AtomicReference<>();
      CountDownLatch recovered = new CountDownLatch(1);
      when(membership.membershipRead(any(), anyLong())).thenReturn(MetadataRpc.MembershipResponse.newBuilder()
          .setStatus(ResultStatus.NOT_FOUND).setRevision(revision).build());
      when(membership.membershipExecute(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.MembershipCommandRequest request = invocation.getArgument(0);
        MetadataRpc.MembershipResponse.Builder response = MetadataRpc.MembershipResponse.newBuilder()
            .setStatus(ResultStatus.OK).setRevision(revision);
        if (request.hasRegister()) {
          participant.set(MetadataRpc.ParticipantToken.newBuilder().setHistoryId(history).setParticipantId(instanceId)
              .setRole(MetadataRpc.ParticipantRole.MINION).setIncarnation(request.getRegister().getIncarnation())
              .setRegistrationEpoch(1).build());
          return response.setParticipant(state(participant.get())).build();
        }
        if (request.hasRenew()) {
          long sequence = request.getRenew().getSequence();
          if (sequence >= 3) {
            recovered.countDown();
          }
          return response.addRenewals(MetadataRpc.RenewalResult.newBuilder().setParticipant(participant.get())
              .setSequence(sequence).setStatus(sequence == 2 ? ResultStatus.NOT_READY : ResultStatus.OK)).build();
        }
        return response.build();
      });
      when(work.taskWork(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.TaskWorkRequest request = invocation.getArgument(0);
        assertEquals(request.getMaxRecords(), 128);
        return MetadataRpc.TaskWorkResult.newBuilder().setStatus(ResultStatus.OK).setRevision(revision)
            .setParticipant(state(participant.get())).setControl(MetadataRpc.MembershipControl.newBuilder().setRound(1))
            .build();
      });
      when(work.taskExecute(any(), anyLong())).thenReturn(MetadataRpc.CommandResult.newBuilder()
          .setStatus(ResultStatus.OK).setRevision(revision).setClientEpoch(1).build());
      try (NativeMinionRuntime runtime = new NativeMinionRuntime(config, membership, work)) {
        runtime.start();
        assertTrue(recovered.await(5, TimeUnit.SECONDS), "A known NOT_READY renewal must not replay forever");
        try (NativeMinionRuntime competing = new NativeMinionRuntime(config, mock(NativeMinionRuntime.Rpc.class),
            mock(NativeMinionRuntime.Rpc.class))) {
          assertThrows(OverlappingFileLockException.class, competing::start);
          assertNotEquals(ServiceStatus.getServiceStatus(instanceId), ServiceStatus.Status.NOT_STARTED,
              "Failed data-directory ownership must not unregister the existing minion status");
        }
      }
      assertEquals(ServiceStatus.getServiceStatus(instanceId), ServiceStatus.Status.NOT_STARTED);
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  private static MetadataRpc.ParticipantState state(MetadataRpc.ParticipantToken participant) {
    return MetadataRpc.ParticipantState.newBuilder().setParticipant(participant).setExpiryRound(100)
        .addCapabilities(MetadataRpc.ServingCapability.MINION_TASK).build();
  }
}
