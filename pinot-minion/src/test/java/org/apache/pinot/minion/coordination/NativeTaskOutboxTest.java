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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Lost mutation and acknowledgement replies must retain the same task attempt and durable command sequence.
public class NativeTaskOutboxTest {
  @Test
  public void testUnknownOutcomeAndAckRetryRetainExactRequest() throws Exception {
    Path directory = Files.createTempDirectory("native-task-outbox-");
    try {
      Path journal = directory.resolve("session");
      MetadataRpc.ParticipantToken participant = MetadataRpc.ParticipantToken.newBuilder()
          .setHistoryId(UUID.randomUUID().toString()).setParticipantId("Minion_test")
          .setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1)
          .setRole(MetadataRpc.ParticipantRole.MINION).build();
      MetadataRpc.CommandResult ok = MetadataRpc.CommandResult.newBuilder().setStatus(MetadataRpc.ResultStatus.OK)
          .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(participant.getHistoryId()).setIndex(10))
          .setClientEpoch(1).build();
      NativeMinionRuntime.Rpc rpc = mock(NativeMinionRuntime.Rpc.class);
      List<MetadataRpc.TaskCommandRequest> operations = new ArrayList<>();
      AtomicInteger acknowledgements = new AtomicInteger();
      when(rpc.taskExecute(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.TaskCommandRequest request = invocation.getArgument(0);
        if (request.hasRegister()) {
          assertTrue(Files.exists(journal.resolve("registration.pb")));
          return ok;
        }
        if (request.hasOperation()) {
          operations.add(request);
          assertEquals(MetadataRpc.TaskCommandRequest.parseFrom(Files.readAllBytes(journal.resolve("pending.pb"))),
              request);
          if (operations.size() == 1) {
            throw new IOException("Reply lost after commit");
          }
          return ok;
        }
        assertEquals(MetadataRpc.CommandResult.parseFrom(Files.readAllBytes(journal.resolve("result.pb"))), ok);
        if (acknowledgements.incrementAndGet() == 1) {
          throw new IOException("Acknowledgement reply lost");
        }
        return ok;
      });
      NativeTaskOutbox outbox = new NativeTaskOutbox(rpc, participant, journal, 1000);
      outbox.enqueue(MetadataRpc.SequencedTaskOperation.newBuilder().setClaim(MetadataRpc.TaskClaim.newBuilder()
          .setTaskId(UUID.randomUUID().toString()).setExpectedVersion(1)));
      assertThrows(IOException.class, outbox::resolve);
      assertTrue(outbox.pending());
      assertThrows(IOException.class, outbox::resolve);
      assertTrue(outbox.pending());
      assertEquals(outbox.resolve(), ok);
      assertFalse(outbox.pending());
      assertEquals(operations.size(), 2);
      assertEquals(operations.get(0), operations.get(1));
      assertEquals(acknowledgements.get(), 2);
      outbox.enqueue(MetadataRpc.SequencedTaskOperation.newBuilder().setClaim(MetadataRpc.TaskClaim.newBuilder()
          .setTaskId(UUID.randomUUID().toString()).setExpectedVersion(1)));
      assertEquals(outbox.resolve(), ok);
      assertEquals(operations.get(2).getOperation().getSequence(), 2L);
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }
}
