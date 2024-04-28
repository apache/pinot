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
package org.apache.pinot.query.planner.physical;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MailboxIdTest {

  @Test
  public void testJson() {
    MailboxId mailboxId = new MailboxId(1036791369000154169L, 2, 4, 1, 4);

    assertEquals(mailboxId.toString(),
        "{\"requestId\":1036791369000154169,\"senderStageId\":2,\"senderWorkerId\":4,"
            + "\"receiverStageId\":1,\"receiverWorkerId\":4}",
        "MailboxId.toString() should return a JSON string representation of the mailboxId object");
  }

  @Test
  public void testPipeString() {
    MailboxId mailboxId = new MailboxId(1036791369000154169L, 2, 4, 1, 4);

    assertEquals(mailboxId.toPipeString(), "1036791369000154169|2|4|1|4",
        "MailboxId.toPipeString() should return a pipe-separated string representation of the mailboxId object");
  }

  @Test
  public void testFromPipeString() {
    MailboxId mailboxId = MailboxId.fromPipeString("1036791369000154169|2|4|1|4");

    assertEquals(mailboxId.getRequestId(), 1036791369000154169L,
        "MailboxId.getRequestId() should return the request ID of the mailboxId object");
    assertEquals(mailboxId.getSenderStageId(), 2,
        "MailboxId.getSenderStageId() should return the sender stage ID of the mailboxId object");
    assertEquals(mailboxId.getSenderWorkerId(), 4,
        "MailboxId.getSenderWorkerId() should return the sender worker ID of the mailboxId object");
    assertEquals(mailboxId.getReceiverStageId(), 1,
        "MailboxId.getReceiverStageId() should return the receiver stage ID of the mailboxId object");
    assertEquals(mailboxId.getReceiverWorkerId(), 4,
        "MailboxId.getReceiverWorkerId() should return the receiver worker ID of the mailboxId object");
  }

  @Test
  public void testEncodeDecodePipe() {
    MailboxId mailboxId = new MailboxId(1036791369000154169L, 2, 4, 1, 4);
    String pipeString = mailboxId.toPipeString();
    MailboxId decodedMailboxId = MailboxId.fromPipeString(pipeString);
    assertEquals(mailboxId, decodedMailboxId);
  }
}
