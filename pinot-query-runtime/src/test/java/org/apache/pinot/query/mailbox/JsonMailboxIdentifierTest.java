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

import org.testng.Assert;
import org.testng.annotations.Test;


public class JsonMailboxIdentifierTest {

  @Test
  public void shouldParseWhenIgnoredFieldsMissing() {
    String jsonString = "{\"jobId\": \"0_1\", \"from\": \"0@myhost:8000\", \"to\": \"1@myhost:8000\"}";
    JsonMailboxIdentifier jsonMailboxIdentifier = JsonMailboxIdentifier.parse(jsonString);

    Assert.assertEquals("0_1", jsonMailboxIdentifier.getJobId());
    Assert.assertEquals("0@myhost:8000", jsonMailboxIdentifier.getFrom());
    Assert.assertEquals("1@myhost:8000", jsonMailboxIdentifier.getTo());
    Assert.assertEquals(0, jsonMailboxIdentifier.getSenderStageId());
    Assert.assertEquals(0, jsonMailboxIdentifier.getReceiverStageId());
  }

  @Test
  public void shouldParseSenderReceiverStageIdIfPresent() {
    String jsonString = "{\"jobId\": \"0_1\", \"from\": \"0@myhost:8000\", \"to\": \"1@myhost:8000\", "
        + "\"senderStageId\": \"10\", \"receiverStageId\": \"9\"}";
    JsonMailboxIdentifier jsonMailboxIdentifier = JsonMailboxIdentifier.parse(jsonString);

    Assert.assertEquals("0_1", jsonMailboxIdentifier.getJobId());
    Assert.assertEquals("0@myhost:8000", jsonMailboxIdentifier.getFrom());
    Assert.assertEquals("1@myhost:8000", jsonMailboxIdentifier.getTo());
    Assert.assertEquals(10, jsonMailboxIdentifier.getSenderStageId());
    Assert.assertEquals(9, jsonMailboxIdentifier.getReceiverStageId());
  }
}
