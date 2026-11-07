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
package org.apache.pinot.query.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ReceiverIdentityTest {
  private static final String INSTANCE_A = "Server_a_8098";
  private static final String INSTANCE_B = "Server_b_8098";
  private static final ReceiverIdentity IDENTITY_A = new ReceiverIdentity(INSTANCE_A, "a:8101");

  @Test
  public void testAcceptsRequestsMeantForIt() {
    assertNull(IDENTITY_A.checkTarget(withInstanceIdHash(QueryServerInstance.hashInstanceId(INSTANCE_A))));
    // Older senders say nothing, and an unknown receiver (hash 0) is not sent
    assertNull(IDENTITY_A.checkTarget(new Metadata()));
    assertNull(IDENTITY_A.checkTarget(withInstanceIdHash(0)));
  }

  @Test
  public void testRejectsRequestsMeantForAnotherInstance() {
    long hashOfB = QueryServerInstance.hashInstanceId(INSTANCE_B);
    String reason = IDENTITY_A.checkTarget(withInstanceIdHash(hashOfB));
    assertNotNull(reason);
    assertTrue(reason.contains(Long.toHexString(hashOfB)), reason);
    assertTrue(reason.contains(IDENTITY_A.toString()), reason);
  }

  /// About half of all ids hash to a negative number. Its header text must read back as the same hash, or the check
  /// would reject every request for those instances.
  @Test
  public void testNegativeHashRoundTripsThroughHeader() {
    String instanceId = "Server_localhost_8098";
    long hash = QueryServerInstance.hashInstanceId(instanceId);
    assertTrue(hash < 0);
    Metadata headers = withInstanceIdHash(hash);
    assertEquals(headers.get(ReceiverIdentity.TARGET_INSTANCE_ID_HASH_KEY), "f6cda5a4edb6290e");
    assertNull(new ReceiverIdentity(instanceId, "localhost:8101").checkTarget(headers));
  }

  /// A header carries ASCII text only, but the hash of any id is ASCII text.
  @Test
  public void testNonAsciiInstanceId() {
    String instanceId = "Server_café_8098";
    ReceiverIdentity identity = new ReceiverIdentity(instanceId, "café:8101");
    assertNull(identity.checkTarget(withInstanceIdHash(QueryServerInstance.hashInstanceId(instanceId))));
    assertNotNull(identity.checkTarget(withInstanceIdHash(QueryServerInstance.hashInstanceId("Server_cafe_8098"))));
  }

  /// A malformed hash does not come from a Pinot sender: there is nothing to check against.
  @Test
  public void testIgnoresMalformedHash() {
    Metadata headers = new Metadata();
    headers.put(ReceiverIdentity.TARGET_INSTANCE_ID_HASH_KEY, "not-a-hash");
    assertNull(IDENTITY_A.checkTarget(headers));
  }

  /// The hash travels between brokers and servers of different versions, so it must never change.
  @Test
  public void testInstanceIdHashIsStable() {
    assertEquals(QueryServerInstance.hashInstanceId(INSTANCE_A),
        QueryServerInstance.hashInstanceId(new String(INSTANCE_A.toCharArray())));
    assertEquals(QueryServerInstance.hashInstanceId("Server_localhost_8098"), 0xf6cda5a4edb6290eL);
  }

  @Test
  public void testRecognizesRejection() {
    Metadata trailers = new Metadata();
    trailers.put(ReceiverIdentity.MISROUTED_TO_KEY, IDENTITY_A.toString());
    StatusRuntimeException rejection = Status.FAILED_PRECONDITION.asRuntimeException(trailers);
    assertTrue(ReceiverIdentity.isMisrouted(rejection));
    assertEquals(ReceiverIdentity.getMisroutedTo(rejection), "Server_a_8098 at a:8101");
    assertTrue(ReceiverIdentity.isMisrouted(new RuntimeException(rejection)));

    // Same status without the trailer, as other failed preconditions would be
    assertFalse(ReceiverIdentity.isMisrouted(Status.FAILED_PRECONDITION.asRuntimeException()));
    assertFalse(ReceiverIdentity.isMisrouted(Status.UNAVAILABLE.asRuntimeException(new Metadata())));
    assertFalse(ReceiverIdentity.isMisrouted(new RuntimeException("not a gRPC error")));
    assertFalse(ReceiverIdentity.isMisrouted(null));
  }

  private static Metadata withInstanceIdHash(long instanceIdHash) {
    Metadata headers = new Metadata();
    ReceiverIdentity.addTargetInstanceIdHash(headers, instanceIdHash);
    return headers;
  }
}
