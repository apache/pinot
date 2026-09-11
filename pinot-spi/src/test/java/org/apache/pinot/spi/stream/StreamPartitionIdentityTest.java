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
package org.apache.pinot.spi.stream;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Equality, ordering, and validation for [StreamPartitionIdentity].
public class StreamPartitionIdentityTest {

  @Test
  public void testV1Identity() {
    StreamPartitionIdentity identity = StreamPartitionIdentity.v1(10000);
    assertEquals(identity.getFormatVersion(), StreamPartitionIdentity.FORMAT_VERSION_V1);
    assertFalse(identity.isV2());
    assertEquals(identity.getPartitionGroupId(), 10000);
    assertThrows(IllegalStateException.class, identity::getTopicId);
    assertThrows(IllegalStateException.class, identity::getPartitionId);
    assertEquals(identity.toString(), "v1:partitionGroupId=10000");
    assertEquals(identity, StreamPartitionIdentity.v1(10000));
    assertEquals(identity.hashCode(), StreamPartitionIdentity.v1(10000).hashCode());
  }

  @Test
  public void testV2Identity() {
    StreamPartitionIdentity identity = StreamPartitionIdentity.v2(1, 0);
    assertEquals(identity.getFormatVersion(), StreamPartitionIdentity.FORMAT_VERSION_V2);
    assertTrue(identity.isV2());
    assertEquals(identity.getTopicId(), 1);
    assertEquals(identity.getPartitionId(), 0);
    assertThrows(IllegalStateException.class, identity::getPartitionGroupId);
    assertEquals(identity.toString(), "v2:topicId=1,partitionId=0");
    assertEquals(identity, StreamPartitionIdentity.v2(1, 0));
    assertEquals(identity.hashCode(), StreamPartitionIdentity.v2(1, 0).hashCode());
  }

  @Test
  public void testHistoricalCollisionPairIsDistinct() {
    StreamPartitionIdentity v1Packed = StreamPartitionIdentity.v1(10000);
    StreamPartitionIdentity v2Topic0Large = StreamPartitionIdentity.v2(0, 10000);
    StreamPartitionIdentity v2Topic1Zero = StreamPartitionIdentity.v2(1, 0);

    assertNotEquals(v1Packed, v2Topic0Large);
    assertNotEquals(v1Packed, v2Topic1Zero);
    assertNotEquals(v2Topic0Large, v2Topic1Zero);
    assertTrue(v1Packed.compareTo(v2Topic0Large) < 0);
    assertTrue(v2Topic0Large.compareTo(v2Topic1Zero) < 0);
  }

  @Test
  public void testV2RejectsNegatives() {
    assertThrows(IllegalArgumentException.class, () -> StreamPartitionIdentity.v2(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> StreamPartitionIdentity.v2(0, -1));
  }

  @Test
  public void testV2AcceptsIntegerMaxPartition() {
    StreamPartitionIdentity identity = StreamPartitionIdentity.v2(0, Integer.MAX_VALUE);
    assertEquals(identity.getPartitionId(), Integer.MAX_VALUE);
  }
}
