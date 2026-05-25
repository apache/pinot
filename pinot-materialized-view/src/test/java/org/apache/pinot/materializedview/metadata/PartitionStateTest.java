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
package org.apache.pinot.materializedview.metadata;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PartitionStateTest {

  @Test
  public void testEncodeValid() {
    assertEquals(PartitionState.VALID.encode(), "V");
  }

  @Test
  public void testEncodeStale() {
    assertEquals(PartitionState.STALE.encode(), "S");
  }

  @Test
  public void testDecodeValid() {
    assertEquals(PartitionState.decode("V"), PartitionState.VALID);
  }

  @Test
  public void testDecodeStale() {
    assertEquals(PartitionState.decode("S"), PartitionState.STALE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeUnknownCodeRejected() {
    PartitionState.decode("E");
  }

  @Test
  public void testRoundTrip() {
    for (PartitionState state : PartitionState.values()) {
      assertEquals(PartitionState.decode(state.encode()), state);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeUnknownCode() {
    PartitionState.decode("X");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeEmptyString() {
    PartitionState.decode("");
  }
}
