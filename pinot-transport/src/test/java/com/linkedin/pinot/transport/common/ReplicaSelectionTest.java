/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.response.ServerInstance;


public class ReplicaSelectionTest {

  @Test
  public void testRandomSelection() {
    // Create 2 random selection with the same seed. Ensure they return the same values
    RandomReplicaSelection sel1 = new RandomReplicaSelection(0);
    RandomReplicaSelection sel2 = new RandomReplicaSelection(0);

    ServerInstance s1 = new ServerInstance("localhost", 8080);
    ServerInstance s2 = new ServerInstance("localhost", 8081);
    ServerInstance s3 = new ServerInstance("localhost", 8082);

    ServerInstance[] servers = { s1, s2, s3 };

    // Verify for an empty list, selectServer returns null
    List<ServerInstance> candidates = new ArrayList<ServerInstance>();
    Assert.assertNull(sel1.selectServer(new SegmentId("1"), candidates, null));

  }

  @Test
  public void testRoundRobinSelection() {
    ReplicaSelection sel1 = new RoundRobinReplicaSelection();

    ServerInstance s1 = new ServerInstance("localhost", 8080);
    ServerInstance s2 = new ServerInstance("localhost", 8081);
    ServerInstance s3 = new ServerInstance("localhost", 8082);

    ServerInstance[] servers = { s1, s2, s3 };

    // Verify for an empty list, selectServer returns null
    List<ServerInstance> candidates = new ArrayList<ServerInstance>();
    Assert.assertNull(sel1.selectServer(new SegmentId("1"), candidates, null));

  }

  @Test
  public void testHashBasedSelection() {
    ReplicaSelection sel1 = new HashReplicaSelection();

    ServerInstance s1 = new ServerInstance("localhost", 8080);
    ServerInstance s2 = new ServerInstance("localhost", 8081);
    ServerInstance s3 = new ServerInstance("localhost", 8082);
    BucketKey k1 = new BucketKey(0);
    BucketKey k2 = new BucketKey(1);
    BucketKey k3 = new BucketKey(2);

    BucketKey[] ks = { k1, k2, k3 };

    ServerInstance[] servers = { s1, s2, s3 };

    // Verify for an empty list, selectServer returns null
    List<ServerInstance> candidates = new ArrayList<ServerInstance>();
    Assert.assertNull(sel1.selectServer(new SegmentId("1"), candidates, null));

    candidates.addAll(Arrays.asList(servers));
    for (int i = 0; i < 10; i++) {
      int num = i % candidates.size();
      BucketKey k = ks[num];
      Assert
          .assertEquals(sel1.selectServer(new SegmentId("0"), candidates, k), candidates.get(num), "Round :" + i);
    }

  }

  public static class BucketKey {
    private final int _key;

    public BucketKey(int key) {
      _key = key;
    }

    @Override
    public int hashCode() {
      return _key;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      BucketKey other = (BucketKey) obj;
      if (_key != other._key) {
        return false;
      }
      return true;
    }

  }
}
