package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestReplicaSelection {

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
    for (int i =0 ;i < 10; i++)
    {
      int num = i%candidates.size();
      BucketKey k = ks[num];
      Assert.assertEquals("Round :" + i, candidates.get(num), sel1.selectServer(new SegmentId("0"), candidates, k));
    }

  }

  public static class BucketKey
  {
    private final int _key;

    public BucketKey(int key)
    {
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
