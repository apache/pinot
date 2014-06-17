package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    Assert.assertNull(sel1.selectServer(new Partition(1), candidates, null, null));


    // Predefined selection has other server not in the candidates list
    ServerInstance s4 = new ServerInstance("localhost", 8083);
    Partition p0 = new Partition(0);
    Map<Partition, ServerInstance> mp = new HashMap<Partition, ServerInstance>();
    mp.put(p0, s4);
    BucketingSelection predefinedSelection = new BucketingSelection(mp);

    candidates.addAll(Arrays.asList(servers));
    for ( int i =0; i < 10; i++)
    {
      Partition p = new Partition(0);
      ServerInstance c1 = sel1.selectServer(p, candidates, predefinedSelection, null);
      ServerInstance c2 = sel2.selectServer(p, candidates, predefinedSelection, null);

      Assert.assertEquals(c1,c2);
    }

    // Test Predefined selection for random
    testPredefinedSelection(sel1, null);

  }

  @Test
  public void testRoundRobinSelection() {
    ReplicaSelection sel1 = new RoundRobinReplicaSelection(0);

    ServerInstance s1 = new ServerInstance("localhost", 8080);
    ServerInstance s2 = new ServerInstance("localhost", 8081);
    ServerInstance s3 = new ServerInstance("localhost", 8082);

    ServerInstance[] servers = { s1, s2, s3 };

    // Verify for an empty list, selectServer returns null
    List<ServerInstance> candidates = new ArrayList<ServerInstance>();
    Assert.assertNull(sel1.selectServer(new Partition(1), candidates, null, null));


    // Predefined selection has other server not in the candidates list
    ServerInstance s4 = new ServerInstance("localhost", 8083);
    Partition p0 = new Partition(0);
    Map<Partition, ServerInstance> mp = new HashMap<Partition, ServerInstance>();
    mp.put(p0, s4);
    BucketingSelection predefinedSelection = new BucketingSelection(mp);

    candidates.addAll(Arrays.asList(servers));
    for (int i =0 ;i < 10; i++)
    {
      int num = i%candidates.size();
      Assert.assertEquals("Round :" + i, candidates.get(num), sel1.selectServer(new Partition(0), candidates, predefinedSelection, null));
    }


    // Test Predefined selection for round robin
    testPredefinedSelection(sel1, null);
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
    Assert.assertNull(sel1.selectServer(new Partition(1), candidates, null, null));
    BucketingSelection predefinedSelection = new BucketingSelection(null);

    candidates.addAll(Arrays.asList(servers));
    for (int i =0 ;i < 10; i++)
    {
      int num = i%candidates.size();
      BucketKey k = ks[num];
      Assert.assertEquals("Round :" + i, candidates.get(num), sel1.selectServer(new Partition(0), candidates, predefinedSelection, k));
    }


    // Test Predefined selection for hash-based
    testPredefinedSelection(sel1, k1);
  }

  /**
   * Here we test if the pre-selected servers are picked.
   * @param sel Replica selection for which to test
   * @param hashKey HashKey
   */
  private void testPredefinedSelection(ReplicaSelection sel, Object hashKey)
  {
    ServerInstance s1 = new ServerInstance("localhost", 8080);
    ServerInstance s2 = new ServerInstance("localhost", 8081);
    ServerInstance s3 = new ServerInstance("localhost", 8082);

    ServerInstance s4 = new ServerInstance("localhost", 8083);
    ServerInstance s5 = new ServerInstance("localhost", 8084);
    ServerInstance s6 = new ServerInstance("localhost", 8085);

    Partition p0 = new Partition(0);
    Partition p1 = new Partition(1);
    Partition p2 = new Partition(2);

    Map<Partition, ServerInstance> mp = new HashMap<Partition, ServerInstance>();
    mp.put(p0, s1);
    mp.put(p1, s2);
    mp.put(p2, s3);

    BucketingSelection sel1 = new BucketingSelection(mp);

    //Candidates have lot more servers
    ServerInstance[] servers = { s4, s5, s6, s1, s2, s3 };
    List<ServerInstance> candidates = new ArrayList<ServerInstance>(Arrays.asList(servers));

    for (int i=0; i < 10; i++)
    {
      Partition p = new Partition(i%3);
      ServerInstance s = sel.selectServer(p, candidates, sel1, hashKey);
      Assert.assertEquals(mp.get(p), s);
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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      BucketKey other = (BucketKey) obj;
      if (_key != other._key)
        return false;
      return true;
    }

  }
}
