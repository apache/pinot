package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MaxScoreSetTest {
  @Test
  public void testContains() {
    MaxScoreSet<Entity> s = new MaxScoreSet<>();
    s.add(makeEntity(1.0));

    Assert.assertTrue(s.contains(makeEntity(1.0)));
    Assert.assertFalse(s.contains(makeEntity(0.9)));
  }

  @Test
  public void testAdd() {
    MaxScoreSet<Entity> s = new MaxScoreSet<>();
    s.add(makeEntity(1.0));

    s.add(makeEntity(0.9));
    Assert.assertTrue(s.contains(makeEntity(1.0)));
    Assert.assertFalse(s.contains(makeEntity(0.9)));

    s.add(makeEntity(1.1));
    Assert.assertTrue(s.contains(makeEntity(1.1)));
    Assert.assertFalse(s.contains(makeEntity(1.0)));
  }

  @Test
  public void testRemove() {
    MaxScoreSet<Entity> s = new MaxScoreSet<>();
    s.add(makeEntity(1.0));

    s.remove(makeEntity(0.9));
    Assert.assertTrue(s.contains(makeEntity(1.0)));

    s.remove(makeEntity(1.0));
    Assert.assertFalse(s.contains(makeEntity(1.0)));
  }

  private static Entity makeEntity(double score) {
    return new Entity("aaa", score, Collections.<Entity>emptyList());
  }
}
