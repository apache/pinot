package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.impl.LinearAggregator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FrameworkTest {
  @Test
  public void testLinearAggregator() {
    LinearAggregator agg = new LinearAggregator();

    Entity e1 = new Entity("e:one");
    Entity e2 = new Entity("e:two");
    Entity e3 = new Entity("e:three");

    Map<Entity, Double> scores1 = new HashMap<>();
    scores1.put(e1, 10d);
    scores1.put(e2, 20d);
    scores1.put(e3, 30d);
    PipelineResult res1 = new PipelineResult(scores1);

    Map<Entity, Double> scores2 = new HashMap<>();
    scores2.put(e1, 3d);
    scores2.put(e2, 6d);
    scores2.put(e3, 9d);
    PipelineResult res2 = new PipelineResult(scores2);

    Map<String, PipelineResult> results = new HashMap<>();
    results.put("p1", res1);
    results.put("p2", res2);

    List<Entity> entities = agg.aggregate(results);
    Assert.assertEquals(entities.get(0), e3);
    Assert.assertEquals(entities.get(1), e2);
    Assert.assertEquals(entities.get(2), e1);
  }
}
