package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.impl.LinearAggregator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FrameworkTest {
  @Test
  public void testLinearAggregator() {
    LinearAggregator agg = new LinearAggregator();

    Entity e1 = new Entity("e:one", 1.0);
    Entity e2 = new Entity("e:two", 2.0);
    Entity e3 = new Entity("e:three", 3.0);

    List<Entity> scores1 = new ArrayList<>();
    scores1.add(e1);
    scores1.add(e2);
    scores1.add(e3);
    PipelineResult res1 = new PipelineResult(scores1);

    List<Entity> scores2 = new ArrayList<>();
    scores2.add(e1);
    scores2.add(e2);
    scores2.add(e3);
    PipelineResult res2 = new PipelineResult(scores2);

    Map<String, PipelineResult> results = new HashMap<>();
    results.put("p1", res1);
    results.put("p2", res2);

    List<Entity> entities = agg.aggregate(results);
    Assert.assertEquals(entities.get(0).getUrn(), e3.getUrn());
    Assert.assertEquals(entities.get(1).getUrn(), e2.getUrn());
    Assert.assertEquals(entities.get(2).getUrn(), e1.getUrn());
  }
}
