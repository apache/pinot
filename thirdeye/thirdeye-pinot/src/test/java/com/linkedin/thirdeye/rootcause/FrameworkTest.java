package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.impl.LinearAggregator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FrameworkTest {
  static class DummyPipeline extends Pipeline {
    public DummyPipeline(String name, Set<String> inputs) {
      super(name, inputs);
    }

    @Override
    public PipelineResult run(PipelineContext context) {
      return new PipelineResult(context, Collections.<Entity>emptySet());
    }
  }

  @Test
  public void testLinearAggregator() {
    LinearAggregator agg = new LinearAggregator("", Collections.<String>emptySet());

    Entity e1 = new Entity("e:one", 1.0);
    Entity e2 = new Entity("e:two", 2.1);
    Entity e3 = new Entity("e:three", 3.2);
    Entity e4 = new Entity("e:four", 4.0);

    Set<Entity> scores1 = new HashSet<>();
    scores1.add(e1);
    scores1.add(e2);
    scores1.add(e3);

    Set<Entity> scores2 = new HashSet<>();
    scores2.add(e2);
    scores2.add(e3);
    scores2.add(e4);

    Map<String, Set<Entity>> inputs = new HashMap<>();
    inputs.put("p1", scores1);
    inputs.put("p2", scores2);

    PipelineContext context = new PipelineContext(inputs);

    List<Entity> entities = new ArrayList<>(agg.run(context).getEntities());
    Collections.sort(entities, Entity.HIGHEST_SCORE_FIRST);

    Assert.assertEquals(entities.size(), 4);
    Assert.assertEquals(entities.get(0).getUrn(), e3.getUrn());
    Assert.assertEquals(entities.get(0).getScore(), 6.4);
    Assert.assertEquals(entities.get(1).getUrn(), e2.getUrn());
    Assert.assertEquals(entities.get(1).getScore(), 4.2);
    Assert.assertEquals(entities.get(2).getUrn(), e4.getUrn());
    Assert.assertEquals(entities.get(2).getScore(), 4.0);
    Assert.assertEquals(entities.get(3).getUrn(), e1.getUrn());
    Assert.assertEquals(entities.get(3).getScore(), 1.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDAGInputPipeline() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("input"));
    pipelines.add(makePipeline("output", "input"));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDAGNoOutput() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("a", "input"));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDAGNoPath() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("a", "input"));
    pipelines.add(makePipeline("output", "a", "b"));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  @Test
  public void testValidDAG() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("a", "input"));
    pipelines.add(makePipeline("b", "input", "a"));
    pipelines.add(makePipeline("output", "a", "b", "input"));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  static DummyPipeline makePipeline(String name, String... inputs) {
    return new DummyPipeline(name, new HashSet<>(Arrays.asList(inputs)));
  }
}
