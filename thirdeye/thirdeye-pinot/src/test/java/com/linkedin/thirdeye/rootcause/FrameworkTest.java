package com.linkedin.thirdeye.rootcause;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import org.testng.annotations.Test;


public class FrameworkTest {
  private static final String INPUT = RCAFramework.INPUT;
  private static final String OUTPUT = RCAFramework.OUTPUT;

  static class DummyPipeline extends Pipeline {
    public DummyPipeline(String name, Set<String> inputs) {
      super(name, inputs);
    }

    @Override
    public PipelineResult run(PipelineContext context) {
      return new PipelineResult(context, Collections.<Entity>emptySet());
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDAGInputPipeline() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline(INPUT));
    pipelines.add(makePipeline(OUTPUT, INPUT));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDAGNoOutput() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("a", INPUT));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDAGNoPath() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("a", INPUT));
    pipelines.add(makePipeline(OUTPUT, "a", "b"));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  @Test
  public void testValidDAG() {
    Collection<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(makePipeline("a", INPUT));
    pipelines.add(makePipeline("b", INPUT, "a"));
    pipelines.add(makePipeline(OUTPUT, "a", "b", INPUT));
    RCAFramework f = new RCAFramework(pipelines, Executors.newSingleThreadExecutor());
  }

  static DummyPipeline makePipeline(String name, String... inputs) {
    return new DummyPipeline(name, new HashSet<>(Arrays.asList(inputs)));
  }
}
