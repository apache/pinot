package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Internal class for DAG execution in RCAFramework
 */
class PipelineCallable implements Callable<PipelineResult> {
  private final static Logger LOG = LoggerFactory.getLogger(PipelineCallable.class);

  public static final long TIMEOUT = RCAFramework.TIMEOUT;

  private final Map<String, Future<PipelineResult>> dependencies;
  private final Pipeline pipeline;

  public PipelineCallable(Map<String, Future<PipelineResult>> dependencies, Pipeline pipeline) {
    this.dependencies = dependencies;
    this.pipeline = pipeline;
  }

  @Override
  public PipelineResult call() throws Exception {
    LOG.info("Preparing pipeline '{}'. Waiting for inputs '{}'", this.pipeline.getOutputName(), this.dependencies.keySet());
    Map<String, Set<Entity>> inputs = new HashMap<>();
    for(Map.Entry<String, Future<PipelineResult>> e : this.dependencies.entrySet()) {
      PipelineResult r = e.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS);
      inputs.put(e.getKey(), r.getEntities());
    }

    long tStart = System.nanoTime();
    try {
      LOG.info("Executing pipeline '{}'", this.pipeline.getOutputName());
      PipelineContext context = new PipelineContext(inputs);
      PipelineResult result = this.pipeline.run(context);

      long runtime = (System.nanoTime() - tStart) / 1000000;
      LOG.info("Completed pipeline '{}' in {}ms. Got {} results", this.pipeline.getOutputName(), runtime, result.getEntities().size());
      return result;
    } finally {
      ThirdeyeMetricsUtil.rcaPipelineCallCounter.inc();
      ThirdeyeMetricsUtil.rcaPipelineDurationCounter.inc(System.nanoTime() - tStart);
    }
  }
}
