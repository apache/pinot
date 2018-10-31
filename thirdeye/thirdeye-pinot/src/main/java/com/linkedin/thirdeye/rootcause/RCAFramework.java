/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Container class for configuring and executing a root cause search with multiple pipelines.
 * The framework is instantiated with multiple (named) pipelines and a result aggregator. The run()
 * method then executes the configured pipelines for arbitrary inputs without
 * maintaining any additional state within the RCAFramework.
 *
 * RCAFramework supports parallel DAG execution and requires pipelines to form a valid path
 * from {@code INPUT} to {@code OUTPUT}. The execution order of pipelines is guaranteed to be
 * compatible with serial execution in one single thread.
 */

/*
 *                   /-> pipeline.run() --> pipeline.run() \
 *                  /                                       \
 * INPUT --> run() ---> pipeline.run() ---> pipeline.run() --> OUTPUT
 *                  \                    /
 *                   \-> pipeline.run() /
 */
public class RCAFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RCAFramework.class);

  public static final String INPUT = "INPUT";
  public static final String OUTPUT = "OUTPUT";
  public static final long TIMEOUT = 600000;

  private final Map<String, Pipeline> pipelines;
  private final ExecutorService executor;

  public RCAFramework(Collection<Pipeline> pipelines, ExecutorService executor) {
    this.executor = executor;

    if(!isValidDAG(pipelines))
      throw new IllegalArgumentException(String.format("Invalid DAG. '%s' not reachable output name '%s'", OUTPUT, INPUT));

    this.pipelines = new HashMap<>();
    for(Pipeline p : pipelines) {
      if(INPUT.equals(p.getOutputName()))
        throw new IllegalArgumentException(String.format("Must not contain a pipeline with output name '%s'", INPUT));
      if(this.pipelines.containsKey(p.getOutputName()))
        throw new IllegalArgumentException(String.format("Already contains pipeline with output name '%s'", p.getOutputName()));
      this.pipelines.put(p.getOutputName(), p);
    }

    if(!this.pipelines.containsKey(OUTPUT))
      throw new IllegalArgumentException(String.format("Must contain a pipeline with output name '%s'", OUTPUT));
  }

  /**
   * Performs rootcause search for a user-specified set of input entities.
   * Fans out entities to individual pipelines, collects results, and aggregates them.
   *
   * @param input user-specified search entities
   * @return aggregated results
   */
  public RCAFrameworkExecutionResult run(Set<Entity> input) throws Exception {
    long tStart = System.nanoTime();
    try {
      Map<String, Pipeline> pipelines = new HashMap<>(this.pipelines);
      pipelines.put(INPUT, new StaticPipeline(INPUT, Collections.<String>emptySet(), input));

      LOG.info("Constructing flow for input '{}'", input);
      Map<String, Future<PipelineResult>> flow = constructDAG(pipelines);

      Map<String, PipelineResult> results = new HashMap<>();
      for (Map.Entry<String, Future<PipelineResult>> e : flow.entrySet()) {
        PipelineResult r = e.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Results for pipeline '{}':", e.getKey());
          logResultDetails(r);
        }
        results.put(e.getKey(), r);
      }

      return new RCAFrameworkExecutionResult(results.get(OUTPUT).getEntities(), results);

    } catch (Exception e) {
      ThirdeyeMetricsUtil.rcaFrameworkExceptionCounter.inc();
      throw e;

    } finally {
      ThirdeyeMetricsUtil.rcaFrameworkCallCounter.inc();
      ThirdeyeMetricsUtil.rcaFrameworkDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  static void logResultDetails(PipelineResult result) {
    List<Entity> entities = new ArrayList<>(result.getEntities());
    Collections.sort(entities, new Comparator<Entity>() {
      @Override
      public int compare(Entity o1, Entity o2) {
        return -Double.compare(o1.getScore(), o2.getScore());
      }
    });

    for(Entity e : entities) {
      LOG.debug("{} [{}] {}", Math.round(e.getScore() * 1000) / 1000.0, e.getClass().getSimpleName(), e.getUrn());
    }
  }

  static boolean isValidDAG(Collection<Pipeline> pipelines) {
    Set<String> visited = new HashSet<>();
    visited.add(INPUT);

    int prevSize = 0;
    while(prevSize < visited.size()) {
      prevSize = visited.size();
      for (Pipeline p : pipelines) {
        if (visited.containsAll(p.getInputNames()))
          visited.add(p.getOutputName());
      }
    }

    return visited.contains(OUTPUT);
  }

  Map<String, Future<PipelineResult>> constructDAG(Map<String, Pipeline> pipelines) {
    // TODO purge pipelines not on critical path
    Map<String, Future<PipelineResult>> tasks = new LinkedHashMap<>();
    Pipeline input = pipelines.get(INPUT);
    PipelineCallable inputCallable = new PipelineCallable(Collections.<String, Future<PipelineResult>>emptyMap(), input);
    tasks.put(INPUT, this.executor.submit(inputCallable));

    int prevSize = 0;
    while(prevSize < tasks.size()) {
      prevSize = tasks.size();
      for(Pipeline p : pipelines.values()) {
        if(!tasks.containsKey(p.getOutputName()) && tasks.keySet().containsAll(p.getInputNames())) {
          Map<String, Future<PipelineResult>> dependencies = new HashMap<>();
          for(String inputName : p.getInputNames()) {
            dependencies.put(inputName, tasks.get(inputName));
          }

          PipelineCallable c = new PipelineCallable(dependencies, p);
          tasks.put(p.getOutputName(), this.executor.submit(c));
        }
      }
    }

    return tasks;
  }
}
