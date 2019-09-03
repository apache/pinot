/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.rootcause.impl;

import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.MaxScoreSet;
import org.apache.pinot.thirdeye.rootcause.Pipeline;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an aggregator that handles the same entity being returned from multiple
 * pipelines by selecting the entity with the highest score. It optionally truncates the
 * number of returned entities to the top k by score.
 */
public class MaxAggregationPipeline extends Pipeline {
  private static Logger LOG = LoggerFactory.getLogger(MaxAggregationPipeline.class);

  private final static String PROP_K = "k";
  private final static int PROP_K_DEFAULT = -1;

  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param k top k truncation before aggregation ({@code -1} for unbounded)
   */
  public MaxAggregationPipeline(String outputName, Set<String> inputNames, int k) {
    super(outputName, inputNames);
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K})
   */
  public MaxAggregationPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.k = MapUtils.getIntValue(properties, PROP_K, PROP_K_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, EntityUtils.topk(new MaxScoreSet<>(context.filter(Entity.class)), this.k));
  }
}
