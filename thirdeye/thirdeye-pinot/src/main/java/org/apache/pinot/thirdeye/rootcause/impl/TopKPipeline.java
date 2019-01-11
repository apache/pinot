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
import org.apache.pinot.thirdeye.rootcause.Pipeline;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import java.util.Map;
import java.util.Set;


/**
 * TopKPipeline is a generic pipeline implementation for ordering, filtering, and truncating incoming
 * Entities. The pipeline first filters incoming Entities based on their {@code class} and then
 * orders them based on score from highest to lowest. It finally truncates the result to at most
 * {@code k} elements and emits the result.
 */
public class TopKPipeline extends Pipeline {
  public static final String PROP_K = "k";
  public static final String PROP_CLASS = "class";

  public static final String PROP_CLASS_DEFAULT = Entity.class.getName();

  private final int k;
  private final Class<? extends Entity> clazz;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param clazz (super) class to filter by
   * @param k maximum number of result elements
   */
  public TopKPipeline(String outputName, Set<String> inputNames, Class<? extends Entity> clazz, int k) {
    super(outputName, inputNames);
    this.k = k;
    this.clazz = clazz;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_CLASS})
   */
  @SuppressWarnings("unchecked")
  public TopKPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) throws Exception {
    super(outputName, inputNames);

    if(!properties.containsKey(PROP_K))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found", PROP_K));
    this.k = Integer.parseInt(properties.get(PROP_K).toString());

    String classProp = PROP_CLASS_DEFAULT;
    if(properties.containsKey(PROP_CLASS))
      classProp = properties.get(PROP_CLASS).toString();
    this.clazz = (Class<? extends Entity>) Class.forName(classProp);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
   return new PipelineResult(context, EntityUtils.topk(context.filter(this.clazz), this.k));
  }
}
