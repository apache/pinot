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

import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * StaticPipeline emits a fixed set of entities as a result, regardless of the input. It is
 * used to encapsulate constants (such as user input) during framework execution.
 */
public class StaticPipeline extends Pipeline {
  private static final String PROP_ENTITIES = "entities";
  private static final String PROP_SCORES = "scores";

  private final Set<Entity> entities;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param entities entities to emit as result
   */
  public StaticPipeline(String outputName, Set<String> inputNames, Set<Entity> entities) {
    super(outputName, inputNames);
    this.entities = entities;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_ENTITIES}, {@code PROP_SCORES})
   */
  @SuppressWarnings("unchecked")
  public StaticPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);

    if(!properties.containsKey(PROP_ENTITIES))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found", PROP_ENTITIES));

    this.entities = new HashSet<>();

    if (properties.get(PROP_ENTITIES) instanceof Map) {
      // with scores
      Map<String, Double> entities = (Map<String, Double>) properties.get(PROP_ENTITIES);
      for (Map.Entry<String, Double> entry : entities.entrySet()) {
        this.entities.add(EntityUtils.parseURN(entry.getKey(), entry.getValue()));
      }

    } else if (properties.get(PROP_ENTITIES) instanceof List) {
      // without scores
      List<String> urns = (List<String>) properties.get(PROP_ENTITIES);
      for (String urn : urns) {
        this.entities.add(EntityUtils.parseURN(urn, 1.0));
      }
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, this.entities);
  }

}
