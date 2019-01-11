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

package org.apache.pinot.thirdeye.rootcause;

import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import java.util.Map;
import java.util.Set;


/**
 * Container object for the execution context (the state) of {@code Pipeline.run()}. Holds the search context
 * with user-specified entities as well as the (incremental) results from executing individual
 * pipelines.
 */
public class PipelineContext {
  private final Map<String, Set<Entity>> inputs;

  public PipelineContext(Map<String, Set<Entity>> inputs) {
    this.inputs = inputs;
  }

  /**
   * Returns a map of sets of entities that were generated as the output of upstream (input)
   * pipelines. The map is keyed by pipeline id.
   *
   * @return Map of input entities, keyed by generating pipeline id
   */
  public Map<String, Set<Entity>> getInputs() {
    return inputs;
  }

  /**
   * Flattens the inputs from different pipelines and filters them by (super) class {@code clazz}.
   * Returns a set of typed Entities or an empty set if no matching instances are found.  URN
   * conflicts are resolved by preserving the entity with the highest score.
   *
   * @param clazz (super) class to filter by
   * @param <T> (super) class of output collection
   * @return set of Entities in input context with given super class
   */
  public <T extends Entity> Set<T> filter(Class<? extends T> clazz) {
    Set<T> filtered = new MaxScoreSet<>();
    for(Set<Entity> entities : this.inputs.values()) {
      filtered.addAll(EntityUtils.filter(entities, clazz));
    }
    return filtered;
  }

}
