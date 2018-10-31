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

import java.util.Set;


/**
 * Interface for a named stateless pipeline as injected into {@code RCAFramework}. Holds the business logic for
 * associating search context entities with other relevant entities. Also performs relative ranking
 * of associated entities in terms of importance to the user.
 *
 * @see RCAFramework
 */
public abstract class Pipeline {
  private final String outputName;
  private final Set<String> inputNames;

  public Pipeline(String outputName, Set<String> inputNames) {
    this.outputName = outputName;
    this.inputNames = inputNames;
  }

  public final String getOutputName() {
    return outputName;
  }

  public final Set<String> getInputNames() {
    return inputNames;
  }

  /**
   * Executes the pipeline given the execution context set up by the RCAFramework. Returns entities
   * as determined relevant given the user-specified search context (contained in the execution
   * context).
   *
   * @param context pipeline execution context
   * @return pipeline results
   */
  public abstract PipelineResult run(PipelineContext context);

}