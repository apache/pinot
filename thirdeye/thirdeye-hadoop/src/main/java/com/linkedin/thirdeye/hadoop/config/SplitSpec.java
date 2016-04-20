/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Class for representing split spec
 * @param threshold - threshold after which to stop splitting on a node in star tree
 * @param order - order in which dimensions should be chosen to split in star tree creation
 */
public class SplitSpec {
  private int threshold = 1000;
  private List<String> order;

  public SplitSpec() {
  }

  public SplitSpec(int threshold, List<String> order) {
    this.threshold = threshold;
    this.order = order;
  }

  @JsonProperty
  public int getThreshold() {
    return threshold;
  }

  @JsonProperty
  public List<String> getOrder() {
    return order;
  }
}
