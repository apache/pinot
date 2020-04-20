/**
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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Class representing configurations related to realtime segment completion.
 */
public class CompletionConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Mode to use when completing segment. DEFAULT for default strategy (build segment if segment is equivalent to the committed segment, else download). DOWNLOAD for always download the segment, never build.")
  private final String _completionMode;

  @JsonCreator
  public CompletionConfig(@JsonProperty(value = "completionMode", required = true) String completionMode) {
    Preconditions.checkArgument(completionMode != null, "'completionMode' must be configured");
    _completionMode = completionMode;
  }

  public String getCompletionMode() {
    return _completionMode;
  }
}
