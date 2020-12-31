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
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class H3IndexColumn extends BaseJsonConfig {
  // the column to build h3 index
  private final String _name;
  // the index resolutions
  private final List<Integer> _resolutions;

  @JsonCreator
  public H3IndexColumn(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "resolutions", required = true) List<Integer> resolutions) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(resolutions), "'resolutions must be configured'");
    _name = name;
    _resolutions = resolutions;
  }

  public List<Integer> getResolutions() {
    return _resolutions;
  }

  public String getName() {
    return _name;
  }
}
