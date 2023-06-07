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
import org.apache.pinot.spi.config.BaseJsonConfig;

public class DedupConfig extends BaseJsonConfig {
  private final boolean _dedupEnabled;
  private final HashFunction _hashFunction;
  private final String _metadataManagerClass;

  public DedupConfig(@JsonProperty(value = "dedupEnabled", required = true) boolean dedupEnabled,
      @JsonProperty(value = "hashFunction") HashFunction hashFunction) {
    this(dedupEnabled, hashFunction, null);
  }
  @JsonCreator
  public DedupConfig(@JsonProperty(value = "dedupEnabled", required = true) boolean dedupEnabled,
      @JsonProperty(value = "hashFunction") HashFunction hashFunction,
      @JsonProperty(value = "metadataManagerClass") String metadataManagerClass
  ) {
    _dedupEnabled = dedupEnabled;
    _hashFunction = hashFunction == null ? HashFunction.NONE : hashFunction;
    _metadataManagerClass = metadataManagerClass;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public boolean isDedupEnabled() {
    return _dedupEnabled;
  }

  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }
}
