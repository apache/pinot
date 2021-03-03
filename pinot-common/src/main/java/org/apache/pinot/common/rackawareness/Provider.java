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
package org.apache.pinot.common.rackawareness;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Enum Provider specifies known rack awareness metadata provider; e.g., Microsoft Azure, Google Cloud Platform (GCP), Amazon Web Service (AWS)
 */
public enum Provider {
  AZURE("azure", "org.apache.pinot.common.rackawareness.AzureInstanceMetadataFetcher");

  private final String zNodeName;

  private final String processorClassName;

  private static final Map<String, Provider> stringToProvider =
      Stream.of(values()).collect(Collectors.toMap(Provider::toString, e -> e));

  /**
   *
   * @param zNodeName zNode under /PROPERTYSTORE/CONFIGS/CLUSTER/rackAwareness to store provider specific configs
   * @param processorClassName name of the class for instance metadata fetcher
   */
  Provider(String zNodeName, String processorClassName) {
    this.zNodeName = zNodeName;
    this.processorClassName = processorClassName;
  }

  /**
   * Returns a Provider from a string which is case insensitive
   * @param providerStr a string to be used for a Provider enum conversion
   * @return a Provider enum converted from case an insensitive string
   */
  public static Optional<Provider> fromString(String providerStr) {
    Preconditions.checkNotNull(providerStr, "providerStr cannot be null.");
    return Optional.ofNullable(stringToProvider.get(providerStr.toLowerCase()));
  }

  public String getzNodeName() {
    return zNodeName;
  }

  public String getProcessorClassName() {
    return processorClassName;
  }

  @Override
  public String toString() {
    return name().toLowerCase();
  }
}
