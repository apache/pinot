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
import org.apache.pinot.common.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents properties needed to fetch instance metadata for Azure
 */
public class AzureInstanceMetadataFetcherProperties {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzureInstanceMetadataFetcherProperties.class);

  private final int _maxRetry;
  private final int _connectionTimeOut;
  private final int _requestTimeOut;

  private static final AzureInstanceMetadataFetcherProperties DEFAULT_PROPERTIES =
      new AzureInstanceMetadataFetcherProperties(CommonConstants.Helix.RACK_AWARENESS_CONNECTION_MAX_RETRY_DEFAULT_VALUE,
          CommonConstants.Helix.RACK_AWARENESS_CONNECTION_CONNECTION_TIME_OUT_DEFAULT_VALUE,
          CommonConstants.Helix.RACK_AWARENESS_CONNECTION_REQUEST_TIME_OUT_DEFAULT_VALUE);

  public AzureInstanceMetadataFetcherProperties(int maxRetry, int connectionTimeOut, int requestTimeOut) {
    _maxRetry = maxRetry;
    _connectionTimeOut = connectionTimeOut;
    _requestTimeOut = requestTimeOut;
  }

  public AzureInstanceMetadataFetcherProperties(
      AzureInstanceMetadataFetcherProperties azureInstanceMetadataFetcherProperties) {
    Preconditions.checkNotNull(azureInstanceMetadataFetcherProperties, "azureInstanceMetadataFetcherProperties cannot be null.");

    _maxRetry = azureInstanceMetadataFetcherProperties._maxRetry;
    _connectionTimeOut = azureInstanceMetadataFetcherProperties._connectionTimeOut;
    _requestTimeOut = azureInstanceMetadataFetcherProperties._requestTimeOut;
  }

  public static AzureInstanceMetadataFetcherProperties getDefaultProperties() {
    return DEFAULT_PROPERTIES;
  }

  public int getMaxRetry() {
    return _maxRetry;
  }

  public int getConnectionTimeOut() {
    return _connectionTimeOut;
  }

  public int getRequestTimeOut() {
    return _requestTimeOut;
  }
}
