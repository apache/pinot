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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents properties needed to fetch instance metadata
 */
public class InstanceMetadataFetcherProperties {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceMetadataFetcherProperties.class);

  private final int _maxRetry;
  private final int _connectionTimeOut;
  private final int _requestTimeOut;

  public InstanceMetadataFetcherProperties(int maxRetry, int connectionTimeOut, int requestTimeOut) {
    _maxRetry = maxRetry;
    _connectionTimeOut = connectionTimeOut;
    _requestTimeOut = requestTimeOut;
  }

  public InstanceMetadataFetcherProperties(InstanceMetadataFetcherProperties instanceMetadataFetcherProperties) {
    Preconditions.checkNotNull(instanceMetadataFetcherProperties, "instanceMetadataFetcherProperties cannot be null.");

    _maxRetry = instanceMetadataFetcherProperties._maxRetry;
    _connectionTimeOut = instanceMetadataFetcherProperties._connectionTimeOut;
    _requestTimeOut = instanceMetadataFetcherProperties._requestTimeOut;
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
