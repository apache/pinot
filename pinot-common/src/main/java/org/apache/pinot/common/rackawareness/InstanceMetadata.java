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
 * This class represents an instance for instance metadata.
 */
public class InstanceMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceMetadata.class);

  private final String _failureDomain;

  InstanceMetadata(String failureDomain) {
    this._failureDomain = Preconditions.checkNotNull(failureDomain, "failureDomain cannot be null.");
  }

  public String getFailureDomain() {
    return _failureDomain;
  }
}
