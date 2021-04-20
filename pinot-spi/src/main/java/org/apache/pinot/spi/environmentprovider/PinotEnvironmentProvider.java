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
package org.apache.pinot.spi.environmentprovider;

import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;

/**
 *  Environment Provider interface implemented by different cloud
 *  providers to customize the base pinot configuration to add environment
 *  variables & instance specific configuration
 */
public interface PinotEnvironmentProvider {

  String INSTANCE_FAILURE_DOMAIN = "pinot.environment.instance.failureDomain";

  /**
   * Customize base pinot configuration to add environment variable & instance specific configuration
   * @param baseConfiguration Base configuration provided by the invoking service
   * @return Custom Pinot Configuration
   */
  PinotConfiguration getEnvironment(Map<String, Object> baseConfiguration);

  /**
   * Initializes the configurations specific to an environment provider.
   */
  void init(PinotConfiguration pinotConfiguration);

}
