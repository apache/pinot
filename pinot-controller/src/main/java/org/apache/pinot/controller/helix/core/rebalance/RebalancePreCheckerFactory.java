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
package org.apache.pinot.controller.helix.core.rebalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RebalancePreCheckerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RebalancePreCheckerFactory.class);

  private RebalancePreCheckerFactory() {
  }

  public static RebalancePreChecker create(String rebalancePreCheckerClassName) {
    try {
      LOGGER.info("Trying to create rebalance pre-checker object for class: {}", rebalancePreCheckerClassName);
      return (RebalancePreChecker) Class.forName(rebalancePreCheckerClassName).newInstance();
    } catch (Exception e) {
      String errMsg = String.format("Failed to create rebalance pre-checker for class: %s",
          rebalancePreCheckerClassName);
      LOGGER.error(errMsg, e);
      throw new RuntimeException(e);
    }
  }
}
