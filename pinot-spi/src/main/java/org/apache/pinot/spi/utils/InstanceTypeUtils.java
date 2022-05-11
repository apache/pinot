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
package org.apache.pinot.spi.utils;

import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;


public class InstanceTypeUtils {
  private InstanceTypeUtils() {
  }

  public static InstanceType getInstanceType(String instanceId) {
    if (instanceId.startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE)) {
      return InstanceType.CONTROLLER;
    }
    if (instanceId.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE)) {
      return InstanceType.BROKER;
    }
    if (instanceId.startsWith(Helix.PREFIX_OF_MINION_INSTANCE)) {
      return InstanceType.MINION;
    }
    // NOTE: Server instance id might not have the instance type prefix
    return InstanceType.SERVER;
  }

  public static boolean isController(String instanceId) {
    return instanceId.startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE);
  }

  public static boolean isBroker(String instanceId) {
    return instanceId.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE);
  }

  public static boolean isServer(String instanceId) {
    // NOTE: Server instance id might not have the instance type prefix
    if (instanceId.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)) {
      return true;
    }
    return !isController(instanceId) && !isBroker(instanceId) && !isMinion(instanceId);
  }

  public static boolean isMinion(String instanceId) {
    return instanceId.startsWith(Helix.PREFIX_OF_MINION_INSTANCE);
  }
}
