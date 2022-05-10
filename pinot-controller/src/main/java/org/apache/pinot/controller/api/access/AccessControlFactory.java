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
package org.apache.pinot.controller.api.access;

import java.io.IOException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.env.PinotConfiguration;


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControlFactory {
  default void init(PinotConfiguration pinotConfiguration) {
    // left blank
  }
  /**
   * Extend the original init method to support Zookeeper BasicAuthAccessControlFactory.
   * Because ZKBasicAuthAccessControlFactory needs to acquire users' info from HelixPropertyStore.
   * The reason why passed helixResourceManager param other than HelixPropertyStore is that
   * we need to init UserACLConfig via helixResourceManager.
   *
   * @param configuration pinot configuration
   * @param helixResourceManager Helix resource manager
   */
  default void init(PinotConfiguration configuration, PinotHelixResourceManager helixResourceManager)
    throws IOException {
    init(configuration);
  }

  AccessControl create();
}
