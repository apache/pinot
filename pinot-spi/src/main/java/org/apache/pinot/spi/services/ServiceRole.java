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
package org.apache.pinot.spi.services;

/**
 * ServiceRole defines a role that Pinot Service could start/stop.
 */
public enum ServiceRole {
  CONTROLLER, BROKER, SERVER, MINION,
  /**
   * "Add Table" is a task, not a service, but the entrypoint {@code ServiceManager} can only manage
   * {@link ServiceStartable}. When finalized "Add Table", may become a class of {@link #MINION}, or
   * {@code ServiceManager} will be able to invoke tasks. Regardless, to add a table will need
   * access to a Helix resource manager.
   */
  ADD_TABLE;
}
