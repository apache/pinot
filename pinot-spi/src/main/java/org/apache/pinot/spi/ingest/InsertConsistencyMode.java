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
package org.apache.pinot.spi.ingest;

/**
 * Controls how long the broker waits before returning a response for an INSERT INTO statement.
 *
 * <ul>
 *   <li>{@link #FIRE_AND_FORGET} — return immediately after the request is enqueued.</li>
 *   <li>{@link #WAIT_FOR_ACCEPT} — return after the coordinator has accepted the statement.</li>
 *   <li>{@link #WAIT_FOR_VISIBLE} — return only after the inserted data is queryable.</li>
 * </ul>
 *
 * <p>This enum is thread-safe (immutable).
 */
public enum InsertConsistencyMode {
  FIRE_AND_FORGET,
  WAIT_FOR_ACCEPT,
  WAIT_FOR_VISIBLE
}
