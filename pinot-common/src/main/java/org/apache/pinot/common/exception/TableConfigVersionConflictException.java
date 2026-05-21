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
package org.apache.pinot.common.exception;

/// Thrown by table-config update paths when the version-checked CAS write to ZooKeeper fails because a
/// concurrent writer updated the znode between the pre-read (used for diffing / deprecation validation) and the
/// subsequent write. Callers SHOULD surface this as HTTP 409 CONFLICT and instruct the client to re-read and
/// retry — the failure is recoverable and is NOT the same as a transient ZK availability error.
public class TableConfigVersionConflictException extends Exception {

  public TableConfigVersionConflictException(String message) {
    super(message);
  }
}
