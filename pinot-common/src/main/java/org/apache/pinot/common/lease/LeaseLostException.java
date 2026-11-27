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
package org.apache.pinot.common.lease;

/// Thrown by [PinotOperationLease#checkOwnership()] when the lease was released or its ZooKeeper session ended.
public class LeaseLostException extends Exception {

  public LeaseLostException(String path, String owner, long sessionId) {
    super("Lost operation lease: " + path + " held by: " + owner + " with session: 0x" + Long.toHexString(sessionId));
  }
}
