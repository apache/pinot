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
package org.apache.pinot.common.failuredetector;

import javax.annotation.concurrent.ThreadSafe;


/// The `ConnectionFailureDetector` marks a server as unhealthy when a query response reports it as failed
/// (connection failure), or, with pings enabled, when a query to it times out and the server then fails to answer a
/// ping (see [FailureDetector#notifyServerNotResponded]). It retries the unhealthy servers with exponentially
/// increasing delays.
///
/// Only the single-stage Netty query path reports timeouts today; the gRPC and multi-stage paths report connection
/// failures only.
///
/// This class doesn't currently implement any additional logic over BaseExponentialBackoffRetryFailureDetector and is
/// retained for backward compatibility.
@ThreadSafe
public class ConnectionFailureDetector extends BaseExponentialBackoffRetryFailureDetector {
}
