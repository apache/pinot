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
package org.apache.pinot.spi.ingestion.error;

/// The verdict an ingestion source (a stream, its schema registry, a file system) gave on a request Pinot made to it,
/// tested by what change unblocks ingestion.
///
/// A failure that is not the source's verdict on our request has no category at all: Pinot's own clock or ambient
/// identity, local disk or config, content that was read but can't be parsed, or cancellation.
public enum IngestionSourceErrorCategory {
  /// "I won't let you in": the source rejected our identity or denied our permission, including an access policy it
  /// enforces and a deliberate "does not exist or not authorized" answer. Someone must change a credential, a grant
  /// or a policy.
  AUTH,
  /// "You're in, but I can't serve this", or the source never answered: the data is missing or expired, the source
  /// keeps throttling or failing after our retries, or DNS, connect, TLS or a timeout stopped the request. Someone
  /// must make the data exist or the endpoint reachable.
  SOURCE_READ
}
