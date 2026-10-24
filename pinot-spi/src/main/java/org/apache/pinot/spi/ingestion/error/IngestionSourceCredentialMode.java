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

/// Whose credential a client used to reach an ingestion source.
///
/// The same credential error can be the table owner's or Pinot's own: an AWS `UnrecognizedClientException` rejects
/// the table's static keys under [#STATIC], but the session credentials the process minted for itself under
/// [#AMBIENT]. Credential rejections count as [IngestionSourceErrorCategory#AUTH] only when the rejected credential
/// was configured on the table.
public enum IngestionSourceCredentialMode {
  /// A credential configured on the table: keys, a password, a token, a service-account key.
  STATIC,
  /// The process's own identity: the default provider chain, a node or pod role, application default credentials.
  AMBIENT,
  /// No credential at all.
  ANONYMOUS
}
