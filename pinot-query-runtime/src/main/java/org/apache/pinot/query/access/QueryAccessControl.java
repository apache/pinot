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
package org.apache.pinot.query.access;

import io.grpc.Attributes;
import io.grpc.Metadata;


public interface QueryAccessControl {
  /**
   * Return whether the client can call the QueryServer or GrpcMailboxServer
   * This is similar to pinot-server AccessControl.isAuthorizedChannel but for multi-stage grpc calls
   * It is intended for inter-service authorization between pinot components rather than fine-grained
   * access control.
   *
   * @param attributes GRPC Attributes, potentially containing client certificates
   * @param metadata GRPC metadata, containing headers
   */
  boolean hasAccess(Attributes attributes, Metadata metadata);
}
