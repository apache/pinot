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
package org.apache.pinot.server.access;

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.NotAuthorizedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Abstract class for basic authentication access control implementations of the server.
 */
abstract class AbstractBasicAuthAccessControl implements AccessControl {
  protected static final String HEADER_AUTHORIZATION = "authorization";
  protected static final String BASIC_AUTH = "Basic";

  /**
   * Returns whether the client has permission to access the endpoints which are not table level for the given access
   * type.
   * - If the requester identity is not authorized, throws a NotAuthorizedException.
   * - If the table name is not present, returns success.
   * - Else, returns success if the principal has access to the table.
   *
   * @param requesterIdentity Request identity
   * @param tableName Name of the table to be accessed
   */
  @Override
  public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
    // Get the principal for the first token
    Optional<? extends BasicAuthPrincipal> principalOpt = getPrincipal(requesterIdentity);
    if (!principalOpt.isPresent()) {
      throw new NotAuthorizedException(BASIC_AUTH);
    }

    // Return success, if tables is null or empty
    if (StringUtils.isEmpty(tableName)) {
      return true;
    }

    // If table name is present, check if the principal has access to it
    return principalOpt.get().hasTable(TableNameBuilder.extractRawTableName(tableName));
  }

  @Override
  public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
    return true;
  }

  /**
   * Return the tokens from the given requester identity.
   * @param requesterIdentity identity of the requester
   * @throws UnsupportedOperationException if the requester identity is not an instance of GrpcRequesterIdentity or
   * HttpRequesterIdentity
   */
  protected List<String> getTokens(RequesterIdentity requesterIdentity) {
    if (requesterIdentity instanceof GrpcRequesterIdentity) {
      GrpcRequesterIdentity identity = (GrpcRequesterIdentity) requesterIdentity;
      return new ArrayList<>(identity.getGrpcMetadata().get(HEADER_AUTHORIZATION));
    }
    if (requesterIdentity instanceof HttpRequesterIdentity) {
      HttpRequesterIdentity identity = (HttpRequesterIdentity) requesterIdentity;
      return new ArrayList<>(identity.getHttpHeaders().get(HEADER_AUTHORIZATION));
    }
    throw new UnsupportedOperationException("GrpcRequesterIdentity or HttpRequesterIdentity is required");
  }

  /**
   * Return the principal for the given requester identity.
   * @param requesterIdentity identity of the requester
   */
  protected abstract Optional<? extends BasicAuthPrincipal> getPrincipal(RequesterIdentity requesterIdentity);
}
