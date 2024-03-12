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

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to simplify access control validation. This class is simple wrapper around AccessControl class.
 */
public final class AccessControlUtils {
  private AccessControlUtils() {
    // left blank
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AccessControlUtils.class);

  /**
   * Validate permission for the given access type against the given table
   *
   * @param tableName name of the table to be accessed
   * @param accessType type of the access
   * @param httpHeaders HTTP headers containing requester identity required by access control object
   * @param endpointUrl the request url for which this access control is called
   * @param accessControl AccessControl object which does the actual validation
   */
  public static void validatePermission(@Nullable String tableName, AccessType accessType,
      @Nullable HttpHeaders httpHeaders, @Nullable String endpointUrl, AccessControl accessControl) {
    if (tableName != null) {
      tableName = DatabaseUtils.translateTableName(tableName, httpHeaders);
    }
    String userMessage = getUserMessage(tableName, accessType, endpointUrl);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    try {
      if (rawTableName == null) {
        if (accessControl.hasAccess(accessType, httpHeaders, endpointUrl)) {
          return;
        }
      } else {
        if (accessControl.hasAccess(rawTableName, accessType, httpHeaders, endpointUrl)) {
          return;
        }
      }
    } catch (WebApplicationException exception) {
      // throwing the exception if it's WebApplicationException
      throw exception;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Caught exception while validating permission for "
          + userMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    throw new ControllerApplicationException(LOGGER, "Permission is denied for " + userMessage,
        Response.Status.FORBIDDEN);
  }

  private static String getUserMessage(String tableName, AccessType accessType, String endpointUrl) {
    if (StringUtils.isBlank(tableName)) {
      return String.format("%s '%s'", accessType, endpointUrl);
    }
    return String.format("%s '%s' for table '%s'", accessType, endpointUrl, tableName);
  }
}
