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

import java.util.Optional;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.resources.ControllerApplicationException;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to simplify access control validation. This class is simple wrapper around AccessControl class.
 */
public class AccessControlUtils {
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
  public void validatePermission(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl,
      AccessControl accessControl) {
    validatePermission(Optional.of(tableName), accessType, httpHeaders, endpointUrl, accessControl);
  }

  /**
   * Validate permission for the given access type for a non-table level endpoint
   *
   * @param accessType type of the access
   * @param httpHeaders HTTP headers containing requester identity required by access control object
   * @param endpointUrl the request url for which this access control is called
   * @param accessControl AccessControl object which does the actual validation
   */
  public void validatePermission(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl,
      AccessControl accessControl) {
    validatePermission(Optional.empty(), accessType, httpHeaders, endpointUrl, accessControl);
  }

  /**
   * Validate permission for the given access type against the given table
   *
   * @param tableNameOpt name of the table to be accessed; if `none`, it's a non-table level endpoint.
   * @param accessType type of the access
   * @param httpHeaders HTTP headers containing requester identity required by access control object
   * @param endpointUrl the request url for which this access control is called
   * @param accessControl AccessControl object which does the actual validation
   */
  public void validatePermission(Optional<String> tableNameOpt, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl, AccessControl accessControl) {
    boolean hasPermission;
    String accessTypeToEndpointMsg = String.format("access type '%s' to the endpoint '%s'", accessType, endpointUrl)
        + tableNameOpt.map(name -> String.format(" for table '%s'", name)).orElse("");
    try {
      if (tableNameOpt.isPresent()) {
        String rawTableName = TableNameBuilder.extractRawTableName(tableNameOpt.get());
        hasPermission = accessControl.hasAccess(rawTableName, accessType, httpHeaders, endpointUrl);
      } else {
        hasPermission = accessControl.hasAccess(accessType, httpHeaders, endpointUrl);
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Caught exception while validating permission for " + accessTypeToEndpointMsg,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    if (!hasPermission) {
      throw new ControllerApplicationException(LOGGER, "Permission is denied for " + accessTypeToEndpointMsg,
          Response.Status.FORBIDDEN);
    }
  }
}
