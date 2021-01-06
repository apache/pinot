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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.resources.ControllerApplicationException;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;


public class AccessControlUtils {

  public static void validateWritePermission(HttpHeaders httpHeaders, String tableName,
      AccessControlFactory accessControlFactory, Logger logger) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    boolean hasWritePermission;
    try {
      AccessControl accessControl = accessControlFactory.create();
      hasWritePermission = accessControl.hasWritePermission(httpHeaders, rawTableName);
    } catch (Exception e) {
      throw new ControllerApplicationException(logger,
          "Caught exception while validating write permission to table: " + rawTableName,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    if (!hasWritePermission) {
      throw new ControllerApplicationException(logger, "No write permission to table: " + rawTableName,
          Response.Status.FORBIDDEN);
    }
  }

  public static void validateWritePermission(HttpHeaders httpHeaders, AccessControlFactory accessControlFactory,
      Logger logger) {
    boolean hasWritePermission;
    try {
      AccessControl accessControl = accessControlFactory.create();
      hasWritePermission = accessControl.hasWritePermission(httpHeaders);
    } catch (Exception e) {
      throw new ControllerApplicationException(logger,
          "Caught exception while validating write permission to the endpoint", Response.Status.INTERNAL_SERVER_ERROR,
          e);
    }
    if (!hasWritePermission) {
      throw new ControllerApplicationException(logger, "No write permission to the endpoint",
          Response.Status.FORBIDDEN);
    }
  }
}