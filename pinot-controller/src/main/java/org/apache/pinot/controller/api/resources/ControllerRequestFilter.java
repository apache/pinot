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
package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
@PreMatching
public class ControllerRequestFilter implements ContainerRequestFilter {

  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerRequestFilter.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // uses the database name from header to build the actual table name
    DatabaseUtils.translateTableNameQueryParam(requestContext, _pinotHelixResourceManager.getTableCache());
  }
}
