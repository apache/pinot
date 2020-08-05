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
package org.apache.pinot.broker.broker;

import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AccessControlFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(AccessControlFactory.class);
  public static final String ACCESS_CONTROL_CLASS_CONFIG = "class";

  public abstract void init(PinotConfiguration confguration);

  public abstract AccessControl create();

  public static AccessControlFactory loadFactory(PinotConfiguration configuration) {
    AccessControlFactory accessControlFactory;
    String accessControlFactoryClassName = configuration.getProperty(ACCESS_CONTROL_CLASS_CONFIG);
    if (accessControlFactoryClassName == null) {
      accessControlFactoryClassName = AllowAllAccessControlFactory.class.getName();
    }
    try {
      LOGGER.info("Instantiating Access control factory class {}", accessControlFactoryClassName);
      accessControlFactory = (AccessControlFactory) Class.forName(accessControlFactoryClassName).newInstance();
      LOGGER.info("Initializing Access control factory class {}", accessControlFactoryClassName);
      accessControlFactory.init(configuration);
      return accessControlFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
