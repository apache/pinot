/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.broker.broker;

import com.linkedin.pinot.broker.api.AccessControl;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AccessControlFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(AccessControlFactory.class);
  public static final String ACCESS_CONTROL_CLASS_CONFIG = "class";

  public abstract  void init(Configuration confguration);

  public abstract  AccessControl create();

  public static AccessControlFactory loadFactory(Configuration configuration) {
    AccessControlFactory accessControlFactory;
    String accessControlFactoryClassName = configuration.getString(ACCESS_CONTROL_CLASS_CONFIG);
    if (accessControlFactoryClassName == null) {
      accessControlFactoryClassName = AllowAllAccessControlFactory.class.getName();
    }
    try {
      LOGGER.info("Instantiating Access control factory class {}", accessControlFactoryClassName);
      accessControlFactory =  (AccessControlFactory) Class.forName(accessControlFactoryClassName).newInstance();
      LOGGER.info("Initializing Access control factory class {}", accessControlFactoryClassName);
      accessControlFactory.init(configuration);
      return accessControlFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
