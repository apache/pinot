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
package org.apache.pinot.controller.api.events;

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MetadataEventNotifierFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(MetadataEventNotifierFactory.class);
  public static final String METADATA_EVENT_CLASS_CONFIG = "factory.class";

  public abstract void init(PinotConfiguration configuration);

  public abstract MetadataEventNotifier create();

  public static MetadataEventNotifierFactory loadFactory(PinotConfiguration configuration) {
    MetadataEventNotifierFactory metadataEventNotifierFactory;

    String metadataEventNotifierClassName =
        configuration.getProperty(METADATA_EVENT_CLASS_CONFIG, DefaultMetadataEventNotifierFactory.class.getName());

    try {
      LOGGER.info("Instantiating metadata event notifier factory class {}", metadataEventNotifierClassName);
      metadataEventNotifierFactory =
          (MetadataEventNotifierFactory) Class.forName(metadataEventNotifierClassName).newInstance();
      metadataEventNotifierFactory.init(configuration);
      return metadataEventNotifierFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
