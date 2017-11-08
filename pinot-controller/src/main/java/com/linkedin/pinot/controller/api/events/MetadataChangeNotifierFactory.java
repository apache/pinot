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
package com.linkedin.pinot.controller.api.events;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MetadataChangeNotifierFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(MetadataChangeNotifierFactory.class);
  public static final String METADATA_CHANGE_CLASS_CONFIG = "factory.class";

  public abstract void init(Configuration configuration);

  public abstract MetadataChangeNotifier create();

  public static MetadataChangeNotifierFactory loadFactory(Configuration configuration) {
    MetadataChangeNotifierFactory metadataChangeNotifierFactory;
    String metadataChangeNotiferClassName = configuration.getString(METADATA_CHANGE_CLASS_CONFIG);
    if (metadataChangeNotiferClassName == null) {
      metadataChangeNotiferClassName = DefaultMetadataChangeNotifierFactory.class.getName();
    }
    try {
      LOGGER.info("Instantiating metadata change notifier factory class {}", metadataChangeNotiferClassName);
      metadataChangeNotifierFactory =  (MetadataChangeNotifierFactory) Class.forName(metadataChangeNotiferClassName).newInstance();
      LOGGER.info("Initializing metadata change notifier factory class {}", metadataChangeNotiferClassName);
      metadataChangeNotifierFactory.init(configuration);
      return metadataChangeNotifierFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
