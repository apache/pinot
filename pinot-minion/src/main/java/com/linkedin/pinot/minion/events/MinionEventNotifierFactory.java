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
package com.linkedin.pinot.minion.events;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MinionEventNotifierFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(MinionEventNotifierFactory.class);
  public static final String METADATA_EVENT_CLASS_CONFIG = "factory.class";

  public abstract void init(Configuration configuration);

  public abstract MinionEventNotifier create();

  public static MinionEventNotifierFactory loadFactory(Configuration configuration) {
    MinionEventNotifierFactory metadataEventNotifierFactory;
    String metadataEventNotifierClassName = configuration.getString(METADATA_EVENT_CLASS_CONFIG);
    if (metadataEventNotifierClassName == null) {
      metadataEventNotifierClassName = DefaultMinionEventNotifierFactory.class.getName();
    }
    try {
      LOGGER.info("Instantiating metadata event notifier factory class {}", metadataEventNotifierClassName);
      metadataEventNotifierFactory =
          (MinionEventNotifierFactory) Class.forName(metadataEventNotifierClassName).newInstance();
      metadataEventNotifierFactory.init(configuration);
      return metadataEventNotifierFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
