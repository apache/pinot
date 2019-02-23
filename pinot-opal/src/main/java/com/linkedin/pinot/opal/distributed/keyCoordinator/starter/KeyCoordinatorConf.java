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
package com.linkedin.pinot.opal.distributed.keyCoordinator.starter;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;

public class KeyCoordinatorConf extends PropertiesConfiguration {

  private static final String CONFIG_STORE_FILE_PATH = "opal.distributed.keycoordinator.configstore.path";
  private static final String CONFIG_STORE_FILE_PATH_DEFAULT = "./config.store";

  public static final String FETCH_MSG_DELAY_MS = "kc.queue.fetch.delay.ms";
  public static final int FETCH_MSG_DELAY_MS_DEFAULT = 100;

  public static final String FETCH_MSG_MAX_DELAY_MS = "kc.queue.fetch.delay.max.ms";
  public static final int FETCH_MSG_MAX_DELAY_MS_DEFAULT = 10000;

  public static final String FETCH_MSG_MAX_BATCH_SIZE = "kc.queue.fetch.size";
  public static final int FETCH_MSG_MAX_BATCH_SIZE_DEFAULT = 1000;

  public static final String KEY_COORDINATOR_KV_STORE = "kvstore";
  public static final String KEY_COORDINATOR_TOPIC_MANAGER_ZK = "kafka.zookeeper";

  public static final String KEY_COORDINATOR_CONSUMER_CONF = "consumer";
  public static final String KEY_COORDINATOR_PRODUCER_CONF = "producer";
  public static final String KEY_COORDINATOR_KAFKA_CONF = "kafka.conf";
  public static final String KEY_COORDINATOR_TOPIC = "topic";
  public static final String KEY_COORDINATOR_PARTITIONS = "partitions";

  // server related config
  public static final String SERVER_CONFIG = "web.server";
  public static final String PORT = "jersey.port";
  public static final int PORT_DEFAULT = 8092;

  public KeyCoordinatorConf(File file) throws ConfigurationException {
    super(file);
  }

  public KeyCoordinatorConf() {
    super();
  }

  public String getConfigStorePathFile() {
    if (containsKey(CONFIG_STORE_FILE_PATH)) {
      return (String) getProperty(CONFIG_STORE_FILE_PATH);
    }
    return CONFIG_STORE_FILE_PATH_DEFAULT;
  }


}
