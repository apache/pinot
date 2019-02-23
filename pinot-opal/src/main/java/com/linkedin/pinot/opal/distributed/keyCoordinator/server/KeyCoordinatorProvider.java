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
package com.linkedin.pinot.opal.distributed.keyCoordinator.server;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KeyCoordinatorProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorProvider.class);
  private static KeyCoordinatorProvider _instance = null;

  private Configuration _conf;
  private KeyCoordinatorQueueProducer _producer;

  public KeyCoordinatorProvider(Configuration conf) {
    _conf = conf;
    _producer = new KeyCoordinatorQueueProducer(conf.subset(ServerKeyCoordinatorConfig.PRODUCER_CONFIG));
    synchronized (KeyCoordinatorProvider.class) {
      if (_instance == null) {
        _instance = this;
      } else {
        throw new RuntimeException("cannot re-initialize key coordinator provide when there is already one instance");
      }
    }
  }

  public static KeyCoordinatorProvider getInstance() {
    if (_instance != null) {
      return _instance;
    } else {
      throw new RuntimeException("cannot get instance of key coordinator provider without initializing one before");
    }

  }

  public KeyCoordinatorQueueProducer getProducer() {
    return _producer;
  }

  public void close() {
    //TODO close producer and what not
  }
}
