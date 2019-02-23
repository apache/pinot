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
package com.linkedin.pinot.opal.distributed.keyCoordinator.internal;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConfigStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigStore.class);
  private Multimap<String, String> _tableSegmentMap = ArrayListMultimap.create();
  private volatile static ConfigStore _instance;
  private String _filePath;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ConfigStore(String filePath) {
    _filePath = filePath;
    try {
      ensureConfigFile(filePath);
//      _tableSegmentMap = loadData();
    } catch (IOException ex) {
      LOGGER.error("config file not found", ex);
      throw new RuntimeException(ex);
    }
  }

  public static ConfigStore getConfigStore(String filePath) {
    if (_instance == null) {
      synchronized (ConfigStore.class) {
        if (_instance == null) {
          _instance = new ConfigStore(filePath);
        }
      }
    }
    return _instance;
  }

  public void addTableSegment(String tableName, String segment) {
    synchronized (_tableSegmentMap) {
      _tableSegmentMap.put(tableName, segment);
    }
  }

  public void deleteTableSegment(String tableName, String segment) {
    synchronized (_tableSegmentMap) {
      _tableSegmentMap.remove(tableName, segment);
    }
  }

  private void ensureConfigFile(String filePathString) throws IOException {
    Path filePath = Paths.get(filePathString);
    if (!Files.exists(filePath)) {
      LOGGER.info("no config file, creating a new one now");
      Files.createFile(filePath);
    }
  }

  private void serializeData() throws IOException {
    synchronized (_filePath) {
      MAPPER.writeValue(new FileOutputStream(_filePath), _tableSegmentMap);
    }
  }

  private Multimap<String, String> loadData() throws IOException {
    synchronized (_filePath) {
      return MAPPER.readValue(new FileInputStream(_filePath), Multimap.class);
    }
  }
}
