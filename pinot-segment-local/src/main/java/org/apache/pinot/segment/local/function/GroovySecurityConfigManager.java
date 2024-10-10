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
package org.apache.pinot.segment.local.function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;


public class GroovySecurityConfigManager {
  public static final String PROPERTYSTORE_PATH = "/CONFIGS/GROOVY_EXECUTION/StaticAnalyzer";
  private static LoadingCache<Integer, GroovyStaticAnalyzerConfig> _configCache;
  private static HelixManager _helixManager;

  public GroovySecurityConfigManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _configCache = CacheBuilder.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<>() {
          @Override
          public GroovyStaticAnalyzerConfig load(Integer key)
              throws Exception {
            Stat stat = new Stat();
            ZNRecord record = _helixManager.getHelixPropertyStore().get(
                "/CONFIGS/GROOVY_EXECUTION/StaticAnalyzer",
                stat, AccessOption.PERSISTENT);
            return GroovyStaticAnalyzerConfig.fromZNRecord(record);
          }
        });
  }

  public void setConfig(GroovyStaticAnalyzerConfig config) throws Exception {
    ZNRecord zr = config.toZNRecord();
    _helixManager.getHelixPropertyStore().set(
        PROPERTYSTORE_PATH,
        zr,
        AccessOption.PERSISTENT
    );
  }

  public GroovyStaticAnalyzerConfig getConfig() throws Exception {
    Stat stat = new Stat();
    ZNRecord record = _helixManager.getHelixPropertyStore().get(
        "/CONFIGS/GROOVY_EXECUTION/StaticAnalyzer",
        stat, AccessOption.PERSISTENT);
    if (record == null) {
      return null;
    } else {
      return GroovyStaticAnalyzerConfig.fromZNRecord(record);
    }
  }
}
