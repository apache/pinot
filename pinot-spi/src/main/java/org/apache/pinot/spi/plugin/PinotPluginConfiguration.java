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
package org.apache.pinot.spi.plugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class PinotPluginConfiguration {

  private final Map<String, List<String>> _importsFromRealm;

  private final String _parentRealmId;

  /**
   * Expose plugin configuration based on Properties
   *
   * Possible entries:
   * key: importFrom.[realmId]
   *  value: comma-separated list of packages
   * key: parent.realmId
   *  value: pinot, in case you want to make use of all pinot-framework classes first
   *
   * @param properties properties-based configuration
   */
  PinotPluginConfiguration(Properties properties) {
    Map<String, List<String>> importsFromRealm = new HashMap<>();
    properties.forEach((k, v) -> {
      String key = k.toString();
      if (k.toString().startsWith("importFrom.")) {
        String realm = key.substring("importFrom.".length());
        List<String> importsFrom = Stream.of(v.toString().split(",")).collect(Collectors.toList());
        importsFromRealm.put(realm, List.copyOf(importsFrom));
      }
    });
    _importsFromRealm = Map.copyOf(importsFromRealm);
    _parentRealmId = properties.getProperty("parent.realmId");
  }

  public Map<String, List<String>> getImportsFromPerRealm() {
    return _importsFromRealm;
  }

  public Optional<String> getParentRealmId() {
    return Optional.ofNullable(_parentRealmId);
  }
}
