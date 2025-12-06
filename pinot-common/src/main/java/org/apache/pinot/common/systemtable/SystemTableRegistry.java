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
package org.apache.pinot.common.systemtable;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.systemtable.SystemTableProvider;


/**
 * Registry to hold and lifecycle-manage system table providers.
 */
public final class SystemTableRegistry implements AutoCloseable {
  public static final SystemTableRegistry INSTANCE = new SystemTableRegistry();

  private final Map<String, SystemTableProvider> _providers = new ConcurrentHashMap<>();

  public void register(SystemTableProvider provider) {
    _providers.put(normalize(provider.getTableName()), provider);
  }

  public @Nullable SystemTableProvider get(String tableName) {
    return _providers.get(normalize(tableName));
  }

  public boolean isRegistered(String tableName) {
    return _providers.containsKey(normalize(tableName));
  }

  public Collection<SystemTableProvider> getProviders() {
    return Collections.unmodifiableCollection(_providers.values());
  }

  @Override
  public void close()
      throws Exception {
    for (SystemTableProvider provider : _providers.values()) {
      provider.close();
    }
  }

  private static String normalize(String tableName) {
    return tableName.toLowerCase(Locale.ROOT);
  }
}
