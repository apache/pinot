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

import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Context object passed to {@link SystemTableProvider#init(SystemTableProviderContext)} during registration.
 * <p>
 * Bundles all broker-provided dependencies so that providers can pick what they need without requiring specific
 * constructor signatures. This makes the SPI forward-compatible: new dependencies can be added here without
 * breaking existing providers.
 */
public final class SystemTableProviderContext {
  @Nullable
  private final TableCache _tableCache;
  @Nullable
  private final HelixAdmin _helixAdmin;
  @Nullable
  private final String _clusterName;
  @Nullable
  private final PinotConfiguration _config;

  public SystemTableProviderContext(@Nullable TableCache tableCache, @Nullable HelixAdmin helixAdmin,
      @Nullable String clusterName, @Nullable PinotConfiguration config) {
    _tableCache = tableCache;
    _helixAdmin = helixAdmin;
    _clusterName = clusterName;
    _config = config;
  }

  @Nullable
  public TableCache getTableCache() {
    return _tableCache;
  }

  @Nullable
  public HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  @Nullable
  public String getClusterName() {
    return _clusterName;
  }

  @Nullable
  public PinotConfiguration getConfig() {
    return _config;
  }
}
