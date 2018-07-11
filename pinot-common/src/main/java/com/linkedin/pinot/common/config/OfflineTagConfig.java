/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

/**
 * Wrapper class over TableConfig for an offline table
 * This class will help answer questions about what are server tags for a table
 */
public class OfflineTagConfig extends TagConfig {

  private String _offlineServerTag;

  public OfflineTagConfig(TableConfig tableConfig) {
    super(tableConfig);

    _offlineServerTag = TagNameUtils.getOfflineTagForTenant(_serverTenant);
  }

  public String getOfflineServerTag() {
    return _offlineServerTag;
  }
}

