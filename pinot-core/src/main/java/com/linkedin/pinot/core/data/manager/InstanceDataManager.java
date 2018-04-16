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
package com.linkedin.pinot.core.data.manager;

import com.linkedin.pinot.common.data.DataManager;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;


// TODO: resolve the dependency issue between pinot-common and pinot-core, merge DataManager into InstanceDataManager
@ThreadSafe
public interface InstanceDataManager extends DataManager {

  /**
   * Gets the table manager for the given table.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Table data manager for the given table, null if table does not exist
   */
  @Nullable
  TableDataManager getTableDataManager(@Nonnull String tableNameWithType);

  @Nonnull
  Collection<TableDataManager> getTableDataManagers();
}
