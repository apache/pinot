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
package com.linkedin.pinot.core.data.manager.offline;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import javax.annotation.Nonnull;


/**
 * Table data manager for OFFLINE table.
 */
public class OfflineTableDataManager extends AbstractTableDataManager {

  @Override
  protected void doInit() {
  }

  @Override
  protected void doShutdown() {
  }

  @Override
  public void addSegment(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig) throws Exception {
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableName);
    addSegment(ColumnarSegmentLoader.load(indexDir, indexLoadingConfig, schema));
  }
}
