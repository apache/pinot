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

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import java.io.File;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.LoggerFactory;


/**
 * An implementation of offline TableDataManager.
 * Provide add and remove segment functionality.
 */
public class OfflineTableDataManager extends AbstractTableDataManager {

  public OfflineTableDataManager() {
    super();
  }

  protected void doShutdown() {
  }

  protected void doInit() {
    LOGGER = LoggerFactory.getLogger(_tableName + "-OfflineTableDataManager");
  }

  @Override
  public void addSegment(SegmentMetadata segmentMetadata, Schema schema)
      throws Exception {
    IndexSegment indexSegment = ColumnarSegmentLoader.loadSegment(new File(segmentMetadata.getIndexDir()), _readMode,
        _indexLoadingConfigMetadata, schema);
    addSegment(indexSegment);
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata)
      throws Exception {
    throw new UnsupportedOperationException("Not supported for Offline segments");
  }
}
