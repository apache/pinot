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

package com.linkedin.pinot.core.data.manager.realtime;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MmapMemoryManager;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import java.io.File;
import java.util.List;


public abstract class RealtimeSegmentDataManager extends SegmentDataManager {
  protected RealtimeSegmentStatsHistory _statsHistory;
  protected RealtimeIndexOffHeapMemoryManager _memoryManager;

  public abstract String getTableName();

  public abstract Schema getSchema();

  public abstract List<String> getNoDictionaryColumns();

  public abstract List<String> getInvertedIndexColumns();

  public abstract File getTableDataDir();

  protected void initStatsHistory(RealtimeTableDataManager realtimeTableDataManager) {
    _statsHistory = realtimeTableDataManager.getStatsHistory();
  }

  public RealtimeSegmentStatsHistory getStatsHistory() {
    return _statsHistory;
  }

  protected void initMemoryManager(RealtimeTableDataManager realtimeTableDataManager, boolean isOffHeapAllocation, String segmentName) {
    if (isOffHeapAllocation) {
      _memoryManager = new MmapMemoryManager(realtimeTableDataManager.getConsumerDir(), segmentName);
    } else {
      _memoryManager = new DirectMemoryManager(segmentName);
    }
  }

  public RealtimeIndexOffHeapMemoryManager getMemoryManager() {
    return _memoryManager;
  }
}
