/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;


public class IndexSegmentImplNew implements IndexSegment {
  private static final Logger logger = Logger.getLogger(IndexSegmentImplNew.class);

  private final SegmentMetadataImpl segmentMetadata;
  private final Map<String, ColumnIndexContainer> indexContainerMap;
  private final File indexDir;

  public IndexSegmentImplNew(SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap, File indexDir) {
    this.segmentMetadata = segmentMetadata;
    this.indexContainerMap = columnIndexContainerMap;
    this.indexDir = indexDir;
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.COLUMNAR;
  }

  @Override
  public String getSegmentName() {
    return segmentMetadata.getName();
  }

  @Override
  public String getAssociatedDirectory() {
    return indexDir.getAbsolutePath();
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return segmentMetadata;
  }

  @Override
  public DataSource getDataSource(String columnName) {
    return null;
  }

  @Override
  public String[] getColumnNames() {
    return (String[]) segmentMetadata.getAllColumns().toArray();
  }

  @Override
  public void destroy() {
    try {
      segmentMetadata.close();
      for (Entry<String, ColumnIndexContainer> entry : indexContainerMap.entrySet()) {
        entry.getValue().unload();
      }
    } catch (Exception e) {
      logger.error(e);
    }
  }

  @Override
  public int getTotalDocs() {
    return segmentMetadata.getTotalDocs();
  }
}
