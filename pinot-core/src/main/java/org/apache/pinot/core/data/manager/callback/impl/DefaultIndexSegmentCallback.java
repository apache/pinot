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
package org.apache.pinot.core.data.manager.callback.impl;

import joptsimple.internal.Strings;
import org.apache.pinot.core.data.manager.callback.IndexSegmentCallback;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.util.Map;

/**
 * class that used for regular append-only ingestion mode or data processing that don't support upsert
 * all method are no-op to ensure that regular append-only ingestion mode has the same performance as before
 */
public class DefaultIndexSegmentCallback implements IndexSegmentCallback {

  public static final DefaultIndexSegmentCallback INSTANCE = new DefaultIndexSegmentCallback();

  private DefaultIndexSegmentCallback() {
  }

  @Override
  public void init(SegmentMetadata segmentMetadata, Map<String, ColumnIndexContainer> columnIndexContainerMap) {
  }

  @Override
  public void postProcessRecords(GenericRow row, int docId) {
  }

  @Override
  public String getVirtualColumnInfo(long offset) {
    return Strings.EMPTY;
  }
}
