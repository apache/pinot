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

import org.apache.pinot.core.data.manager.callback.DataManagerCallback;
import org.apache.pinot.core.data.manager.callback.IndexSegmentCallback;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

/**
 * class that used for regular append-only ingestion mode or data processing that don't support upsert
 * all method are no-op to ensure that regular append-only ingestion mode has the same performance as before
 */
public class DefaultDataManagerCallbackImpl implements DataManagerCallback {

  public static final DefaultDataManagerCallbackImpl INSTANCE = new DefaultDataManagerCallbackImpl();

  private final IndexSegmentCallback _indexSegmentCallback;

  private DefaultDataManagerCallbackImpl() {
    _indexSegmentCallback = DefaultIndexSegmentCallback.INSTANCE;
  }

  @Override
  public void onDataManagerCreation() {
  }

  /**
   * return cached default index segment callback for better performance
   * @return a default cached object of {@link org.apache.pinot.core.data.manager.callback.IndexSegmentCallback}
   */
  public IndexSegmentCallback getIndexSegmentCallback() {
    return _indexSegmentCallback;
  }

  @Override
  public void onRowTransformed(GenericRow row, StreamPartitionMsgOffset offset) {
  }

  @Override
  public void onRowIndexed(GenericRow row, StreamPartitionMsgOffset offset) {
  }

  @Override
  public void onConsumptionStoppedOrEndReached() {
  }

  @Override
  public void onDataManagerDestroyed() {
  }
}
