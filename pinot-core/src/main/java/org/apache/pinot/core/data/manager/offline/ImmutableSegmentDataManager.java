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
package org.apache.pinot.core.data.manager.offline;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.callback.DataManagerCallback;
import org.apache.pinot.core.data.manager.callback.impl.DefaultDataManagerCallbackImpl;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;

import java.io.IOException;


/**
 * Segment data manager for immutable segment.
 */
public class ImmutableSegmentDataManager extends SegmentDataManager {

  private final ImmutableSegment _immutableSegment;
  private final DataManagerCallback _dataManagerCallback;

  public ImmutableSegmentDataManager(ImmutableSegment immutableSegment) {
    this(immutableSegment, DefaultDataManagerCallbackImpl.INSTANCE);
  }

  public ImmutableSegmentDataManager(ImmutableSegment immutableSegment, DataManagerCallback dataManagerCallback) {
    _immutableSegment = immutableSegment;
    _dataManagerCallback = dataManagerCallback;
    try {
      _dataManagerCallback.init();
    } catch (IOException ex) {
      ExceptionUtils.rethrow(ex);
    }
  }

  @Override
  public String getSegmentName() {
    return _immutableSegment.getSegmentName();
  }

  @Override
  public ImmutableSegment getSegment() {
    return _immutableSegment;
  }

  @Override
  public void destroy() {
    _dataManagerCallback.destroy();
    _immutableSegment.destroy();
  }

  @Override
  public String toString() {
    return "ImmutableSegmentDataManager(" + _immutableSegment.getSegmentName() + ")";
  }
}
