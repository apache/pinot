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
package org.apache.pinot.core.data.manager.realtime;

import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;


/**
 * Factory for the SegmentCommitter interface
 */
public class SegmentCommitterFactory {
  private static Logger LOGGER;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;

  public SegmentCommitterFactory(Logger segmentLogger, IndexLoadingConfig indexLoadingConfig,
      ServerSegmentCompletionProtocolHandler protocolHandler) {
    LOGGER = segmentLogger;
    _indexLoadingConfig = indexLoadingConfig;
    _protocolHandler = protocolHandler;
  }
  
  public SegmentCommitter createSplitSegmentCommitter(SegmentCompletionProtocol.Request.Params params, String controllerVipUrl) {
    return new SplitSegmentCommitter(LOGGER, _protocolHandler, _indexLoadingConfig, params, controllerVipUrl);
  }

  public SegmentCommitter createDefaultSegmentCommitter(SegmentCompletionProtocol.Request.Params params) {
    return new DefaultSegmentCommitter(LOGGER, _protocolHandler, params);
  }
}
