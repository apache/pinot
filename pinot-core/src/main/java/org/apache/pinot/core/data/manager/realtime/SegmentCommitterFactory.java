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

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for the SegmentCommitter interface
 */
public class SegmentCommitterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCommitterFactory.class);
  private static SegmentCommitter _segmentCommitter;

  // Prevent factory from being instantiated.
  private SegmentCommitterFactory() {

  }

  public static void init(Configuration config) {
    String committerClass = (String) config.getProperty(CommonConstants.Segment.Realtime.COMMITTER_CLASS);
    try {
      LOGGER.info("Creating segment committer class {}", committerClass);
      _segmentCommitter = (SegmentCommitter) Class.forName(committerClass).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not create segment committer " + committerClass);
    }
  }

  public static SegmentCommitter create(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      IndexLoadingConfig indexLoadingConfig, SegmentCompletionProtocol.Request.Params params, SegmentCompletionProtocol.Response prevResponse) {
    return _segmentCommitter.init(segmentLogger, protocolHandler, indexLoadingConfig, params, prevResponse);
  }
}
