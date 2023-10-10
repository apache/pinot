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

import java.net.URISyntaxException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;


/**
 * Factory for the SegmentCommitter interface
 */
public class SegmentCommitterFactory {
  private static Logger _logger;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final TableConfig _tableConfig;
  private final StreamConfig _streamConfig;
  private final ServerMetrics _serverMetrics;
  private final IndexLoadingConfig _indexLoadingConfig;

  public SegmentCommitterFactory(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig, ServerMetrics serverMetrics) {
    _logger = segmentLogger;
    _protocolHandler = protocolHandler;
    _tableConfig = tableConfig;
    _streamConfig = new StreamConfig(_tableConfig.getTableName(),
        IngestionConfigUtils.getStreamConfigMap(_tableConfig));
    _indexLoadingConfig = indexLoadingConfig;
    _serverMetrics = serverMetrics;
  }

  public SegmentCommitter createSegmentCommitter(SegmentCompletionProtocol.Request.Params params,
      String controllerVipUrl)
      throws URISyntaxException {
    boolean uploadToFs = _streamConfig.isServerUploadToDeepStore();
    String peerSegmentDownloadScheme = _tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
    String segmentStoreUri = _indexLoadingConfig.getSegmentStoreURI();

    SegmentUploader segmentUploader;
    if (uploadToFs || peerSegmentDownloadScheme != null) {
      // TODO: peer scheme non-null check exists for backwards compatibility. remove check once users have migrated
      segmentUploader = new PinotFSSegmentUploader(segmentStoreUri,
          ServerSegmentCompletionProtocolHandler.getSegmentUploadRequestTimeoutMs(), _serverMetrics);
    } else {
      segmentUploader = new Server2ControllerSegmentUploader(_logger,
          _protocolHandler.getFileUploadDownloadClient(),
          _protocolHandler.getSegmentCommitUploadURL(params, controllerVipUrl), params.getSegmentName(),
          ServerSegmentCompletionProtocolHandler.getSegmentUploadRequestTimeoutMs(), _serverMetrics,
          _protocolHandler.getAuthProvider(), _tableConfig.getTableName());
    }

    return new SplitSegmentCommitter(_logger, _protocolHandler, params, segmentUploader, peerSegmentDownloadScheme);
  }
}
