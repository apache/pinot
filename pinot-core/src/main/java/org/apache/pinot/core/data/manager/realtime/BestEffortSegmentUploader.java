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

import java.io.File;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// A segment uploader which does best effort segment upload to a segment store (with store root dir configured as
// _segmentStoreUriStr). The upload is successful if it is done within a configurable timeout period.
// The final segment location would be in the URI _segmentStoreUriStr/_tableNameWithType/segmentName if
// successful. If a segment upload fails or there is no segment store uri configured, it sets the segment location as
// the default URI.
public class BestEffortSegmentUploader implements SegmentUploader {
  private Logger LOGGER = LoggerFactory.getLogger(BestEffortSegmentUploader.class);
  private String _segmentStoreUriStr;
  private ExecutorService _executorService = Executors.newCachedThreadPool();
  // The default segment location URI to return if the upload fails.
  private URI _defaultSegmentLocationURI;
  private int _timeoutInMs;

  public BestEffortSegmentUploader(String segmentStoreDirUri, int timeoutMillis, String defaultSegment) {
    _segmentStoreUriStr = segmentStoreUriStr;
    _timeoutInMs = timeoutInMs;
    _defaultSegmentLocationURI = URI.create(defaultSegment);
  }

  public URI uploadSegment(File segmentFile, String tableNameWithType, String segmentName) {
    if (_segmentStoreUriStr == null || _segmentStoreUriStr.isEmpty()) {
      return _defaultSegmentLocationURI;
    }

    Callable<URI> uploadTask = () -> {
      try {
        PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
        URI destUri = new URI(StringUtil.join(File.separator, _segmentStoreUriStr, tableNameWithType, segmentName));
        // Check and delete any existing segment file.
        if (pinotFS.exists(destUri)) {
          pinotFS.delete(destUri, true);
        }
        pinotFS.copyFromLocalFile(segmentFile, destUri);
        return destUri;
      } catch (Exception e) {
        LOGGER.error("Failed copy segment tar file to segment store {}: {}", segmentFile.getName(), e);
      }
      return _defaultSegmentLocationURI;
    };
    Future<URI> future = _executorService.submit(uploadTask);
    try {
      URI segmentLocation = future.get(_timeoutInMs, TimeUnit.MILLISECONDS);
      return segmentLocation;
    } catch (Exception e) {
      LOGGER.error("Failed to upload file {} of segment {} for table {} ", segmentFile.getAbsolutePath(), segmentName,
          tableNameWithType, e);
    }

    return _defaultSegmentLocationURI;
  }
}
