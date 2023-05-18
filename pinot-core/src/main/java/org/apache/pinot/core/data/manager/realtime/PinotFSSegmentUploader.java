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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A segment uploader which does segment upload to a segment store (with store root dir configured as
 * _segmentStoreUriStr) using PinotFS within a configurable timeout period. The final segment location would be in the
 * URI _segmentStoreUriStr/_tableNameWithType/segmentName+random_uuid if successful.
 */
public class PinotFSSegmentUploader implements SegmentUploader {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSSegmentUploader.class);
  public static final int DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS = 10 * 1000;

  private final String _segmentStoreUriStr;
  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final int _timeoutInMs;
  private final ServerMetrics _serverMetrics;

  public PinotFSSegmentUploader(String segmentStoreDirUri, int timeoutMillis, ServerMetrics serverMetrics) {
    _segmentStoreUriStr = segmentStoreDirUri;
    _timeoutInMs = timeoutMillis;
    _serverMetrics = serverMetrics;
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName) {
    return uploadSegment(segmentFile, segmentName, _timeoutInMs);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis) {
    if (_segmentStoreUriStr == null || _segmentStoreUriStr.isEmpty()) {
      LOGGER.error("Missing segment store uri. Failed to upload segment file {} for {}.", segmentFile.getName(),
          segmentName.getSegmentName());
      return null;
    }
    final String rawTableName = TableNameBuilder.extractRawTableName(segmentName.getTableName());
    Callable<URI> uploadTask = () -> {
      URI destUri = new URI(StringUtil.join(File.separator, _segmentStoreUriStr, segmentName.getTableName(),
          segmentName.getSegmentName() + UUID.randomUUID().toString()));
      long startTime = System.currentTimeMillis();
      try {
        PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
        // Check and delete any existing segment file.
        if (pinotFS.exists(destUri)) {
          pinotFS.delete(destUri, true);
        }
        pinotFS.copyFromLocalFile(segmentFile, destUri);
        return destUri;
      } catch (Exception e) {
        LOGGER.warn("Failed copy segment tar file {} to segment store {}: {}", segmentFile.getName(), destUri, e);
      } finally {
        long duration = System.currentTimeMillis() - startTime;
        _serverMetrics.addTimedTableValue(rawTableName, ServerTimer.SEGMENT_UPLOAD_TIME_MS, duration,
            TimeUnit.MILLISECONDS);
      }
      return null;
    };
    Future<URI> future = _executorService.submit(uploadTask);
    try {
      URI segmentLocation = future.get(timeoutInMillis, TimeUnit.MILLISECONDS);
      LOGGER.info("Successfully upload segment {} to {}.", segmentName, segmentLocation);
      _serverMetrics.addMeteredTableValue(rawTableName,
          segmentLocation == null ? ServerMeter.SEGMENT_UPLOAD_FAILURE : ServerMeter.SEGMENT_UPLOAD_SUCCESS, 1);
      return segmentLocation;
    } catch (InterruptedException e) {
      LOGGER.info("Interrupted while waiting for segment upload of {} to {}.", segmentName, _segmentStoreUriStr);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      // Emit a separate metric for timeout since this is relatively more common than other errors.
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_TIMEOUT, 1);
      LOGGER.warn("Timed out waiting to upload segment: {} for table: {}", segmentName.getSegmentName(), rawTableName);
    } catch (Exception e) {
      LOGGER
          .warn("Failed to upload file {} of segment {} for table {} ", segmentFile.getAbsolutePath(), segmentName, e);
    }
    _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);

    return null;
  }
}
