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
import java.net.URISyntaxException;
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
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A segment uploader which does segment upload to a segment store (with store root dir configured as
/// \_segmentStoreUriStr) using PinotFS within a configurable timeout period. The final segment location would be in the
/// URI \_segmentStoreUriStr/\_tableNameWithType/segmentName+random_uuid if successful. It can also upload the segment
/// metadata tar to \_segmentStoreUriStr/\_tableNameWithType/segmentName.metadata.tar.gz.
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
          SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName())));
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
      LOGGER.warn("Failed to upload file {} of segment {} for table {}",
              segmentFile.getAbsolutePath(), segmentName, rawTableName, e);
    }
    _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);

    return null;
  }

  @Override
  public boolean isMetadataTarUploadSupported() {
    return true;
  }

  @Override
  public URI uploadMetadataTar(File metadataTarFile, LLCSegmentName segmentName, int timeoutInMillis) {
    if (_segmentStoreUriStr == null || _segmentStoreUriStr.isEmpty()) {
      LOGGER.error("Missing segment store uri. Failed to upload metadata tar {} for {}.", metadataTarFile.getName(),
          segmentName.getSegmentName());
      return null;
    }
    final String rawTableName = TableNameBuilder.extractRawTableName(segmentName.getTableName());
    // Like the segment file, upload to a unique tmp name. The controller moves it to <segment>.metadata.tar.gz at
    // commit end. The final object is never written or deleted here, so a failed or timed-out upload cannot destroy a
    // sidecar left by a previous attempt.
    final URI destUri;
    try {
      destUri = new URI(StringUtil.join(File.separator, _segmentStoreUriStr, segmentName.getTableName(),
          SegmentCompletionUtils.generateTmpSegmentFileName(
              segmentName.getSegmentName() + Constants.METADATA_TAR_GZ_FILE_EXT)));
    } catch (URISyntaxException e) {
      LOGGER.warn("Invalid segment store uri {} for metadata tar of segment {}", _segmentStoreUriStr, segmentName, e);
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.METADATA_TAR_UPLOAD_FAILURE, 1);
      return null;
    }
    Callable<URI> uploadTask = () -> {
      long startTime = System.currentTimeMillis();
      try {
        PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
        pinotFS.copyFromLocalFile(metadataTarFile, destUri);
        return destUri;
      } catch (Exception e) {
        LOGGER.warn("Failed copy metadata tar file {} to segment store {}: {}", metadataTarFile.getName(), destUri, e);
      } finally {
        long duration = System.currentTimeMillis() - startTime;
        _serverMetrics.addTimedTableValue(rawTableName, ServerTimer.METADATA_TAR_UPLOAD_TIME_MS, duration,
            TimeUnit.MILLISECONDS);
      }
      return null;
    };
    Future<URI> future = _executorService.submit(uploadTask);
    try {
      URI metadataTarLocation = future.get(timeoutInMillis, TimeUnit.MILLISECONDS);
      if (metadataTarLocation != null) {
        LOGGER.info("Successfully uploaded metadata tar of segment {} to {}.", segmentName, metadataTarLocation);
        _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.METADATA_TAR_UPLOAD_SUCCESS, 1);
        return metadataTarLocation;
      }
    } catch (InterruptedException e) {
      LOGGER.info("Interrupted while waiting for metadata tar upload of {} to {}.", segmentName, _segmentStoreUriStr);
      future.cancel(true);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      future.cancel(true);
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.METADATA_TAR_UPLOAD_TIMEOUT, 1);
      LOGGER.warn("Timed out waiting to upload metadata tar of segment: {} for table: {}",
          segmentName.getSegmentName(), rawTableName);
    } catch (Exception e) {
      LOGGER.warn("Failed to upload metadata tar {} of segment {} for table {}", metadataTarFile.getAbsolutePath(),
          segmentName, rawTableName, e);
    }
    // Fire and forget: the store may be slow or hung (that is why we got here) and the commit must not wait for it.
    // A copy that outlives the timeout can still leave a tmp object; the controller's tmp file cleanup removes it.
    _executorService.submit(() -> deleteTmpQuietly(destUri));
    _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.METADATA_TAR_UPLOAD_FAILURE, 1);
    return null;
  }

  private void deleteTmpQuietly(URI tmpUri) {
    try {
      PinotFSFactory.create(tmpUri.getScheme()).delete(tmpUri, true);
    } catch (Exception e) {
      LOGGER.warn("Could not delete temporary metadata tar {}", tmpUri, e);
    }
  }
}
