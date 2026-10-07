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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;


/// Sends segmentStart, segmentUpload, & segmentCommitEnd to the controller
/// If that succeeds, swap in-memory segment with the one built.
public class SplitSegmentCommitter implements SegmentCommitter {
  private static final int METADATA_TAR_UPLOAD_TIMEOUT_MS =
      PinotFSSegmentUploader.DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS;

  protected final SegmentCompletionProtocol.Request.Params _params;
  protected final ServerSegmentCompletionProtocolHandler _protocolHandler;
  protected final SegmentUploader _segmentUploader;
  protected final String _peerDownloadScheme;
  private final boolean _uploadMetadataTar;
  private final ServerMetrics _serverMetrics;
  protected final Logger _segmentLogger;

  public SplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader,
      @Nullable String peerDownloadScheme, boolean uploadMetadataTar, ServerMetrics serverMetrics) {
    _segmentLogger = segmentLogger;
    _protocolHandler = protocolHandler;
    _params = new SegmentCompletionProtocol.Request.Params(params);
    _segmentUploader = segmentUploader;
    _peerDownloadScheme = peerDownloadScheme;
    _uploadMetadataTar = uploadMetadataTar;
    _serverMetrics = serverMetrics;
  }

  @VisibleForTesting
  SegmentUploader getSegmentUploader() {
    return _segmentUploader;
  }

  public SplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader) {
    this(segmentLogger, protocolHandler, params, segmentUploader, null, false, ServerMetrics.get());
  }

  @Override
  public SegmentCompletionProtocol.Response commit(
      RealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor) {
    File segmentTarFile = segmentBuildDescriptor.getSegmentTarFile();

    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(_params);
    if (!segmentCommitStartResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      _segmentLogger.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    String segmentLocation = uploadSegment(segmentTarFile, _segmentUploader, _params);
    if (segmentLocation == null) {
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    _params.withSegmentLocation(segmentLocation);

    if (_uploadMetadataTar && _segmentUploader.isMetadataTarUploadSupported()
        && !isPeerSegmentLocation(segmentLocation)) {
      String metadataTarLocation =
          uploadMetadataTarQuietly(segmentBuildDescriptor.getMetadataFiles(), _params.getSegmentName());
      if (metadataTarLocation != null) {
        _params.withMetadataTarLocation(metadataTarLocation);
      }
    }

    SegmentCompletionProtocol.Response commitEndResponse =
        _protocolHandler.segmentCommitEndWithMetadata(_params, segmentBuildDescriptor.getMetadataFiles());

    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      _segmentLogger.warn("CommitEnd failed with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }

  // Return null iff the segment upload fails.
  protected String uploadSegment(File segmentTarFile, SegmentUploader segmentUploader,
      SegmentCompletionProtocol.Request.Params params) {
    URI segmentLocation = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(params.getSegmentName()));
    if (segmentLocation != null) {
      return segmentLocation.toString();
    }
    if (_peerDownloadScheme != null) {
      return StringUtil.join("/", CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME,
            params.getSegmentName());
    }
    return null;
  }

  // A peer:// location means the segment is NOT in deep store, so there is nothing for a sidecar to describe.
  // Mirrors the check in PinotLLCRealtimeSegmentManager.commitSegmentFile().
  private static boolean isPeerSegmentLocation(String segmentLocation) {
    return segmentLocation.regionMatches(true, 0, CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME, 0,
        CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME.length());
  }

  /// Best-effort: builds a tar containing one directory with metadata.properties and creation.meta, and uploads it
  /// under a tmp name next to the segment. Returns the tmp location for the controller to move at commit end, or
  /// null on any problem. Never fails or throws; a sidecar problem must not affect the segment commit.
  @Nullable
  private String uploadMetadataTarQuietly(@Nullable Map<String, File> metadataFiles, String segmentName) {
    if (metadataFiles == null || metadataFiles.isEmpty()) {
      return null;
    }
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String rawTableName = TableNameBuilder.extractRawTableName(llcSegmentName.getTableName());
    File stagingDir = null;
    File metadataTarFile = null;
    try {
      stagingDir = new File(FileUtils.getTempDirectory(), segmentName + "_meta_" + UUID.randomUUID());
      FileUtils.forceMkdir(stagingDir);
      for (Map.Entry<String, File> entry : metadataFiles.entrySet()) {
        // Byte-for-byte copy: creation.meta is binary.
        FileUtils.copyFile(entry.getValue(), new File(stagingDir, entry.getKey()));
      }
      metadataTarFile = new File(FileUtils.getTempDirectory(),
          segmentName + "_" + UUID.randomUUID() + Constants.METADATA_TAR_GZ_FILE_EXT);
      // Tar the DIRECTORY so that the first archive entry is a directory, as DefaultMetadataExtractor expects.
      TarCompressionUtils.createCompressedTarFile(stagingDir, metadataTarFile);
      URI metadataTarLocation =
          _segmentUploader.uploadMetadataTar(metadataTarFile, llcSegmentName, METADATA_TAR_UPLOAD_TIMEOUT_MS);
      return metadataTarLocation == null ? null : metadataTarLocation.toString();
    } catch (Exception e) {
      _segmentLogger.warn("Failed to upload metadata tar for segment: {}; continuing with commit", segmentName, e);
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.METADATA_TAR_UPLOAD_FAILURE, 1);
      return null;
    } finally {
      FileUtils.deleteQuietly(stagingDir);
      FileUtils.deleteQuietly(metadataTarFile);
    }
  }
}
