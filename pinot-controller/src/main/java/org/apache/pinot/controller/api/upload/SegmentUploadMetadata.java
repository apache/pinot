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
package org.apache.pinot.controller.api.upload;

import java.io.File;
import java.net.URI;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.segment.spi.SegmentMetadata;


/**
 * Data object used while adding or updating segments. It's comprised of the following fields:
 * <ol>
 *   <li>segmentDownloadURIStr – The segment download URI persisted into the ZK metadata.</li>
 *   <li>sourceDownloadURIStr – The URI from where the segment could be downloaded.</li>
 *   <li>finalSegmentLocationURI – The final location of the segment in the deep-store.</li>
 *   <li>segmentSizeInBytes – The segment size in bytes.</li>
 *   <li>segmentMetadata – The segment metadata as defined in {@link org.apache.pinot.segment.spi.SegmentMetadata}.</li>
 *   <li>encryptionInfo – A pair consisting of the crypter class used to encrypt the segment, and the encrypted segment
 *   file.</li>
 *   <li>segmentMetadataZNRecord – The segment metadata represented as a helix
 *   {@link org.apache.helix.zookeeper.datamodel.ZNRecord}.</li>
 * </ol>
 */
public class SegmentUploadMetadata {
  private final String _segmentDownloadURIStr;
  private final String _sourceDownloadURIStr;
  private final URI _finalSegmentLocationURI;
  private final Long _segmentSizeInBytes;
  private final SegmentMetadata _segmentMetadata;
  private final Pair<String, File> _encryptionInfo;
  private ZNRecord _segmentMetadataZNRecord;

  public SegmentUploadMetadata(String segmentDownloadURIStr, String sourceDownloadURIStr, URI finalSegmentLocationURI,
      Long segmentSizeInBytes, SegmentMetadata segmentMetadata, Pair<String, File> encryptionInfo) {
    _segmentDownloadURIStr = segmentDownloadURIStr;
    _sourceDownloadURIStr = sourceDownloadURIStr;
    _segmentSizeInBytes = segmentSizeInBytes;
    _segmentMetadata = segmentMetadata;
    _encryptionInfo = encryptionInfo;
    _finalSegmentLocationURI = finalSegmentLocationURI;
  }

  public String getSegmentDownloadURIStr() {
    return _segmentDownloadURIStr;
  }

  public String getSourceDownloadURIStr() {
    return _sourceDownloadURIStr;
  }

  public URI getFinalSegmentLocationURI() {
    return _finalSegmentLocationURI;
  }

  public Long getSegmentSizeInBytes() {
    return _segmentSizeInBytes;
  }

  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  public Pair<String, File> getEncryptionInfo() {
    return _encryptionInfo;
  }

  public void setSegmentMetadataZNRecord(ZNRecord segmentMetadataZNRecord) {
    _segmentMetadataZNRecord = segmentMetadataZNRecord;
  }

  public ZNRecord getSegmentMetadataZNRecord() {
    return _segmentMetadataZNRecord;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentUploadMetadata that = (SegmentUploadMetadata) o;
    return Objects.equals(_segmentDownloadURIStr, that._segmentDownloadURIStr)
        && Objects.equals(_sourceDownloadURIStr, that._sourceDownloadURIStr)
        && Objects.equals(_finalSegmentLocationURI, that._finalSegmentLocationURI)
        && Objects.equals(_segmentSizeInBytes, that._segmentSizeInBytes)
        && Objects.equals(_segmentMetadata, that._segmentMetadata)
        && Objects.equals(_encryptionInfo, that._encryptionInfo)
        && Objects.equals(_segmentMetadataZNRecord, that._segmentMetadataZNRecord);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentDownloadURIStr, _sourceDownloadURIStr, _finalSegmentLocationURI,
        _segmentSizeInBytes, _segmentMetadata, _encryptionInfo, _segmentMetadataZNRecord);
  }
}
