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
import java.util.Objects;


/**
 * Data object used to capture the segment metadata files information along with the segment download URI while
 * uploading segments in the mode.
 *
 * <ol>
 *   <li>segmentDownloadURI – The segment download URI</li>
 *   <li>segmentCreationMeta – The creation.meta file within the segment metadata tar file.</li>
 *   <li>segmentMetadataProperties – The metadata.properties file within the segment metadata tar file.</li>
 * </ol>
 */
public class SegmentMetadataInfo {
  private String _segmentDownloadURI;
  private File _segmentCreationMetaFile;
  private File _segmentMetadataPropertiesFile;

  public String getSegmentDownloadURI() {
    return _segmentDownloadURI;
  }

  public void setSegmentDownloadURI(String segmentDownloadURI) {
    _segmentDownloadURI = segmentDownloadURI;
  }

  public File getSegmentCreationMetaFile() {
    return _segmentCreationMetaFile;
  }

  public File getSegmentMetadataPropertiesFile() {
    return _segmentMetadataPropertiesFile;
  }

  public void setSegmentCreationMetaFile(File file) {
    _segmentCreationMetaFile = file;
  }

  public void setSegmentMetadataPropertiesFile(File file) {
    _segmentMetadataPropertiesFile = file;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentMetadataInfo that = (SegmentMetadataInfo) o;
    return Objects.equals(_segmentDownloadURI, that._segmentDownloadURI) && Objects.equals(_segmentCreationMetaFile,
        that._segmentCreationMetaFile) && Objects.equals(_segmentMetadataPropertiesFile,
        that._segmentMetadataPropertiesFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentDownloadURI, _segmentCreationMetaFile, _segmentMetadataPropertiesFile);
  }
}
