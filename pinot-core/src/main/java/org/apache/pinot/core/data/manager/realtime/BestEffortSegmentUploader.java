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
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;


// A segment uploader which do best effort segment upload to a segment store. If a segment upload failed or there is no
// segment store uri configure, it still sets the segment location as SERVER and returns success as result.
public class BestEffortSegmentUploader implements SegmentUploader {
  private Logger _segmentLogger;
  private String _tableNameWithType;
  private String _segmentStoreUriStr;
  public BestEffortSegmentUploader(Logger segmentLogger, String tableNameWithType,
      String segmentStoreUriStr) {
    _segmentLogger = segmentLogger;
    _tableNameWithType = tableNameWithType;
    _segmentStoreUriStr = segmentStoreUriStr;
  }

  @Override
  public UploadStatus segmentUpload(File segmentFile) {
    if (_segmentStoreUriStr == null) {
      return new UploadStatus(true, "SERVER:///");
    }
    try {
      PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
      URI destUri =
          new URI(StringUtil.join(File.separator, _segmentStoreUriStr, _tableNameWithType, segmentFile.getName()));
      if (pinotFS.exists(destUri)) {
        pinotFS.delete(destUri, true);
      }
      pinotFS.copyFromLocalFile(segmentFile, destUri);
      return new UploadStatus(true, destUri.toString());
    } catch (Exception e) {
      _segmentLogger.error("Failed copy segment tar file to segment store {}: {}", segmentFile.getName(), e);
    }
    return new UploadStatus(true, "SERVER:///");
  }
}
