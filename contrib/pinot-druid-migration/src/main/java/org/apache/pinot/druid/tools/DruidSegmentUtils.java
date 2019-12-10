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
package org.apache.pinot.druid.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.utils.CompressionUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;


/**
 * DruidSegmentUtils provides helpful methods for dealing with Druid segments.
 */
public class DruidSegmentUtils {
  public static QueryableIndex createIndex(File druidSegment)
      throws IOException {
    ColumnConfig config = new DruidProcessingConfig() {
      @Override
      public String getFormatString() {
        return "processing-%s";
      }

      @Override
      public int intermediateComputeSizeBytes() {
        return 100 * 1024 * 1024;
      }

      @Override
      public int getNumThreads() {
        return 1;
      }

      @Override
      public int columnCacheSizeBytes() {
        return 25 * 1024 * 1024;
      }
    };

    ObjectMapper mapper = new DefaultObjectMapper();
    final IndexIO indexIO = new IndexIO(mapper, config);
    return indexIO.loadIndex(druidSegment);
  }

  public static File uncompressSegmentFile(File druidSegment) {
    File uncompressedInput = new File(druidSegment.getParentFile().getPath(), "uncompressedInput");
    uncompressedInput.mkdir();

    if (druidSegment.getName().endsWith(".zip")) {
      try {
        CompressionUtils.unzip(druidSegment, uncompressedInput);
        return new File(uncompressedInput.getPath());
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else if (druidSegment.getName().endsWith("tar.gz")) {
      try {
        return TarGzCompressionUtils.unTar(druidSegment, uncompressedInput).get(0);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    throw new IllegalArgumentException("Unsupported file type for Druid segment.");
  }
}
