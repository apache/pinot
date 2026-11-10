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
package org.apache.pinot.segment.local.utils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utils methods to manage the TTL watermark for dedup and upsert tables, as both share very similar logic.
public class WatermarkUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(WatermarkUtils.class);

  /// Written after the watermark by the upsert managers that cap the watermark to persist. Older readers only read the
  /// leading double, so the extra bytes don't break a rollback.
  public static final int CAPPED_WATERMARK_VERSION = 1;
  public static final int NO_WATERMARK_VERSION = 0;

  private WatermarkUtils() {
  }

  /// Loads watermark from the file if exists.
  public static double loadWatermark(File watermarkFile, double defaultWatermark) {
    if (watermarkFile.exists()) {
      try {
        byte[] bytes = FileUtils.readFileToByteArray(watermarkFile);
        double watermark = ByteBuffer.wrap(bytes).getDouble();
        LOGGER.info("Loaded watermark: {} from file: {}", watermark, watermarkFile);
        return watermark;
      } catch (Exception e) {
        LOGGER.warn("Failed to load watermark from file: {}, skipping", watermarkFile);
      }
    }
    return defaultWatermark;
  }

  /// Loads the version written after the watermark, or NO_WATERMARK_VERSION if the file is missing or has none.
  public static int loadWatermarkVersion(File watermarkFile) {
    if (watermarkFile.exists()) {
      try {
        byte[] bytes = FileUtils.readFileToByteArray(watermarkFile);
        if (bytes.length >= Double.BYTES + Integer.BYTES) {
          return ByteBuffer.wrap(bytes).getInt(Double.BYTES);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to load watermark version from file: {}, skipping", watermarkFile);
      }
    }
    return NO_WATERMARK_VERSION;
  }

  /// Persists watermark to the file, followed by the version unless it's NO_WATERMARK_VERSION. Pass
  /// CAPPED_WATERMARK_VERSION only when the watermark is capped to keep segments with stale snapshots within TTL.
  public static void persistWatermark(double watermark, File watermarkFile, int version) {
    try {
      if (watermarkFile.exists()) {
        if (!FileUtils.deleteQuietly(watermarkFile)) {
          LOGGER.warn("Cannot delete watermark file: {} to persist watermark: {}, skipping", watermarkFile, watermark);
          return;
        }
      }
      try (OutputStream outputStream = new FileOutputStream(watermarkFile, false);
          DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
        dataOutputStream.writeDouble(watermark);
        if (version != NO_WATERMARK_VERSION) {
          dataOutputStream.writeInt(version);
        }
      }
      LOGGER.info("Persisted watermark: {} to file: {}", watermark, watermarkFile);
    } catch (Exception e) {
      LOGGER.warn("Failed to persist watermark: {} to file: {}, skipping", watermark, watermarkFile);
    }
  }

  /// Deletes the watermark file.
  public static void deleteWatermark(File watermarkFile) {
    if (watermarkFile.exists()) {
      if (!FileUtils.deleteQuietly(watermarkFile)) {
        LOGGER.warn("Cannot delete watermark file: {}, skipping", watermarkFile);
      }
    }
  }
}
