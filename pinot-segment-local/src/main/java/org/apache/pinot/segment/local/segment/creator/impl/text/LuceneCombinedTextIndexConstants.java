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
package org.apache.pinot.segment.local.segment.creator.impl.text;

/**
 * Constants for Lucene text index V2 format.
 * This class defines the format structure and constants used by both the writer and reader.
 */
public final class LuceneCombinedTextIndexConstants {
  private LuceneCombinedTextIndexConstants() {
    // Utility class
  }

  /** Magic number for V2 format */
  public static final String MAGIC_NUMBER = "LUCENE_V2";

  /** Version number for V2 format */
  public static final int VERSION = 2;

  /** Magic number length in bytes */
  public static final int MAGIC_NUMBER_LENGTH = MAGIC_NUMBER.length();

  /** Version field size in bytes */
  public static final int VERSION_SIZE = Integer.BYTES;

  /** Total buffer size field size in bytes */
  public static final int TOTAL_SIZE_FIELD_SIZE = Long.BYTES;

  /** File count field size in bytes */
  public static final int FILE_COUNT_FIELD_SIZE = Integer.BYTES;

  /** Reserved field size in bytes */
  public static final int RESERVED_FIELD_SIZE = Integer.BYTES;

  /** File name length field size in bytes */
  public static final int FILE_NAME_LENGTH_FIELD_SIZE = Short.BYTES;

  /** File offset field size in bytes */
  public static final int FILE_OFFSET_FIELD_SIZE = Long.BYTES;

  /** File size field size in bytes */
  public static final int FILE_SIZE_FIELD_SIZE = Long.BYTES;

  /**
   * Calculates the header size in bytes.
   * Header structure: Magic number + Version + Total buffer size + File count + Reserved
   */
  public static int getHeaderSize() {
    return MAGIC_NUMBER_LENGTH + VERSION_SIZE + TOTAL_SIZE_FIELD_SIZE + FILE_COUNT_FIELD_SIZE + RESERVED_FIELD_SIZE;
  }

  /**
   * Calculates the file metadata entry size (excluding variable length filename).
   * Entry structure: File name length + File offset + File size
   */
  public static int getFileMetadataEntrySize() {
    return FILE_NAME_LENGTH_FIELD_SIZE + FILE_OFFSET_FIELD_SIZE + FILE_SIZE_FIELD_SIZE;
  }

  /**
   * Calculates the total file metadata entry size including the filename.
   * Entry structure: File name length + File name (variable length) + File offset + File size
   */
  public static int getFileMetadataEntrySizeWithFilename(String filename) {
    return getFileMetadataEntrySize() + filename.length();
  }

  /** Header field offsets */
  public static final class HeaderOffsets {
    private HeaderOffsets() {
      // Utility class
    }

    /** Magic number starts at offset 0 */
    public static final int MAGIC_NUMBER_OFFSET = 0;

    /** Version starts after magic number */
    public static final int VERSION_OFFSET = MAGIC_NUMBER_OFFSET + MAGIC_NUMBER_LENGTH;

    /** Total buffer size starts after version */
    public static final int TOTAL_SIZE_OFFSET = VERSION_OFFSET + VERSION_SIZE;

    /** File count starts after total buffer size */
    public static final int FILE_COUNT_OFFSET = TOTAL_SIZE_OFFSET + TOTAL_SIZE_FIELD_SIZE;

    /** Reserved field starts after file count */
    public static final int RESERVED_OFFSET = FILE_COUNT_OFFSET + FILE_COUNT_FIELD_SIZE;
  }
}
