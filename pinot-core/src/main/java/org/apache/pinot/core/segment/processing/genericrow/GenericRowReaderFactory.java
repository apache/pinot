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
package org.apache.pinot.core.segment.processing.genericrow;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.FieldSpec;


public class GenericRowReaderFactory {
  // Enum for various GenericRowFileReaders to be added.
  public enum GenericRowReaderType {
    ArrowFileGenericRowReader, GenericRowFileReader;
    // Add more Readers here.

    private static final Map<String, GenericRowReaderType> VALUE_MAP = new HashMap<>();

    public static GenericRowReaderType fromString(String name) {
      GenericRowReaderType readerType = VALUE_MAP.get(name.toLowerCase());

      if (readerType == null) {
        throw new IllegalArgumentException("No enum constant for: " + name);
      }
      return readerType;
    }

    static {
      for (GenericRowReaderType genericRowReaderType : GenericRowReaderType.values()) {
        VALUE_MAP.put(genericRowReaderType.name().toLowerCase(), genericRowReaderType);
      }
    }
  }

  private GenericRowReaderFactory() {
  }

  private static ArrowFileGenericRowReader createArrowFileGenericRowReader(Map<String, Object> params) {
    List<File> dataFiles = (List<File>) params.get("dataFiles");
    List<File> sortColumnFiles = (List<File>) params.get("sortColumnFiles");
    List<Integer> chunkRowCounts = (List<Integer>) params.get("chunkRowCounts");
    Schema arrowSchema = (Schema) params.get("arrowSchema");
    int totalNumRows = (int) params.get("totalNumRows");

    return new ArrowFileGenericRowReader(dataFiles, sortColumnFiles, chunkRowCounts, arrowSchema, totalNumRows);
  }

  private static GenericRowFileReader createGenericRowFileReader(Map<String, Object> params)
      throws IOException {
    File offsetFile = (File) params.get("offsetFile");
    File dataFile = (File) params.get("dataFile");
    List<FieldSpec> fieldSpecs = (List<FieldSpec>) params.get("fieldSpecs");
    boolean includeNullFields = (boolean) params.get("includeNullFields");
    int numSortFields = (int) params.get("numSortFields");

    return new GenericRowFileReader(offsetFile, dataFile, fieldSpecs, includeNullFields, numSortFields);
  }

  public static GenericRowReader getGenericRowReader(String readerName, Map<String, Object> params)
      throws IOException {
    GenericRowReaderType readerType = GenericRowReaderType.fromString(readerName);
    switch (readerType) {
      case ArrowFileGenericRowReader:
        return createArrowFileGenericRowReader(params);
      case GenericRowFileReader:
        return createGenericRowFileReader(params);
      default:
        throw new IllegalArgumentException("Unsupported reader type: " + readerType);
    }
  }
}
