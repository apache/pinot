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
import org.apache.pinot.core.segment.processing.mapper.MapperOutputReader;
import org.apache.pinot.spi.data.FieldSpec;


public class MapperOutputReaderFactory {
  // Enum for various GenericRowFileReaders to be added.
  public enum MapperOutputReaderType {
    GenericRowFileReader;
    // Add more Readers here.

    private static final Map<String, MapperOutputReaderType> VALUE_MAP = new HashMap<>();

    public static MapperOutputReaderType fromString(String name) {
      MapperOutputReaderType readerType = VALUE_MAP.get(name.toLowerCase());

      if (readerType == null) {
        throw new IllegalArgumentException("No enum constant for: " + name);
      }
      return readerType;
    }

    static {
      for (MapperOutputReaderType mapperOutputReaderType : MapperOutputReaderType.values()) {
        VALUE_MAP.put(mapperOutputReaderType.name().toLowerCase(), mapperOutputReaderType);
      }
    }
  }

  private MapperOutputReaderFactory() {
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

  public static MapperOutputReader getMapperOutputReader(String readerName, Map<String, Object> params)
      throws IOException {
    MapperOutputReaderType readerType = MapperOutputReaderType.fromString(readerName);
    switch (readerType) {
      case GenericRowFileReader:
        return createGenericRowFileReader(params);
      default:
        throw new IllegalArgumentException("Unsupported reader type: " + readerType);
    }
  }
}
