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
package org.apache.pinot.segment.spi.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class MergedColumnConfigDeserializer<C> implements ColumnConfigDeserializer<C> {

  private final OnConflict _onConflict;
  private final Iterable<ColumnConfigDeserializer<C>> _deserializers;

  @SafeVarargs
  public MergedColumnConfigDeserializer(OnConflict onConflict, ColumnConfigDeserializer<C>... deserializers) {
    this(onConflict, Arrays.asList(deserializers));
  }

  public MergedColumnConfigDeserializer(OnConflict onConflict, Iterable<ColumnConfigDeserializer<C>> deserializers) {
    _onConflict = onConflict;
    _deserializers = deserializers;
  }

  @Override
  public Map<String, C> deserialize(TableConfig tableConfig, Schema schema) {
    Map<String, C> result = new HashMap<>();
    for (ColumnConfigDeserializer<C> deserializer : _deserializers) {
      Map<String, C> partialResult = deserializer.deserialize(tableConfig, schema);

      for (Map.Entry<String, C> entry : partialResult.entrySet()) {
        String column = entry.getKey();
        if (result.containsKey(column)) {
          _onConflict.merge(result, column, entry.getValue());
        } else {
          result.put(column, entry.getValue());
        }
      }
    }
    return result;
  }

  public static class ConfigDeclaredTwiceException extends RuntimeException {
    private final String _column;

    public ConfigDeclaredTwiceException(String column) {
      super("Configuration is declared in two different ways for column " + column);
      _column = column;
    }

    public String getColumn() {
      return _column;
    }
  }

  public static enum OnConflict {
    FAIL {
      @Override
      public <C> void merge(Map<String, C> map, String column, C newValue) {
        throw new ConfigDeclaredTwiceException(column);
      }
    },
    PICK_FIRST {
      @Override
      public <C> void merge(Map<String, C> map, String column, C newValue) {
      }
    },
    PICK_LAST {
      @Override
      public <C> void merge(Map<String, C> map, String column, C newValue) {
        map.put(column, newValue);
      }
    };

    public abstract <C> void merge(Map<String, C> map, String column, C newValue);
  }
}
