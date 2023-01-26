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

package org.apache.pinot.segment.local.segment.index.nullvalue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.index.readers.NullValueVectorReaderImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.EmptyIndexConf;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class NullValueIndexType implements IndexType<EmptyIndexConf, NullValueVectorReader, NullValueVectorCreator> {

  public static final NullValueIndexType INSTANCE = new NullValueIndexType();

  private NullValueIndexType() {
  }

  @Override
  public String getId() {
    return "nullable";
  }

  @Override
  public String getIndexName() {
    return "nullvalue_vector";
  }

  @Override
  public Class<EmptyIndexConf> getIndexConfigClass() {
    return EmptyIndexConf.class;
  }

  @Override
  public IndexDeclaration<EmptyIndexConf> deserializeSpreadConf(TableConfig tableConfig, Schema schema, String column) {
    return tableConfig.getIndexingConfig().isNullHandlingEnabled()
        ? IndexDeclaration.declared(EmptyIndexConf.INSTANCE)
        : IndexDeclaration.notDeclared(this);
  }

  @Override
  public NullValueVectorCreator createIndexCreator(IndexCreationContext context, EmptyIndexConf indexConfig)
      throws Exception {
    return new NullValueVectorCreator(context.getIndexDir(), context.getFieldSpec().getName());
  }

  public NullValueVectorCreator createIndexCreator(File indexDir, String columnName) {
    return new NullValueVectorCreator(indexDir, columnName);
  }

  @Override
  public IndexReaderFactory<NullValueVectorReader> getReaderFactory() {
    return new IndexReaderFactory<>() {
      @Nullable
      @Override
      public NullValueVectorReader read(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
          ColumnMetadata metadata, File segmentDir)
          throws IOException {
        // For historical and test reasons, NullValueIndexType doesn't really care about its config
        // if there is a buffer for this index, it is read even if the config explicitly ask to disable it.
        if (!segmentReader.hasIndexFor(metadata.getColumnName(), NullValueIndexType.INSTANCE)) {
          return null;
        }
        PinotDataBuffer buffer = segmentReader.getIndexFor(metadata.getColumnName(), NullValueIndexType.INSTANCE);
        return new NullValueVectorReaderImpl(buffer);
      }
    };
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new IndexHandler() {
      @Override
      public void updateIndices(SegmentDirectory.Writer segmentWriter) {
      }

      @Override
      public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
        return false;
      }

      @Override
      public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter) {
      }
    };
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION;
  }

  @Override
  public String toString() {
    return getId();
  }
}
