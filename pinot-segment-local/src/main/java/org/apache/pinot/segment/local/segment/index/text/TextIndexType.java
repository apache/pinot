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

package org.apache.pinot.segment.local.segment.index.text;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.NativeTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.TextIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TextIndexType extends AbstractIndexType<TextIndexConfig, TextIndexReader, TextIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<TextIndexConfig> {
  public static final TextIndexType INSTANCE = new TextIndexType();
  private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexType.class);

  private TextIndexType() {
  }

  @Override
  public String getId() {
    return StandardIndexes.TEXT_ID;
  }

  @Override
  public Class<TextIndexConfig> getIndexConfigClass() {
    return TextIndexConfig.class;
  }

  @Override
  public Map<String, TextIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    Map<String, Map<String, String>> allColProps = indexLoadingConfig.getColumnProperties();
    return indexLoadingConfig.getTextIndexColumns().stream().collect(Collectors.toMap(
        Function.identity(),
        colName -> new TextIndexConfigBuilder(indexLoadingConfig.getFSTIndexType())
            .withProperties(allColProps.get(colName))
            .build()
    ));
  }

  @Override
  public TextIndexConfig getDefaultConfig() {
    return TextIndexConfig.DISABLED;
  }

  @Override
  public ColumnConfigDeserializer<TextIndexConfig> getDeserializer() {
    return IndexConfigDeserializer.fromIndexes("text", getIndexConfigClass())
        .withExclusiveAlternative((tableConfig, schema) -> {
          List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
          if (fieldConfigList == null) {
            return Collections.emptyMap();
          }
          // TODO(index-spi): This doesn't feel right, but it is here to keep backward compatibility.
          //  If there are two text indexes in two different columns and one explicitly specifies LUCENE and the but
          //  the other specifies native, both are created as native. No error is thrown and no log is shown.
          FSTType fstType = FSTType.LUCENE;
          for (FieldConfig fieldConfig : fieldConfigList) {
            if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.TEXT)) {
              Map<String, String> properties = fieldConfig.getProperties();
              if (TextIndexUtils.isFstTypeNative(properties)) {
                fstType = FSTType.NATIVE;
              }
            }
          }
          Map<String, TextIndexConfig> result = new HashMap<>();
          for (FieldConfig fieldConfig : fieldConfigList) {
            if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.TEXT)) {
              String column = fieldConfig.getName();
              Map<String, String> properties = fieldConfig.getProperties();
              // TODO(index-spi): This is how the index type should be detected
              FSTType actualFstType = TextIndexUtils.isFstTypeNative(properties) ? FSTType.NATIVE : FSTType.LUCENE;
              if (actualFstType != fstType) {
                LOGGER.warn("Column {} text index is specified as {}, but {} will be used to keep backward guarantees",
                    column, fstType, actualFstType);
              }
              result.put(column, new TextIndexConfigBuilder(fstType).withProperties(properties).build());
            }
          }
          return result;
        });
  }

  @Override
  public TextIndexCreator createIndexCreator(IndexCreationContext context, TextIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "Text index is currently only supported on STRING type columns");
    if (indexConfig.getFstType() == FSTType.NATIVE) {
      return new NativeTextIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(),
          indexConfig.getRawValueForTextIndex(), context.getFieldSpec());
    } else {
      return new LuceneTextIndexCreator(context, indexConfig);
    }
  }

  @Override
  public String toString() {
    return getId();
  }

  @Override
  public boolean storedAsBuffer() {
    return false;
  }

  @Override
  public IndexReaderFactory<TextIndexReader> getReaderFactory() {
    return new IndexReaderFactory<TextIndexReader>() {
      @Nullable
      @Override
      public TextIndexReader createIndexReader(SegmentDirectory.Reader segmentReader,
          FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata)
          throws IndexReaderConstraintException {
        if (fieldIndexConfigs == null) {
          return null;
        }
        if (metadata.getDataType() != FieldSpec.DataType.STRING) {
          throw new IndexReaderConstraintException(metadata.getColumnName(), TextIndexType.INSTANCE,
              "Text index is currently only supported on STRING type columns");
        }
        File segmentDir = segmentReader.toSegmentDirectory().getPath().toFile();
        FSTType textIndexFSTType = TextIndexUtils.getFSTTypeOfIndex(segmentDir, metadata.getColumnName());
        if (textIndexFSTType == FSTType.NATIVE) {
          // TODO: Support loading native text index from a PinotDataBuffer
          return new NativeTextIndexReader(metadata.getColumnName(), segmentDir);
        }
        TextIndexConfig indexConfig = fieldIndexConfigs.getConfig(TextIndexType.INSTANCE);
        if (!indexConfig.isEnabled()) {
          return null;
        }
        return new LuceneTextIndexReader(metadata.getColumnName(), segmentDir, metadata.getTotalDocs(), indexConfig);
      }
    };
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new TextIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION;
  }
}
