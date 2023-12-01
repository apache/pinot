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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;


public class TextIndexConfigBuilder extends TextIndexConfig.AbstractBuilder {
  public TextIndexConfigBuilder(@Nullable FSTType fstType) {
    super(fstType);
  }

  public TextIndexConfigBuilder(TextIndexConfig other) {
    super(other);
  }

  @Override
  public TextIndexConfig.AbstractBuilder withProperties(@Nullable Map<String, String> textIndexProperties) {
    if (textIndexProperties != null) {
      if (Boolean.parseBoolean(textIndexProperties.get(FieldConfig.TEXT_INDEX_NO_RAW_DATA))) {
        _rawValueForTextIndex = textIndexProperties.get(FieldConfig.TEXT_INDEX_RAW_VALUE);
        if (_rawValueForTextIndex == null) {
          _rawValueForTextIndex = FieldConfig.TEXT_INDEX_DEFAULT_RAW_VALUE;
        }
      }
      _enableQueryCache = Boolean.parseBoolean(textIndexProperties.get(FieldConfig.TEXT_INDEX_ENABLE_QUERY_CACHE));
      _useANDForMultiTermQueries = Boolean.parseBoolean(
          textIndexProperties.get(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES));
      _stopWordsInclude = TextIndexUtils.extractStopWordsInclude(textIndexProperties);
      _stopWordsExclude = TextIndexUtils.extractStopWordsExclude(textIndexProperties);

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_USE_COMPOUND_FILE) != null) {
        _luceneUseCompoundFile =
            Boolean.parseBoolean(textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_USE_COMPOUND_FILE));
      }

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB) != null) {
        _luceneMaxBufferSizeMB =
            Integer.parseInt(textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB));
      }

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS) != null) {
        _luceneAnalyzerClass = textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS);
      }

      for (Map.Entry<String, String> entry : textIndexProperties.entrySet()) {
        if (entry.getKey().equalsIgnoreCase(FieldConfig.TEXT_FST_TYPE)) {
          _fstType = FSTType.NATIVE;
        } else {
          _fstType = FSTType.LUCENE;
        }
      }
    }
    return this;
  }
}
