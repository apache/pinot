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
import org.apache.pinot.segment.spi.utils.CsvParser;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;


public class TextIndexConfigBuilder extends TextIndexConfig.AbstractBuilder {
  public TextIndexConfigBuilder() {
    super((FSTType) null);
  }

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
      _enablePrefixSuffixMatchingInPhraseQueries =
          Boolean.parseBoolean(textIndexProperties.get(FieldConfig.TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES));

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

      // Note that we cannot depend on jackson's default behavior to automatically coerce the comma delimited args to
      // List<String>. This is because the args may contain comma and other special characters such as space. Therefore,
      // we use our own csv parser to parse the values directly.
      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS) != null) {
        _luceneAnalyzerClassArgs = CsvParser.parse(
                textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS), true, false);
      }

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES) != null) {
        _luceneAnalyzerClassArgTypes = CsvParser.parse(
                textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES), false, true);
      }

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS) != null) {
        _luceneQueryParserClass = textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS);
      }

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX) != null) {
        _reuseMutableIndex =
            Boolean.parseBoolean(textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX));
      }

      if (textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_NRT_CACHING_DIRECTORY_BUFFER_SIZE) != null) {
        _luceneNRTCachingDirectoryMaxBufferSizeMB =
            Integer.parseInt(textIndexProperties.get(FieldConfig.TEXT_INDEX_LUCENE_NRT_CACHING_DIRECTORY_BUFFER_SIZE));
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
