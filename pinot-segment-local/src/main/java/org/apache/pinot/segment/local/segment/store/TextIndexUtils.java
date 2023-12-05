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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;


public class TextIndexUtils {
  private TextIndexUtils() {
  }

  static void cleanupTextIndex(File segDir, String column) {
    // Remove the lucene index file and potentially the docId mapping file.
    File luceneIndexFile = new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneIndexFile);
    File luceneMappingFile = new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneMappingFile);
    File luceneV9IndexFile = new File(segDir, column + Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV9IndexFile);
    File luceneV9MappingFile = new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV9MappingFile);

    // Remove the native index file
    File nativeIndexFile = new File(segDir, column + Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(nativeIndexFile);
  }

  static boolean hasTextIndex(File segDir, String column) {
    return new File(segDir, column + Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION).exists();
  }

  public static boolean isFstTypeNative(@Nullable Map<String, String> textIndexProperties) {
    if (textIndexProperties == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : textIndexProperties.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(FieldConfig.TEXT_FST_TYPE)) {
        return entry.getValue().equalsIgnoreCase(FieldConfig.TEXT_NATIVE_FST_LITERAL);
      }
    }
    return false;
  }

  public static FSTType getFSTTypeOfIndex(File indexDir, String column) {
    return SegmentDirectoryPaths.findTextIndexIndexFile(indexDir, column) != null ? FSTType.LUCENE : FSTType.NATIVE;
  }

  @Nonnull
  public static List<String> extractStopWordsInclude(String colName,
      Map<String, Map<String, String>> columnProperties) {
    return extractStopWordsInclude(columnProperties.getOrDefault(colName, null));
  }

  @Nonnull
  public static List<String> extractStopWordsExclude(String colName,
      Map<String, Map<String, String>> columnProperties) {
    return extractStopWordsExclude(columnProperties.getOrDefault(colName, null));
  }

  @Nonnull
  public static List<String> extractStopWordsInclude(Map<String, String> columnProperty) {
    return parseEntryAsString(columnProperty, FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY);
  }

  @Nonnull
  public static List<String> extractStopWordsExclude(Map<String, String> columnProperty) {
    return parseEntryAsString(columnProperty, FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY);
  }

  @Nonnull
  private static List<String> parseEntryAsString(@Nullable Map<String, String> columnProperties,
      String stopWordKey) {
    if (columnProperties == null) {
      return Collections.EMPTY_LIST;
    }
    String includeWords = columnProperties.getOrDefault(stopWordKey, "");
    return Arrays.stream(includeWords.split(FieldConfig.TEXT_INDEX_STOP_WORD_SEPERATOR))
        .map(String::trim).collect(Collectors.toList());
  }

  public static Analyzer getAnalyzerFromClassName(String luceneAnalyzerClass) throws
      ReflectiveOperationException {
    // Support instantiation with default constructor for now unless customized
    return (Analyzer) Class.forName(luceneAnalyzerFQCN).getConstructor().newInstance();
  }

  public static StandardAnalyzer getStandardAnalyzerWithCustomizedStopWords(@Nullable List<String> stopWordsInclude,
      @Nullable List<String> stopWordsExclude) {
    HashSet<String> stopWordSet = LuceneTextIndexCreator.getDefaultEnglishStopWordsSet();
    if (stopWordsInclude != null) {
      stopWordSet.addAll(stopWordsInclude);
    }
    if (stopWordsExclude != null) {
      stopWordsExclude.forEach(stopWordSet::remove);
    }
    return new StandardAnalyzer(new CharArraySet(stopWordSet, true));
  }
}
