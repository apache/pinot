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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.utils.CsvParser;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;


public class TextIndexConfig extends IndexConfig {
  private static final int LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB = 500;
  private static final boolean LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE = true;
  private static final boolean LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH = false;
  private static final boolean LUCENE_INDEX_REUSE_MUTABLE_INDEX = false;
  private static final int LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB = 0;
  private static final boolean LUCENE_INDEX_DEFAULT_USE_AND_FOR_MULTI_TERM_QUERIES = false;
  private static final boolean LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY = false;
  private static final DocIdTranslatorMode LUCENE_TRANSLATOR_MODE = null;

  // keep in sync with constructor!
  private static final List<String> PROPERTY_NAMES = List.of(
      "disabled", "fst", "rawValue", "queryCache", "useANDForMultiTermQueries", "stopWordsInclude", "stopWordsExclude",
      "luceneUseCompoundFile", "luceneMaxBufferSizeMB", "luceneAnalyzerClass", "luceneAnalyzerClassArgs",
      "luceneAnalyzerClassArgTypes", "luceneQueryParserClass", "enablePrefixSuffixMatchingInPhraseQueries",
      "reuseMutableIndex", "luceneNRTCachingDirectoryMaxBufferSizeMB", "useLogByteSizeMergePolicy",
      "docIdTranslatorMode"
  );

  public static final TextIndexConfig DISABLED =
      new TextIndexConfig(true, null, null, false, false, Collections.emptyList(), Collections.emptyList(), false,
          LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB, null, null, null, null, false, false, 0, false,
          null);

  private final FSTType _fstType;
  @Nullable
  private final Object _rawValueForTextIndex;
  private final boolean _enableQueryCache;
  private final boolean _useANDForMultiTermQueries;
  private final List<String> _stopWordsInclude;
  private final List<String> _stopWordsExclude;
  private final boolean _luceneUseCompoundFile;
  private final int _luceneMaxBufferSizeMB;
  private final String _luceneAnalyzerClass;
  private final List<String> _luceneAnalyzerClassArgs;
  private final List<String> _luceneAnalyzerClassArgTypes;
  private final String _luceneQueryParserClass;
  private final boolean _enablePrefixSuffixMatchingInPhraseQueries;
  private final boolean _reuseMutableIndex;
  private final int _luceneNRTCachingDirectoryMaxBufferSizeMB;
  private final boolean _useLogByteSizeMergePolicy;
  private final DocIdTranslatorMode _docIdTranslatorMode;

  public enum DocIdTranslatorMode {
    // build and keep mapping
    Default,
    // build mapping and check if it can be dropped
    TryOptimize,
    // mapping is not necessary, use lucene ids
    Skip;

    static DocIdTranslatorMode of(String mode) {
      if (mode == null) {
        return null;
      }
      for (DocIdTranslatorMode value : values()) {
        if (value.name().equalsIgnoreCase(mode)) {
          return value;
        }
      }

      return null;
    }
  }

  @JsonCreator
  public TextIndexConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("fst") FSTType fstType,
      @JsonProperty("rawValue") @Nullable Object rawValueForTextIndex,
      @JsonProperty("queryCache") boolean enableQueryCache,
      @JsonProperty("useANDForMultiTermQueries") boolean useANDForMultiTermQueries,
      @JsonProperty("stopWordsInclude") List<String> stopWordsInclude,
      @JsonProperty("stopWordsExclude") List<String> stopWordsExclude,
      @JsonProperty("luceneUseCompoundFile") Boolean luceneUseCompoundFile,
      @JsonProperty("luceneMaxBufferSizeMB") Integer luceneMaxBufferSizeMB,
      @JsonProperty("luceneAnalyzerClass") String luceneAnalyzerClass,
      @JsonProperty("luceneAnalyzerClassArgs") String luceneAnalyzerClassArgs,
      @JsonProperty("luceneAnalyzerClassArgTypes") String luceneAnalyzerClassArgTypes,
      @JsonProperty("luceneQueryParserClass") String luceneQueryParserClass,
      @JsonProperty("enablePrefixSuffixMatchingInPhraseQueries") Boolean enablePrefixSuffixMatchingInPhraseQueries,
      @JsonProperty("reuseMutableIndex") Boolean reuseMutableIndex,
      @JsonProperty("luceneNRTCachingDirectoryMaxBufferSizeMB") Integer luceneNRTCachingDirectoryMaxBufferSizeMB,
      @JsonProperty("useLogByteSizeMergePolicy") Boolean useLogByteSizeMergePolicy,
      @JsonProperty("docIdTranslatorMode") DocIdTranslatorMode docIdTranslatorMode) {
    super(disabled);
    _fstType = fstType;
    _rawValueForTextIndex = rawValueForTextIndex;
    _enableQueryCache = enableQueryCache;
    _useANDForMultiTermQueries = useANDForMultiTermQueries;
    _stopWordsInclude = stopWordsInclude;
    _stopWordsExclude = stopWordsExclude;
    _luceneUseCompoundFile =
        luceneUseCompoundFile == null ? LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE : luceneUseCompoundFile;
    _luceneMaxBufferSizeMB =
        luceneMaxBufferSizeMB == null ? LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB : luceneMaxBufferSizeMB;
    _luceneAnalyzerClass = (luceneAnalyzerClass == null || luceneAnalyzerClass.isEmpty())
        ? FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS : luceneAnalyzerClass;

    // Note that we cannot depend on jackson's default behavior to automatically coerce the comma delimited args to
    // List<String>. This is because the args may contain comma and other special characters such as space. Therefore,
    // we use our own csv parser to parse the values directly.
    _luceneAnalyzerClassArgs = CsvParser.parse(luceneAnalyzerClassArgs, true, false);
    _luceneAnalyzerClassArgTypes = CsvParser.parse(luceneAnalyzerClassArgTypes, false, true);
    _luceneQueryParserClass = luceneQueryParserClass == null
            ? FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS : luceneQueryParserClass;
    _enablePrefixSuffixMatchingInPhraseQueries =
        enablePrefixSuffixMatchingInPhraseQueries == null ? LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH
            : enablePrefixSuffixMatchingInPhraseQueries;
    _reuseMutableIndex = reuseMutableIndex == null ? LUCENE_INDEX_REUSE_MUTABLE_INDEX : reuseMutableIndex;
    _luceneNRTCachingDirectoryMaxBufferSizeMB =
        luceneNRTCachingDirectoryMaxBufferSizeMB == null ? LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB
            : luceneNRTCachingDirectoryMaxBufferSizeMB;
    _useLogByteSizeMergePolicy = useLogByteSizeMergePolicy == null ? LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY
        : useLogByteSizeMergePolicy;
    _docIdTranslatorMode = docIdTranslatorMode == null ? LUCENE_TRANSLATOR_MODE : docIdTranslatorMode;
  }

  public FSTType getFstType() {
    return _fstType;
  }

  @Nullable
  public Object getRawValueForTextIndex() {
    return _rawValueForTextIndex;
  }

  /**
   * Whether Lucene query result cache should be enabled.
   *
   * While it helps a lot with performance for repeated queries, on the downside it causes heap issues.
   */
  public boolean isEnableQueryCache() {
    return _enableQueryCache;
  }

  public boolean isUseANDForMultiTermQueries() {
    return _useANDForMultiTermQueries;
  }

  public List<String> getStopWordsInclude() {
    return _stopWordsInclude;
  }

  public List<String> getStopWordsExclude() {
    return _stopWordsExclude;
  }

  /**
   * Whether Lucene IndexWriter uses compound file format. Improves indexing speed but may cause file descriptor issues
   */
  public boolean isLuceneUseCompoundFile() {
    return _luceneUseCompoundFile;
  }

  /**
   * Lucene buffer size. Helps with indexing speed but may cause heap issues
   */
  public int getLuceneMaxBufferSizeMB() {
    return _luceneMaxBufferSizeMB;
  }

  /**
   * Lucene analyzer fully qualified class name specifying which analyzer class to use for indexing
   */
  public String getLuceneAnalyzerClass() {
    return _luceneAnalyzerClass;
  }

  /**
   * Lucene analyzer arguments in String type. At runtime, the string representation are best-effort coerced into the
   * proper type with the fully-qualified value type specified in luceneAnalyzerClassArgTypes
   */
  public List<String> getLuceneAnalyzerClassArgs() {
    return _luceneAnalyzerClassArgs;
  }

  /**
   * Lucene analyzer fully qualified argument value types for each argument. At runtime, the values specified in the
   * luceneAnalyserClassArgs (string representation) are best-effort coerced into the specified value type.
   */
  public List<String> getLuceneAnalyzerClassArgTypes() {
    return _luceneAnalyzerClassArgTypes;
  }

  /**
   * Lucene query parser fully qualified class name specifying which lucene query parser class to use for query parsing
   */
  public String getLuceneQueryParserClass() {
    return _luceneQueryParserClass;
  }

  /**
   *  Whether to enable prefix and suffix wildcard term matching (i.e., .*value for prefix and value.* for suffix
   *  term matching) in a phrase query. By default, Pinot today treats .* in a phrase query like ".*value str1 value.*"
   *  as literal. If this flag is enabled, .*value will be treated as suffix matching and value.* will be treated as
   *  prefix matching.
   */
  public boolean isEnablePrefixSuffixMatchingInPhraseQueries() {
    return _enablePrefixSuffixMatchingInPhraseQueries;
  }

  public boolean isReuseMutableIndex() {
    return _reuseMutableIndex;
  }

  public boolean isUseLogByteSizeMergePolicy() {
    return _useLogByteSizeMergePolicy;
  }

  public DocIdTranslatorMode getDocIdTranslatorMode() {
    return _docIdTranslatorMode;
  }

  public int getLuceneNRTCachingDirectoryMaxBufferSizeMB() {
    return _luceneNRTCachingDirectoryMaxBufferSizeMB;
  }

  public static abstract class AbstractBuilder {
    @Nullable
    protected FSTType _fstType;
    @Nullable
    protected Object _rawValueForTextIndex;
    protected boolean _enableQueryCache = false;
    protected boolean _useANDForMultiTermQueries = LUCENE_INDEX_DEFAULT_USE_AND_FOR_MULTI_TERM_QUERIES;
    protected List<String> _stopWordsInclude = new ArrayList<>();
    protected List<String> _stopWordsExclude = new ArrayList<>();
    protected boolean _luceneUseCompoundFile = LUCENE_INDEX_DEFAULT_USE_COMPOUND_FILE;
    protected int _luceneMaxBufferSizeMB = LUCENE_INDEX_DEFAULT_MAX_BUFFER_SIZE_MB;
    protected String _luceneAnalyzerClass = FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS;
    protected List<String> _luceneAnalyzerClassArgs = new ArrayList<>();
    protected List<String> _luceneAnalyzerClassArgTypes = new ArrayList<>();
    protected String _luceneQueryParserClass = FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS;
    protected boolean _enablePrefixSuffixMatchingInPhraseQueries =
        LUCENE_INDEX_ENABLE_PREFIX_SUFFIX_MATCH_IN_PHRASE_SEARCH;
    protected boolean _reuseMutableIndex = LUCENE_INDEX_REUSE_MUTABLE_INDEX;
    protected int _luceneNRTCachingDirectoryMaxBufferSizeMB = LUCENE_INDEX_NRT_CACHING_DIRECTORY_MAX_BUFFER_SIZE_MB;
    protected boolean _useLogByteSizeMergePolicy = LUCENE_USE_LOG_BYTE_SIZE_MERGE_POLICY;
    @Nullable
    protected DocIdTranslatorMode _docIdTranslatorMode = LUCENE_TRANSLATOR_MODE;

    public AbstractBuilder(@Nullable FSTType fstType) {
      _fstType = fstType;
    }

    public AbstractBuilder(TextIndexConfig other) {
      _fstType = other._fstType;
      _enableQueryCache = other._enableQueryCache;
      _useANDForMultiTermQueries = other._useANDForMultiTermQueries;
      _stopWordsInclude =
          other._stopWordsInclude == null ? new ArrayList<>() : new ArrayList<>(other._stopWordsInclude);
      _stopWordsExclude =
          other._stopWordsExclude == null ? new ArrayList<>() : new ArrayList<>(other._stopWordsExclude);
      _luceneUseCompoundFile = other._luceneUseCompoundFile;
      _luceneMaxBufferSizeMB = other._luceneMaxBufferSizeMB;
      _luceneAnalyzerClass = other._luceneAnalyzerClass;
      _luceneAnalyzerClassArgs = other._luceneAnalyzerClassArgs;
      _luceneAnalyzerClassArgTypes = other._luceneAnalyzerClassArgTypes;
      _luceneQueryParserClass = other._luceneQueryParserClass;
      _enablePrefixSuffixMatchingInPhraseQueries = other._enablePrefixSuffixMatchingInPhraseQueries;
      _reuseMutableIndex = other._reuseMutableIndex;
      _luceneNRTCachingDirectoryMaxBufferSizeMB = other._luceneNRTCachingDirectoryMaxBufferSizeMB;
      _useLogByteSizeMergePolicy = other._useLogByteSizeMergePolicy;
      _docIdTranslatorMode = other._docIdTranslatorMode;
    }

    public TextIndexConfig build() {
      return new TextIndexConfig(false, _fstType, _rawValueForTextIndex, _enableQueryCache, _useANDForMultiTermQueries,
          _stopWordsInclude, _stopWordsExclude, _luceneUseCompoundFile, _luceneMaxBufferSizeMB, _luceneAnalyzerClass,
          CsvParser.serialize(_luceneAnalyzerClassArgs, true, false),
          CsvParser.serialize(_luceneAnalyzerClassArgTypes, true, false),
          _luceneQueryParserClass, _enablePrefixSuffixMatchingInPhraseQueries, _reuseMutableIndex,
          _luceneNRTCachingDirectoryMaxBufferSizeMB, _useLogByteSizeMergePolicy,
          _docIdTranslatorMode);
    }

    public abstract AbstractBuilder withProperties(@Nullable Map<String, String> textIndexProperties);

    public AbstractBuilder withRawValueForTextIndex(@Nullable Object rawValueForTextIndex) {
      _rawValueForTextIndex = rawValueForTextIndex;
      return this;
    }

    public AbstractBuilder withUseANDForMultiTermQueries(boolean useANDForMultiTermQueries) {
      _useANDForMultiTermQueries = useANDForMultiTermQueries;
      return this;
    }

    public AbstractBuilder withStopWordsInclude(List<String> stopWordsInclude) {
      _stopWordsInclude = stopWordsInclude;
      return this;
    }

    public AbstractBuilder withStopWordsExclude(List<String> stopWordsExclude) {
      _stopWordsExclude = stopWordsExclude;
      return this;
    }

    public AbstractBuilder withLuceneUseCompoundFile(boolean useCompoundFile) {
      _luceneUseCompoundFile = useCompoundFile;
      return this;
    }

    public AbstractBuilder withLuceneMaxBufferSizeMB(int maxBufferSizeMB) {
      _luceneMaxBufferSizeMB = maxBufferSizeMB;
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClass(String luceneAnalyzerClass) {
      _luceneAnalyzerClass = luceneAnalyzerClass;
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgs(String luceneAnalyzerClassArgs) {
      _luceneAnalyzerClassArgs = CsvParser.parse(luceneAnalyzerClassArgs, true, false);
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgs(List<String> luceneAnalyzerClassArgs) {
      _luceneAnalyzerClassArgs = luceneAnalyzerClassArgs;
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgTypes(String luceneAnalyzerClassArgTypes) {
      _luceneAnalyzerClassArgTypes = CsvParser.parse(luceneAnalyzerClassArgTypes, false, true);
      return this;
    }

    public AbstractBuilder withLuceneAnalyzerClassArgTypes(List<String> luceneAnalyzerClassArgTypes) {
      _luceneAnalyzerClassArgTypes = luceneAnalyzerClassArgTypes;
      return this;
    }

    public AbstractBuilder withLuceneQueryParserClass(String luceneQueryParserClass) {
      _luceneQueryParserClass = luceneQueryParserClass;
      return this;
    }

    public AbstractBuilder withEnablePrefixSuffixMatchingInPhraseQueries(
        boolean enablePrefixSuffixMatchingInPhraseQueries) {
      _enablePrefixSuffixMatchingInPhraseQueries = enablePrefixSuffixMatchingInPhraseQueries;
      return this;
    }

    public AbstractBuilder withReuseMutableIndex(boolean reuseMutableIndex) {
      _reuseMutableIndex = reuseMutableIndex;
      return this;
    }

    public AbstractBuilder withLuceneNRTCachingDirectoryMaxBufferSizeMB(int luceneNRTCachingDirectoryMaxBufferSizeMB) {
      _luceneNRTCachingDirectoryMaxBufferSizeMB = luceneNRTCachingDirectoryMaxBufferSizeMB;
      return this;
    }

    public AbstractBuilder withUseLogByteSizeMergePolicy(boolean useLogByteSizeMergePolicy) {
      _useLogByteSizeMergePolicy = useLogByteSizeMergePolicy;
      return this;
    }

    public AbstractBuilder withDocIdTranslatorMode(String mode) {
      _docIdTranslatorMode = DocIdTranslatorMode.of(mode);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TextIndexConfig that = (TextIndexConfig) o;
    return _enableQueryCache == that._enableQueryCache
        && _useANDForMultiTermQueries == that._useANDForMultiTermQueries
        && _luceneUseCompoundFile == that._luceneUseCompoundFile
        && _luceneMaxBufferSizeMB == that._luceneMaxBufferSizeMB
        && _enablePrefixSuffixMatchingInPhraseQueries == that._enablePrefixSuffixMatchingInPhraseQueries
        && _reuseMutableIndex == that._reuseMutableIndex
        && _useLogByteSizeMergePolicy == that._useLogByteSizeMergePolicy
        && _docIdTranslatorMode == that._docIdTranslatorMode
        && _luceneNRTCachingDirectoryMaxBufferSizeMB == that._luceneNRTCachingDirectoryMaxBufferSizeMB
        && _fstType == that._fstType
        && Objects.equals(_rawValueForTextIndex, that._rawValueForTextIndex)
        && Objects.equals(_stopWordsInclude, that._stopWordsInclude)
        && Objects.equals(_stopWordsExclude, that._stopWordsExclude)
        && Objects.equals(_luceneAnalyzerClass, that._luceneAnalyzerClass)
        && Objects.equals(_luceneAnalyzerClassArgs, that._luceneAnalyzerClassArgs)
        && Objects.equals(_luceneAnalyzerClassArgTypes, that._luceneAnalyzerClassArgTypes)
        && Objects.equals(_luceneQueryParserClass, that._luceneQueryParserClass);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _fstType, _rawValueForTextIndex, _enableQueryCache,
        _useANDForMultiTermQueries, _stopWordsInclude, _stopWordsExclude, _luceneUseCompoundFile,
        _luceneMaxBufferSizeMB, _luceneAnalyzerClass, _luceneAnalyzerClassArgs, _luceneAnalyzerClassArgTypes,
        _luceneQueryParserClass, _enablePrefixSuffixMatchingInPhraseQueries, _reuseMutableIndex,
        _luceneNRTCachingDirectoryMaxBufferSizeMB, _useLogByteSizeMergePolicy, _docIdTranslatorMode);
  }

  public static boolean isProperty(String prop) {
    return PROPERTY_NAMES.contains(prop);
  }
}
