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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.utils.LuceneTextIndexUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig.DocIdTranslatorMode;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.CsvParser;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


/**
 * This is used to read/search the Lucene text index.
 * When {@link ImmutableSegmentLoader} loads the segment,
 * it also loads (mmaps) the Lucene text index if the segment has TEXT column(s).
 *
 * This class is a version of LuceneTestIndexReader adapted to handling multiple columns.
 */
public class MultiColumnLuceneTextIndexReader implements MultiColumnTextIndexReader {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MultiColumnLuceneTextIndexReader.class);
  public static final String CLASSIC_QUERY_PARSER = "org.apache.lucene.queryparser.classic.QueryParser";

  private final List<String> _columns;
  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  private final DocIdTranslator _docIdTranslator;
  private final Analyzer _analyzer;
  private boolean _useANDForMultiTermQueries = false;
  private final String _queryParserClass;
  private Constructor<QueryParserBase> _queryParserClassConstructor;
  private boolean _enablePrefixSuffixMatchingInPhraseQueries = false;
  private final Map<String, ColumnConfig> _perColumnConfigs;

  /* Per-column text configuration that overrides top-level configuration. */
  public static class ColumnConfig {
    private final String _queryParserClass;
    private final Constructor<QueryParserBase> _queryParserClassConstructor;
    private final Boolean _useANDForMultiTermQueries;
    private final Boolean _enablePrefixSuffixMatchingInPhraseQueries;
    private final List<String> _stopWordsInclude;
    private final List<String> _stopWordsExclude;
    private final Boolean _isCaseSensitive;
    private final String _luceneAnalyzerClass;
    private final List<String> _luceneAnalyzerClassArgs;
    private final List<String> _luceneAnalyzerClassArgTypes;

    public ColumnConfig(String queryParserClass,
        Constructor<QueryParserBase> queryParserClassConstructor,
        Boolean useANDForMultiTermQueries,
        Boolean enablePrefixSuffixMatchingInPhraseQueries,
        List<String> stopWordsInclude,
        List<String> stopWordsExclude,
        Boolean isCaseSensitive,
        String luceneAnalyzerClass,
        List<String> luceneAnalyzerClassArgs,
        List<String> luceneAnalyzerClassArgTypes) {
      _queryParserClass = queryParserClass;
      _queryParserClassConstructor = queryParserClassConstructor;
      _useANDForMultiTermQueries = useANDForMultiTermQueries;
      _enablePrefixSuffixMatchingInPhraseQueries = enablePrefixSuffixMatchingInPhraseQueries;
      _stopWordsInclude = stopWordsInclude;
      _stopWordsExclude = stopWordsExclude;
      _isCaseSensitive = isCaseSensitive;
      _luceneAnalyzerClass = luceneAnalyzerClass;
      _luceneAnalyzerClassArgs = luceneAnalyzerClassArgs;
      _luceneAnalyzerClassArgTypes = luceneAnalyzerClassArgTypes;
    }

    public boolean requiresCustomAnalyzer() {
      return _stopWordsExclude != null
          || _stopWordsInclude != null
          || _isCaseSensitive != null
          || _luceneAnalyzerClass != null
          || _luceneAnalyzerClassArgs != null
          || _luceneAnalyzerClassArgTypes != null;
    }

    public String getQueryParserClass() {
      return _queryParserClass;
    }

    public Constructor<QueryParserBase> getQueryParserClassConstructor() {
      return _queryParserClassConstructor;
    }

    public Boolean getUseANDForMultiTermQueries() {
      return _useANDForMultiTermQueries;
    }

    public Boolean getEnablePrefixSuffixMatchingInPhraseQueries() {
      return _enablePrefixSuffixMatchingInPhraseQueries;
    }

    public List<String> getStopWordsInclude() {
      return _stopWordsInclude;
    }

    public List<String> getStopWordsExclude() {
      return _stopWordsExclude;
    }

    public Boolean isCaseSensitive() {
      return _isCaseSensitive;
    }

    public String getLuceneAnalyzerClass() {
      return _luceneAnalyzerClass;
    }

    public List<String> getLuceneAnalyzerClassArgs() {
      return _luceneAnalyzerClassArgs;
    }

    public List<String> getLuceneAnalyzerClassArgTypes() {
      return _luceneAnalyzerClassArgTypes;
    }
  }

  public MultiColumnLuceneTextIndexReader(
      List<String> columns,
      File indexDir,
      int numDocs,
      Map<String, String> sharedConfig,
      Map<String, Map<String, String>> perColumnConfig) {
    _columns = columns;
    TextIndexConfig config = new TextIndexConfigBuilder(FSTType.LUCENE).withProperties(sharedConfig).build();
    try {
      File indexFile = getTextIndexFile(indexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      _indexReader = DirectoryReader.open(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);
      if (!config.isEnableQueryCache()) {
        // Disable Lucene query result cache. While it helps a lot with performance for
        // repeated queries, on the downside it can cause heap issues.
        _indexSearcher.setQueryCache(null);
      }
      if (config.isUseANDForMultiTermQueries()) {
        _useANDForMultiTermQueries = true;
      }
      if (config.isEnablePrefixSuffixMatchingInPhraseQueries()) {
        _enablePrefixSuffixMatchingInPhraseQueries = true;
      }
      // TODO: consider using a threshold of num docs per segment to decide between building
      // mapping file upfront on segment load v/s on-the-fly during query processing
      // If the properties file exists, use the analyzer properties and query parser class from the properties file
      File propertiesFile = new File(indexFile, V1Constants.Indexes.LUCENE_TEXT_INDEX_PROPERTIES_FILE);
      if (propertiesFile.exists()) {
        config = TextIndexUtils.getUpdatedConfigFromPropertiesFile(propertiesFile, config);
      }

      _docIdTranslator = prepareDocIdTranslator(indexDir, _columns, numDocs, _indexSearcher, config, indexFile);
      _queryParserClass = config.getLuceneQueryParserClass();
      _queryParserClassConstructor =
          TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(_queryParserClass);
      _perColumnConfigs = buildColumnConfigs(perColumnConfig);

      Map<String, ColumnConfig> cfgs = _perColumnConfigs;
      _analyzer = buildAnalyzer(config, cfgs);

      LOGGER.info("Successfully read lucene index for {} from {}", _columns, indexDir);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index reader for column {}, exception {}", columns,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public static Analyzer buildAnalyzer(TextIndexConfig config, Map<String, ColumnConfig> colConfigs)
      throws ReflectiveOperationException {
    Analyzer analyzer = TextIndexUtils.getAnalyzer(config);
    HashMap<String, Analyzer> perColumnAnalyzers = null;
    for (Map.Entry<String, ColumnConfig> entry : colConfigs.entrySet()) {
      ColumnConfig colConfig = entry.getValue();

      if (colConfig.requiresCustomAnalyzer()) {
        Analyzer colAnalyzer = TextIndexUtils.getAnalyzer(config, colConfig);

        if (perColumnAnalyzers == null) {
          perColumnAnalyzers = new HashMap<>();
        }

        perColumnAnalyzers.put(entry.getKey(), colAnalyzer);
      }
    }

    if (perColumnAnalyzers != null) {
      return new PerFieldAnalyzerWrapper(analyzer, perColumnAnalyzers);
    } else {
      return analyzer;
    }
  }

  /**
   * Parse given per column configurations.
   * @param perColumnConfig
   * @return map of columns names to per-column lucene configuration
   * @throws ReflectiveOperationException
   */
  public static Map<String, ColumnConfig> buildColumnConfigs(Map<String, Map<String, String>> perColumnConfig)
      throws ReflectiveOperationException {
    Map<String, ColumnConfig> perColumnConfigs;
    if (perColumnConfig == null || perColumnConfig.isEmpty()) {
      perColumnConfigs = Collections.emptyMap();
    } else {
      perColumnConfigs = new HashMap<>(perColumnConfig.size());
      for (Map.Entry<String, Map<String, String>> entry : perColumnConfig.entrySet()) {
        String column = entry.getKey();
        Map<String, String> columnProps = entry.getValue();

        String parserClass = columnProps.get(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS);
        Constructor<QueryParserBase> parserConstructor =
            parserClass != null ? TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(parserClass)
                : null;
        Boolean useAnd = getBoolean(columnProps, FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES);
        Boolean enablePhrase = getBoolean(columnProps, FieldConfig.TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES);
        List<String> includeKey = extractWords(columnProps, FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY);
        List<String> excludeKey = extractWords(columnProps, FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY);
        Boolean isCaseSensitive = getBoolean(columnProps, FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY);
        String luceneAnalyzerClass = columnProps.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS);
        List<String> luceneAnalyzerClassArgs =
            columnProps.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS) != null
                ? CsvParser.parse(columnProps.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS), true, false)
                : null;
        List<String> luceneAnalyzerClassArgTypes =
            columnProps.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES) != null
                ? CsvParser.parse(columnProps.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES), false, true)
                : null;

        ColumnConfig columnConfig =
            new ColumnConfig(parserClass, parserConstructor, useAnd, enablePhrase, includeKey, excludeKey,
                isCaseSensitive, luceneAnalyzerClass, luceneAnalyzerClassArgs, luceneAnalyzerClassArgTypes);
        perColumnConfigs.put(column, columnConfig);
      }
    }
    return perColumnConfigs;
  }

  private static List<String> extractWords(Map<String, String> properties, String key) {
    if (properties.get(key) != null) {
      return TextIndexUtils.parseEntryAsString(properties, key);
    }

    return null;
  }

  private static Boolean getBoolean(Map<String, String> map, String key) {
    String value = map.get(key);
    if (value == null) {
      return null;
    }

    return Boolean.valueOf(value);
  }

  public MultiColumnLuceneTextIndexReader(SegmentMetadata metadata) {
    this(metadata.getMultiColumnTextMetadata().getColumns(), metadata.getIndexDir(), metadata.getTotalDocs(),
        metadata.getMultiColumnTextMetadata().getSharedProperties(),
        metadata.getMultiColumnTextMetadata().getPerColumnProperties());
  }

  /**
   * CASE 1: If IndexLoadingConfig specifies a segment version to load and if it is different then
   * the on-disk version of the segment, then {@link ImmutableSegmentLoader}
   * will take care of up-converting the on-disk segment to v3 before load. The converter
   * already has support for converting v1 text index to v3. So the text index can be
   * loaded from segmentIndexDir/v3/ since v3 sub-directory would have already been created
   *
   * CASE 2: However, if IndexLoadingConfig doesn't specify the segment version to load or if the specified
   * version is same as the on-disk version of the segment, then ImmutableSegmentLoader will load
   * whatever the version of segment is on disk.
   *
   * @param segmentIndexDir top-level segment index directory
   * @return text index file
   */
  private File getTextIndexFile(File segmentIndexDir) {
    // will return null if file does not exist
    File file =
        SegmentDirectoryPaths.findTextIndexIndexFile(segmentIndexDir, MultiColumnTextIndexConstants.INDEX_DIR_NAME);
    if (file == null) {
      throw new IllegalStateException("Failed to find multi-column text index file for columns: " + _columns);
    }
    return file;
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    // It should be possible to query without passing column if tokens are prefixed with column name,
    // e.g. col1:some_token col2:other_token
    throw new UnsupportedOperationException("Multi-column text index requires column name to query!");
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to query!");
  }

  @Override
  public MutableRoaringBitmap getDocIds(String column, String searchQuery, @Nullable String optionsString) {
    if (optionsString != null && !optionsString.trim().isEmpty()) {
      LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(optionsString);
      if (!options.getOptions().isEmpty()) {
        return getDocIdsWithOptions(column, searchQuery, options);
      }
    }
    return getDocIdsWithoutOptions(column, searchQuery);
  }

  // TODO: Consider creating a base class (e.g., BaseLuceneTextIndexReader) to avoid code duplication
  // for getDocIdsWithOptions method across LuceneTextIndexReader, MultiColumnLuceneTextIndexReader,
  // RealtimeLuceneTextIndex, and MultiColumnRealtimeLuceneTextIndex
  private MutableRoaringBitmap getDocIdsWithOptions(String column, String actualQuery,
      LuceneTextIndexUtils.LuceneTextIndexOptions options) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    Collector docIDCollector = new LuceneDocIdCollector(docIds, _docIdTranslator);
    try {
      Query query = LuceneTextIndexUtils.createQueryParserWithOptions(actualQuery, options, column, _analyzer);
      _indexSearcher.search(query, docIDCollector);
      return docIds;
    } catch (Exception e) {
      throw new RuntimeException("Failed while searching the text index for column " + column
          + " with search query: " + actualQuery, e);
    }
  }

  private MutableRoaringBitmap getDocIdsWithoutOptions(String column, String searchQuery) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    Collector docIDCollector = new LuceneDocIdCollector(docIds, _docIdTranslator);
    try {
      Constructor<QueryParserBase> queryParserClassConstructor = _queryParserClassConstructor;
      String queryParserClass = _queryParserClass;
      boolean enablePrefixSuffixMatchingInPhraseQueries = _enablePrefixSuffixMatchingInPhraseQueries;
      boolean useANDForMultiTermQueries = _useANDForMultiTermQueries;

      ColumnConfig columnConfig = _perColumnConfigs.get(column);
      if (columnConfig != null) {
        if (columnConfig._queryParserClassConstructor != null) {
          queryParserClassConstructor = columnConfig._queryParserClassConstructor;
        }
        if (columnConfig._queryParserClass != null) {
          queryParserClass = columnConfig._queryParserClass;
        }
        if (columnConfig._enablePrefixSuffixMatchingInPhraseQueries != null) {
          enablePrefixSuffixMatchingInPhraseQueries = columnConfig._enablePrefixSuffixMatchingInPhraseQueries;
        }
        if (columnConfig._useANDForMultiTermQueries != null) {
          useANDForMultiTermQueries = columnConfig._useANDForMultiTermQueries;
        }
      }

      // Lucene query parsers are generally stateful and a new instance must be created per query.
      QueryParserBase parser = queryParserClassConstructor.newInstance(column, _analyzer);
      // Phrase search with prefix/suffix matching may have leading *. E.g., `*pache pinot` which can be stripped by
      // the query parser. To support the feature, we need to explicitly set the config to be true.

      if (queryParserClass.equals(CLASSIC_QUERY_PARSER)
          && enablePrefixSuffixMatchingInPhraseQueries) {
        parser.setAllowLeadingWildcard(true);
      }
      if (queryParserClass.equals(CLASSIC_QUERY_PARSER) && useANDForMultiTermQueries) {
        parser.setDefaultOperator(QueryParser.Operator.AND);
      }
      Query query = parser.parse(searchQuery);
      if (queryParserClass.equals(CLASSIC_QUERY_PARSER)
          && enablePrefixSuffixMatchingInPhraseQueries) {
        query = LuceneTextIndexUtils.convertToMultiTermSpanQuery(query);
      }
      _indexSearcher.search(query, docIDCollector);
      return docIds;
    } catch (Exception e) {
      String msg =
          "Caught exception while searching the text index for columns: " + _columns + " search query:" + searchQuery;
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * When we destroy the loaded ImmutableSegment, all the indexes
   * (for each column) are destroyed and as part of that
   * we release the text index
   *
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    _indexReader.close();
    _indexDirectory.close();
    _docIdTranslator.close();
    _analyzer.close();
  }

  DocIdTranslator prepareDocIdTranslator(File segmentIndexDir, List<String> columns, int numDocs,
      IndexSearcher indexSearcher, TextIndexConfig config, File indexDir)
      throws IOException {
    if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.Skip) {
      LOGGER.debug("Using no-op doc id translator");
      return NoOpDocIdTranslator.INSTANCE;
    }

    int length = Integer.BYTES * numDocs;
    File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
        MultiColumnTextIndexConstants.INDEX_DIR_NAME
            + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    // The mapping is local to a segment. It is created on the server during segment load.
    // Unless we are running Pinot on Solaris/SPARC, the underlying architecture is
    // LITTLE_ENDIAN (Linux/x86). So use that as byte order.
    String desc = "Text index docId mapping buffer: " + columns;
    PinotDataBuffer buffer = null;

    try {
      if (docIdMappingFile.exists()) {
        // we will be here for segment reload and server restart
        // for refresh, we will not be here since segment is deleted/replaced
        // TODO: see if we can prefetch the pages
        buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ true, 0, length, ByteOrder.LITTLE_ENDIAN, desc);

        return new DefaultDocIdTranslator(buffer);
      } else {
        buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ false, 0, length, ByteOrder.LITTLE_ENDIAN, desc);

        if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.TryOptimize) {
          LOGGER.debug("Creating lucene to pinot doc id mapping.");
          boolean allIdsAreEqual = true;

          class DocIdVisitor extends DocumentStoredFieldVisitor {
            int _pinotDocId;

            @Override
            public Status needsField(FieldInfo fieldInfo) {
              // assume doc id is the only stored document
              assert LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME.equals(fieldInfo.name);
              return Status.YES;
            }

            @Override
            public void intField(FieldInfo fieldInfo, int value) {
              _pinotDocId = value;
            }
          }

          DocIdVisitor visitor = new DocIdVisitor();
          StoredFields storedFields = indexSearcher.storedFields();

          for (int i = 0; i < numDocs; i++) {
            storedFields.document(i, visitor);
            int pinotDocId = visitor._pinotDocId;
            allIdsAreEqual &= (i == pinotDocId);
            buffer.putInt(i * Integer.BYTES, pinotDocId);
          }

          if (allIdsAreEqual) {
            LOGGER.debug("Lucene doc ids are equal to Pinot's. Deleting mapping and updating index settings.");
            // TODO: it'd be better to unmap without flushing the buffer. Only some buffer types support it, though.
            buffer.close();
            buffer = null;
            // get rid of mapping and use lucene ids
            docIdMappingFile.delete();

            // mapping is unnecessary so store flag in config file to skip checking on next load
            TextIndexConfig newConfig =
                new TextIndexConfigBuilder(config).withDocIdTranslatorMode(DocIdTranslatorMode.Skip.name()).build();
            TextIndexUtils.writeConfigToPropertiesFile(indexDir, newConfig);

            return NoOpDocIdTranslator.INSTANCE;
          } else {
            LOGGER.debug("Lucene doc ids are not equal to Pinot's. Keeping the mapping.");
            // mapping is required so switch to default mode
            TextIndexConfig newConfig =
                new TextIndexConfigBuilder(config).withDocIdTranslatorMode(DocIdTranslatorMode.Default.name()).build();
            TextIndexUtils.writeConfigToPropertiesFile(indexDir, newConfig);

            return new DefaultDocIdTranslator(buffer);
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            Document document = indexSearcher.storedFields().document(i);
            IndexableField field = document.getField(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME);
            int pinotDocId = Integer.parseInt(field.stringValue());
            buffer.putInt(i * Integer.BYTES, pinotDocId);
          }
          return new DefaultDocIdTranslator(buffer);
        }
      }
    } catch (Exception e) {
      if (buffer != null) {
        buffer.close();
      }

      throw new RuntimeException(
          "Caught exception while building doc id mapping for text index columns: " + columns, e);
    }
  }

  @Override
  public boolean isMultiColumn() {
    return true;
  }
}
