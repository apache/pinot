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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.Analyzer;
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
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.utils.LuceneTextIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig.DocIdTranslatorMode;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


/**
 * This is used to read/search the Lucene text index.
 * When {@link ImmutableSegmentLoader} loads the segment,
 * it also loads (mmaps) the Lucene text index if the segment has TEXT column(s).
 */
public class LuceneTextIndexReader implements TextIndexReader {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexReader.class);

  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  private final String _column;
  private final DocIdTranslator _docIdTranslator;
  private final Analyzer _analyzer;
  private boolean _useANDForMultiTermQueries = false;
  private final String _queryParserClass;
  private Constructor<QueryParserBase> _queryParserClassConstructor;
  private boolean _enablePrefixSuffixMatchingInPhraseQueries = false;

  public LuceneTextIndexReader(String column, File indexDir, int numDocs, TextIndexConfig config) {
    _column = column;
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

      _docIdTranslator = prepareDocIdTranslator(indexDir, _column, numDocs, _indexSearcher, config, indexFile);
      _analyzer = TextIndexUtils.getAnalyzer(config);
      _queryParserClass = config.getLuceneQueryParserClass();
      _queryParserClassConstructor =
          TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(_queryParserClass);
      LOGGER.info("Successfully read lucene index for {} from {}", _column, indexDir);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index reader for column {}, exception {}", column,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * As part of loading the segment in ImmutableSegmentLoader,
   * we load the text index (per column if it exists) and store
   * the reference in {@link PhysicalColumnIndexContainer}
   * similar to how it is done for other types of indexes.
   *
   * @param column   column name
   * @param indexDir segment index directory
   * @param numDocs  number of documents in the segment
   */
  public LuceneTextIndexReader(String column, File indexDir, int numDocs,
      @Nullable Map<String, String> textIndexProperties) {
    this(column, indexDir, numDocs,
        new TextIndexConfigBuilder(FSTType.LUCENE).withProperties(textIndexProperties).build());
  }

  /**
   * Buffer-based constructor for reading Lucene text index from a combined buffer.
   * This constructor creates a Lucene Directory from a PinotDataBuffer containing
   * all the text index files in a combined format.
   *
   * @param column the column name
   * @param indexBuffer the buffer containing combined text index data
   * @param numDocs number of documents in the segment
   * @param config text index configuration
   */
  public LuceneTextIndexReader(String column, PinotDataBuffer indexBuffer, int numDocs, TextIndexConfig config) {
    _column = column;
    try {
      // Create Lucene Directory from buffer
      Directory indexDirectory = LuceneTextIndexBufferReader.createLuceneDirectory(indexBuffer, column);

      // Extract properties from buffer
      Properties properties = LuceneTextIndexBufferReader.extractProperties(indexBuffer, column);
      if (!properties.isEmpty()) {
        config = updateConfigFromProperties(properties, config);
      }

      // Initialize from components
      _indexDirectory = indexDirectory;
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
      PinotDataBuffer docIdMappingBuffer = LuceneTextIndexBufferReader.extractDocIdMappingBuffer(indexBuffer, column);
      // Initialize docId translator
      _docIdTranslator = createDocIdTranslator(docIdMappingBuffer, config, numDocs);
      // Initialize analyzer and query parser
      _analyzer = TextIndexUtils.getAnalyzer(config);
      _queryParserClass = config.getLuceneQueryParserClass();
      _queryParserClassConstructor =
          TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(_queryParserClass);

      LOGGER.info("Successfully read lucene index for {} from buffer", _column);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index reader for column {} from buffer", column, e);
      throw new RuntimeException(e);
    }
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
    File file = SegmentDirectoryPaths.findTextIndexIndexFile(segmentIndexDir, _column);
    if (file == null) {
      throw new IllegalStateException("Failed to find text index file for column: " + _column);
    }
    return file;
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException("");
  }

  @Deprecated
  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    return getDocIds(searchQuery, null);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery, @Nullable String optionsString) {
    if (optionsString != null && !optionsString.trim().isEmpty()) {
      LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(optionsString);
      if (!options.getOptions().isEmpty()) {
        return getDocIdsWithOptions(searchQuery, options);
      }
    }
    return getDocIdsWithoutOptions(searchQuery);
  }

  private MutableRoaringBitmap getDocIdsWithoutOptions(String searchQuery) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    Collector docIDCollector = new LuceneDocIdCollector(docIds, _docIdTranslator);
    try {
      // Lucene query parsers are generally stateful and a new instance must be created per query.
      QueryParserBase parser = _queryParserClassConstructor.newInstance(_column, _analyzer);
      // Phrase search with prefix/suffix matching may have leading *. E.g., `*pache pinot` which can be stripped by
      // the query parser. To support the feature, we need to explicitly set the config to be true.
      if (_enablePrefixSuffixMatchingInPhraseQueries) {
        parser.setAllowLeadingWildcard(true);
      }
      if (_useANDForMultiTermQueries) {
        parser.setDefaultOperator(QueryParser.Operator.AND);
      }
      Query query = parser.parse(searchQuery);
      if (_queryParserClass.equals("org.apache.lucene.queryparser.classic.QueryParser")
              && _enablePrefixSuffixMatchingInPhraseQueries) {
        query = LuceneTextIndexUtils.convertToMultiTermSpanQuery(query);
      }
      _indexSearcher.search(query, docIDCollector);
      return docIds;
    } catch (Exception e) {
      String msg =
          "Caught exception while searching the text index for column:" + _column + " search query:" + searchQuery;
      throw new RuntimeException(msg, e);
    }
  }

  // TODO: Consider creating a base class (e.g., BaseLuceneTextIndexReader) to avoid code duplication
  // for getDocIdsWithOptions method across LuceneTextIndexReader, MultiColumnLuceneTextIndexReader,
  // RealtimeLuceneTextIndex, and MultiColumnRealtimeLuceneTextIndex
  private MutableRoaringBitmap getDocIdsWithOptions(String actualQuery,
      LuceneTextIndexUtils.LuceneTextIndexOptions options) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    Collector docIDCollector = new LuceneDocIdCollector(docIds, _docIdTranslator);
    try {
      Query query = LuceneTextIndexUtils.createQueryParserWithOptions(actualQuery, options, _column, _analyzer);
      _indexSearcher.search(query, docIDCollector);
      return docIds;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed while searching the text index for column " + _column + " with search query: " + actualQuery, e);
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

  /**
   * Updates TextIndexConfig from Properties object
   */
  private TextIndexConfig updateConfigFromProperties(Properties properties, TextIndexConfig config) {
    TextIndexConfigBuilder builder = new TextIndexConfigBuilder(config);

    String analyzerClass = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS);
    if (analyzerClass != null) {
      builder.withLuceneAnalyzerClass(analyzerClass);
    }

    String analyzerArgs = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS);
    if (analyzerArgs != null) {
      List<String> args = Arrays.asList(analyzerArgs.split(","));
      builder.withLuceneAnalyzerClassArgs(args);
    }

    String analyzerArgTypes = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES);
    if (analyzerArgTypes != null) {
      List<String> argTypes = Arrays.asList(analyzerArgTypes.split(","));
      builder.withLuceneAnalyzerClassArgTypes(argTypes);
    }

    String queryParserClass = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS);
    if (queryParserClass != null) {
      builder.withLuceneQueryParserClass(queryParserClass);
    }

    String docIdTranslatorMode = properties.getProperty(FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE);
    if (docIdTranslatorMode != null) {
      builder.withDocIdTranslatorMode(docIdTranslatorMode);
    }

    return builder.build();
  }

  DocIdTranslator prepareDocIdTranslator(File segmentIndexDir, String column, int numDocs, IndexSearcher indexSearcher,
      TextIndexConfig config, File indexDir)
      throws IOException {
    if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.Skip) {
      LOGGER.debug("Using no-op doc id translator");
      return NoOpDocIdTranslator.INSTANCE;
    }

    int length = Integer.BYTES * numDocs;
    File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
        column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    // The mapping is local to a segment. It is created on the server during segment load.
    // Unless we are running Pinot on Solaris/SPARC, the underlying architecture is
    // LITTLE_ENDIAN (Linux/x86). So use that as byte order.
    String desc = "Text index docId mapping buffer: " + column;
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
            Document document = indexSearcher.doc(i);
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

      throw new RuntimeException("Caught exception while building doc id mapping for text index column: " + column, e);
    }
  }

  /**
   * Creates docId translator from buffer
   */
  private DocIdTranslator createDocIdTranslator(PinotDataBuffer docIdMappingBuffer, TextIndexConfig config, int numDocs)
      throws IOException {
    if (config.getDocIdTranslatorMode() == DocIdTranslatorMode.Skip) {
      LOGGER.debug("Using no-op doc id translator");
      return NoOpDocIdTranslator.INSTANCE;
    }

    if (docIdMappingBuffer != null) {
      return new DefaultDocIdTranslator(docIdMappingBuffer);
    }

    // Create a new buffer and populate it
    int length = Integer.BYTES * numDocs;
    String desc = "Text index docId mapping buffer: " + _column;
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(length, ByteOrder.LITTLE_ENDIAN, desc);

    try {
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
        StoredFields storedFields = _indexSearcher.storedFields();

        for (int i = 0; i < numDocs; i++) {
          storedFields.document(i, visitor);
          int pinotDocId = visitor._pinotDocId;
          allIdsAreEqual &= (i == pinotDocId);
          buffer.putInt(i * Integer.BYTES, pinotDocId);
        }

        if (allIdsAreEqual) {
          LOGGER.debug("Lucene doc ids are equal to Pinot's. Using no-op translator.");
          buffer.close();
          return NoOpDocIdTranslator.INSTANCE;
        } else {
          LOGGER.debug("Lucene doc ids are not equal to Pinot's. Using default translator.");
          return new DefaultDocIdTranslator(buffer);
        }
      } else {
        for (int i = 0; i < numDocs; i++) {
          Document document = _indexSearcher.doc(i);
          IndexableField field = document.getField(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME);
          int pinotDocId = Integer.parseInt(field.stringValue());
          buffer.putInt(i * Integer.BYTES, pinotDocId);
        }
        return new DefaultDocIdTranslator(buffer);
      }
    } catch (Exception e) {
      buffer.close();
      throw new RuntimeException("Caught exception while creating doc id translator for text index column: " + _column,
          e);
    }
  }
}
