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
package org.apache.pinot.tools.segment.converter;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexCreatorFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Class to convert segment with dictionary encoded column to raw index (without dictionary).
 */
@SuppressWarnings({"FieldCanBeLocal", "unused", "rawtypes", "unchecked"})
@CommandLine.Command
public class DictionaryToRawIndexConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryToRawIndexConverter.class);

  @CommandLine.Option(names = {"-dataDir"}, required = true,
      description = "Directory containing uncompressed segments")
  private String _dataDir = null;

  @CommandLine.Option(names = {"-columns"}, required = true,
      description = "Comma separated list of column names to convert")
  private String _columns = null;

  @CommandLine.Option(names = {"-tableName"}, required = false,
      description = "New table name, if different from original")
  private String _tableName = null;

  @CommandLine.Option(names = {"-outputDir"}, required = true, description = "Output directory for writing results")
  private String _outputDir = null;

  @CommandLine.Option(names = {"-overwrite"}, required = false, description = "Overwrite output directory")
  private boolean _overwrite = false;

  @CommandLine.Option(names = {"-numThreads"}, required = false,
      description = "Number of threads to launch for conversion")
  private int _numThreads = 4;

  @CommandLine.Option(names = {"-compressOutput"}, required = false,
      description = "Compress (tar + gzip) output segment")
  private boolean _compressOutput = false;

  @CommandLine.Option(names = {"-compressionType"}, required = false, description = "Compression Type")
  private String _compressionType = "Snappy";

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "print this message")
  private boolean _help = false;

  /**
   * Setter for {@link #_dataDir}
   * @param dataDir Data directory containing un-tarred segments.
   * @return this
   */
  public DictionaryToRawIndexConverter setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  /**
   * Setter for {@link #_outputDir}
   *
   * @param outputDir Directory where output segments should be written
   * @return this
   */
  public DictionaryToRawIndexConverter setOutputDir(String outputDir) {
    _outputDir = outputDir;
    return this;
  }

  /**
   * Setter for columns to convert.
   *
   * @param columns Comma separated list of columns
   * @return this
   */
  public DictionaryToRawIndexConverter setColumns(String columns) {
    _columns = columns;
    return this;
  }

  /**
   * Setter for {@link #_overwrite}
   * When set to true, already existing output directory is overwritten.
   *
   * @param overwrite True for overwriting existing output dir, False otherwise
   * @return this
   */
  public DictionaryToRawIndexConverter setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
    return this;
  }

  /**
   * Method to perform the conversion for a set of segments in the {@link #_dataDir}
   *
   * @return True if successful, False otherwise
   * @throws Exception
   */
  public boolean convert()
      throws Exception {
    if (_help) {
      printUsage();
      return true;
    }

    File dataDir = new File(_dataDir);
    File outputDir = new File(_outputDir);

    if (!dataDir.exists()) {
      LOGGER.error("Data directory '{}' does not exist.", _dataDir);
      return false;
    } else if (outputDir.exists()) {
      if (_overwrite) {
        LOGGER.info("Overwriting existing output directory '{}'", _outputDir);
        FileUtils.deleteQuietly(outputDir);
        outputDir = new File(_outputDir);
        outputDir.mkdir();
      } else {
        LOGGER.error("Output directory '{}' already exists, use -overwrite to overwrite", outputDir);
        return false;
      }
    }

    File[] segmentFiles = dataDir.listFiles();
    if (segmentFiles == null || segmentFiles.length == 0) {
      LOGGER.error("Empty data directory '{}'.", _dataDir);
      return false;
    }

    boolean ret = true;
    final File outDir = outputDir;
    ExecutorService executorService = Executors.newFixedThreadPool(_numThreads);
    for (final File segmentDir : segmentFiles) {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            convertSegment(segmentDir, _columns.split("\\s*,\\s*"), outDir, _compressOutput);
          } catch (Exception e) {
            LOGGER.error("Exception caught while converting segment {}", segmentDir.getName(), e);
            e.printStackTrace();
          }
        }
      });
    }

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.HOURS);
    return ret;
  }

  /**
   * This method converts the specified columns of the given segment from dictionary encoded
   * forward index to raw index without dictionary.
   *
   * @param segmentDir Segment directory
   * @param columns Columns to convert
   * @param outputDir Directory for writing output segment
   * @param compressOutput Tar/gzip the output segment
   * @return True if successful, False otherwise
   * @throws Exception
   */
  public boolean convertSegment(File segmentDir, String[] columns, File outputDir, boolean compressOutput)
      throws Exception {
    File newSegment;

    if (segmentDir.isFile()) {
      if (segmentDir.getName().endsWith(".tar.gz") || segmentDir.getName().endsWith(".tgz")) {
        LOGGER.info("Uncompressing input segment '{}'", segmentDir);
        newSegment = TarGzCompressionUtils.untar(segmentDir, outputDir).get(0);
      } else {
        LOGGER.warn("Skipping non-segment file '{}'", segmentDir.getAbsoluteFile());
        return false;
      }
    } else {
      newSegment = new File(outputDir, segmentDir.getName());
      FileUtils.copyDirectory(segmentDir, newSegment);
    }

    IndexSegment segment = ImmutableSegmentLoader.load(newSegment, ReadMode.mmap);
    for (String column : columns) {
      LOGGER.info("Converting column '{}' for segment '{}'.", column, segment.getSegmentName());
      convertOneColumn(segment, column, newSegment);
    }

    updateMetadata(newSegment, columns, _tableName);
    segment.destroy();

    if (compressOutput) {
      LOGGER.info("Compressing segment '{}'", newSegment);
      File segmentTarFile = new File(outputDir, newSegment.getName() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
      TarGzCompressionUtils.createTarGzFile(newSegment, segmentTarFile);
      FileUtils.deleteQuietly(newSegment);
    }
    return true;
  }

  /**
   * Helper method to update the metadata.properties for the converted segment.
   *
   * @param segmentDir Segment directory
   * @param columns Converted columns
   * @param tableName New table name to be written in the meta-data. Skipped if null.
   */
  private void updateMetadata(File segmentDir, String[] columns, String tableName) {
    File metadataFile = new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(metadataFile);

    if (tableName != null) {
      properties
          .setProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME, TableNameBuilder.extractRawTableName(tableName));
    }

    for (String column : columns) {
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), false);
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT), -1);
    }
    CommonsConfigurationUtils.saveToFile(properties, metadataFile);
  }

  /**
   * Helper method to print usage at the command line interface.
   */
  public void printUsage() {
    System.out.println("Usage: DictionaryTORawIndexConverter");
    for (Field field : DictionaryToRawIndexConverter.class.getDeclaredFields()) {
      if (field.isAnnotationPresent(CommandLine.Option.class)) {
        CommandLine.Option option = field.getAnnotation(CommandLine.Option.class);
        System.out
            .println(String.format("\t%-15s: %s (required=%s)",
                Arrays.toString(option.names()), Arrays.toString(option.description()), option.required()));
      }
    }
  }

  /**
   * Helper method to perform conversion for the specific column.
   *
   * @param segment Input segment to convert
   * @param column Column to convert
   * @param newSegment Directory where raw index to be written
   * @throws IOException
   */
  private void convertOneColumn(IndexSegment segment, String column, File newSegment)
      throws IOException {
    DataSource dataSource = segment.getDataSource(column);
    ForwardIndexReader forwardIndexReader = dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndexReader != null,
        "Forward index disabled for column: %s, cannot convert column!", column);
    Dictionary dictionary = dataSource.getDictionary();

    if (dictionary == null) {
      LOGGER.error("Column '{}' does not have dictionary, cannot convert to raw index.", column);
      return;
    }

    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    if (!dataSourceMetadata.isSingleValue()) {
      LOGGER.error("Cannot convert multi-valued columns '{}'", column);
      return;
    }

    ChunkCompressionType compressionType = ChunkCompressionType.valueOf(_compressionType);
    DataType storedType = dictionary.getValueType();
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
    int lengthOfLongestEntry = (storedType == DataType.STRING) ? getLengthOfLongestEntry(dictionary) : -1;

    try (ForwardIndexCreator rawIndexCreator = ForwardIndexCreatorFactory
        .getRawIndexCreatorForSVColumn(newSegment, compressionType, column, storedType, numDocs, lengthOfLongestEntry,
            false, ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext()) {
      switch (storedType) {
        case INT:
          for (int docId = 0; docId < numDocs; docId++) {
            rawIndexCreator.putInt(dictionary.getIntValue(forwardIndexReader.getDictId(docId, readerContext)));
          }
          break;
        case LONG:
          for (int docId = 0; docId < numDocs; docId++) {
            rawIndexCreator.putLong(dictionary.getLongValue(forwardIndexReader.getDictId(docId, readerContext)));
          }
          break;
        case FLOAT:
          for (int docId = 0; docId < numDocs; docId++) {
            rawIndexCreator.putFloat(dictionary.getFloatValue(forwardIndexReader.getDictId(docId, readerContext)));
          }
          break;
        case DOUBLE:
          for (int docId = 0; docId < numDocs; docId++) {
            rawIndexCreator.putDouble(dictionary.getDoubleValue(forwardIndexReader.getDictId(docId, readerContext)));
          }
          break;
        case STRING:
          for (int docId = 0; docId < numDocs; docId++) {
            rawIndexCreator.putString(dictionary.getStringValue(forwardIndexReader.getDictId(docId, readerContext)));
          }
          break;
        case BYTES:
          for (int docId = 0; docId < numDocs; docId++) {
            rawIndexCreator.putBytes(dictionary.getBytesValue(forwardIndexReader.getDictId(docId, readerContext)));
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }

    deleteForwardIndex(newSegment.getParentFile(), column, dataSourceMetadata.isSorted());
  }

  /**
   * Helper method to remove the forward index for the given column.
   *
   * @param segmentDir Segment directory from which to remove the forward index.
   * @param column Column for which to remove the index.
   * @param sorted True if column is sorted, False otherwise
   */
  private void deleteForwardIndex(File segmentDir, String column, boolean sorted) {
    File dictionaryFile = new File(segmentDir, (column + V1Constants.Dict.FILE_EXTENSION));
    FileUtils.deleteQuietly(dictionaryFile);

    String fwdIndexFileExtension = (sorted) ? V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION
        : V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
    File fwdIndexFile = new File(segmentDir, (column + fwdIndexFileExtension));
    FileUtils.deleteQuietly(fwdIndexFile);
  }

  /**
   * Helper method to get the length
   * @param dictionary Column dictionary
   * @return Length of longest entry
   */
  private int getLengthOfLongestEntry(Dictionary dictionary) {
    int lengthOfLongestEntry = 0;

    int length = dictionary.length();
    for (int dictId = 0; dictId < length; dictId++) {
      String value = (String) dictionary.get(dictId);
      lengthOfLongestEntry = Math.max(lengthOfLongestEntry, value.getBytes(UTF_8).length);
    }

    return lengthOfLongestEntry;
  }

  /**
   * Main method for the class.
   *
   * @param args Arguments for the converter
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {
    DictionaryToRawIndexConverter converter = new DictionaryToRawIndexConverter();
    CommandLine commandLine = new CommandLine(converter);
    commandLine.parseArgs(args);
    converter.convert();
  }
}
