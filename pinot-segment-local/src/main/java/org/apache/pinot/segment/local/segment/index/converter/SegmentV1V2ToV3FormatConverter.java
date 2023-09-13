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
package org.apache.pinot.segment.local.segment.index.converter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@inheritDoc}
 */
public class SegmentV1V2ToV3FormatConverter implements SegmentFormatConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentV1V2ToV3FormatConverter.class);
  private static final String V3_TEMP_DIR_SUFFIX = ".v3.tmp";

  // NOTE: this can convert segments in v1 and v2 format to v3.
  // we use variable names with v2 prefix for readability
  @Override
  public void convert(File v2SegmentDirectory)
      throws Exception {
    Preconditions.checkNotNull(v2SegmentDirectory, "Segment directory should not be null");

    Preconditions.checkState(v2SegmentDirectory.exists() && v2SegmentDirectory.isDirectory(),
        "Segment directory: " + v2SegmentDirectory + " must exist and should be a directory");

    LOGGER.info("Converting segment: {} to v3 format", v2SegmentDirectory);

    // check existing segment version
    SegmentMetadataImpl v2Metadata = new SegmentMetadataImpl(v2SegmentDirectory);
    SegmentVersion oldVersion = v2Metadata.getVersion();
    Preconditions.checkState(oldVersion != SegmentVersion.v3, "Segment {} is already in v3 format but at wrong path",
        v2Metadata.getName());

    Preconditions.checkArgument(oldVersion == SegmentVersion.v1 || oldVersion == SegmentVersion.v2,
        "Can not convert segment version: {} at path: {} ", oldVersion, v2SegmentDirectory);

    deleteStaleConversionDirectories(v2SegmentDirectory);

    File v3TempDirectory = v3ConversionTempDirectory(v2SegmentDirectory);
    setDirectoryPermissions(v3TempDirectory);

    createMetadataFile(v2SegmentDirectory, v3TempDirectory);
    copyCreationMetadataIfExists(v2SegmentDirectory, v3TempDirectory);
    copyIndexData(v2SegmentDirectory, v2Metadata, v3TempDirectory);

    File newLocation = SegmentDirectoryPaths.segmentDirectoryFor(v2SegmentDirectory, SegmentVersion.v3);
    LOGGER.info("v3 segment location for segment: {} is {}", v2Metadata.getName(), newLocation);
    v3TempDirectory.renameTo(newLocation);
    deleteV2Files(v2SegmentDirectory);
  }

  private void deleteV2Files(File v2SegmentDirectory)
      throws IOException {
    LOGGER.info("Deleting files in v1 segment directory: {}", v2SegmentDirectory);
    File[] files = v2SegmentDirectory.listFiles();
    if (files == null) {
      // unexpected condition but we don't want to stop server
      LOGGER.error("v1 segment directory: {}  returned null list of files", v2SegmentDirectory);
      return;
    }
    for (File file : files) {
      if (file.isFile() && file.exists()) {
        FileUtils.deleteQuietly(file);
      }
      if (file.isDirectory() && file.getName().endsWith(V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION)) {
        FileUtils.deleteDirectory(file);
      }
    }
  }

  @VisibleForTesting
  public File v3ConversionTempDirectory(File v2SegmentDirectory)
      throws IOException {
    File v3TempDirectory =
        Files.createTempDirectory(v2SegmentDirectory.toPath(), v2SegmentDirectory.getName() + V3_TEMP_DIR_SUFFIX)
            .toFile();
    return v3TempDirectory;
  }

  private void setDirectoryPermissions(File v3Directory)
      throws IOException {
    EnumSet<PosixFilePermission> permissions =
        EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE);
    try {
      Files.setPosixFilePermissions(v3Directory.toPath(), permissions);
    } catch (UnsupportedOperationException ex) {
      LOGGER.error("unsupported non-posix filesystem permissions setting");
    }
  }

  private void copyIndexData(File v2Directory, SegmentMetadataImpl v2Metadata, File v3Directory)
      throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());
    PinotConfiguration configuration = new PinotConfiguration(props);
    try (SegmentDirectory v2Segment = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(v2Directory.toURI(), new SegmentDirectoryLoaderContext.Builder().setSegmentName(v2Metadata.getName())
            .setSegmentDirectoryConfigs(configuration).build());
        SegmentDirectory v3Segment = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
            .load(v3Directory.toURI(), new SegmentDirectoryLoaderContext.Builder().setSegmentName(v2Metadata.getName())
                .setSegmentDirectoryConfigs(configuration).build())) {
      try (SegmentDirectory.Reader v2DataReader = v2Segment.createReader();
          SegmentDirectory.Writer v3DataWriter = v3Segment.createWriter()) {
        for (String column : v2Metadata.getAllColumns()) {
          for (IndexType<?, ?, ?> indexType : sortedIndexTypes()) {
            // NOTE: Text index is copied separately
            if (indexType != StandardIndexes.text()) {
              copyIndexIfExists(v2DataReader, v3DataWriter, column, indexType);
            }
          }
        }
        v3DataWriter.save();
      }
    }
    copyLuceneTextIndexIfExists(v2Directory, v3Directory);
    copyStarTreeV2(v2Directory, v3Directory);
  }

  private List<IndexType<?, ?, ?>> sortedIndexTypes() {
    return IndexService.getInstance().getAllIndexes().stream()
        .sorted((i1, i2) -> i1.getId().compareTo(i2.getId()))
        .collect(Collectors.toList());
  }

  private void copyIndexIfExists(SegmentDirectory.Reader reader, SegmentDirectory.Writer writer, String column,
      IndexType indexType)
      throws IOException {
    if (reader.hasIndexFor(column, indexType)) {
      readCopyBuffers(reader, writer, column, indexType);
    }
  }

  private void copyStarTreeV2(File src, File dest)
      throws IOException {
    File indexFile = new File(src, StarTreeV2Constants.INDEX_FILE_NAME);
    if (indexFile.exists()) {
      FileUtils.copyFile(indexFile, new File(dest, StarTreeV2Constants.INDEX_FILE_NAME));
      FileUtils.copyFile(new File(src, StarTreeV2Constants.INDEX_MAP_FILE_NAME),
          new File(dest, StarTreeV2Constants.INDEX_MAP_FILE_NAME));
    }
  }

  private void readCopyBuffers(SegmentDirectory.Reader reader, SegmentDirectory.Writer writer, String column,
      IndexType indexType)
      throws IOException {
    PinotDataBuffer oldBuffer = reader.getIndexFor(column, indexType);
    long oldBufferSize = oldBuffer.size();
    PinotDataBuffer newBuffer = writer.newIndexFor(column, indexType, oldBufferSize);
    oldBuffer.copyTo(0, newBuffer, 0, oldBufferSize);
  }

  private void createMetadataFile(File currentDir, File v3Dir)
      throws ConfigurationException {
    File v2MetadataFile = new File(currentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    File v3MetadataFile = new File(v3Dir, V1Constants.MetadataKeys.METADATA_FILE_NAME);

    final PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(v2MetadataFile);
    // update the segment version
    properties.setProperty(V1Constants.MetadataKeys.Segment.SEGMENT_VERSION, SegmentVersion.v3.toString());
    FileHandler handler = new FileHandler(properties);
    handler.save(v3MetadataFile);
  }

  private void copyCreationMetadataIfExists(File currentDir, File v3Dir)
      throws IOException {
    File v2CreationFile = new File(currentDir, V1Constants.SEGMENT_CREATION_META);
    if (v2CreationFile.exists()) {
      File v3CreationFile = new File(v3Dir, V1Constants.SEGMENT_CREATION_META);
      Files.copy(v2CreationFile.toPath(), v3CreationFile.toPath());
    }
  }

  private void copyLuceneTextIndexIfExists(File segmentDirectory, File v3Dir)
      throws IOException {
    // TODO: see if this can be done by reusing some existing methods
    String suffix = V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION;
    File[] textIndexFiles = segmentDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(suffix);
      }
    });
    for (File textIndexFile : textIndexFiles) {
      File[] indexFiles = textIndexFile.listFiles();
      File v3LuceneIndexDir = new File(v3Dir, textIndexFile.getName());
      v3LuceneIndexDir.mkdir();
      for (File indexFile : indexFiles) {
        File v3LuceneIndexFile = new File(v3LuceneIndexDir, indexFile.getName());
        Files.copy(indexFile.toPath(), v3LuceneIndexFile.toPath());
      }
    }
    // if segment reload is issued asking for up-conversion of
    // on-disk segment format from v1/v2 to v3, then in addition
    // to moving the lucene text index files, we need to move the
    // docID mapping/cache file created by us in v1/v2 during an earlier
    // load of the segment.
    String docIDFileSuffix = V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION;
    File[] textIndexDocIdMappingFiles = segmentDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(docIDFileSuffix);
      }
    });
    for (File docIdMappingFile : textIndexDocIdMappingFiles) {
      File v3DocIdMappingFile = new File(v3Dir, docIdMappingFile.getName());
      Files.copy(docIdMappingFile.toPath(), v3DocIdMappingFile.toPath());
    }
  }

  private void deleteStaleConversionDirectories(File segmentDirectory) {
    final String prefix = segmentDirectory.getName() + V3_TEMP_DIR_SUFFIX;
    File[] files = segmentDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix);
      }
    });

    for (File file : files) {
      LOGGER.info("Deleting stale v3 directory: {}", file);
      FileUtils.deleteQuietly(file);
    }
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: $0 <table directory with segments>");
      System.exit(1);
    }
    File tableDirectory = new File(args[0]);
    Preconditions.checkState(tableDirectory.exists(), "Directory: {} does not exist", tableDirectory);
    Preconditions.checkState(tableDirectory.isDirectory(), "Path: {} is not a directory", tableDirectory);
    File[] files = tableDirectory.listFiles();
    SegmentFormatConverter converter = new SegmentV1V2ToV3FormatConverter();

    for (File file : files) {
      if (!file.isDirectory()) {
        System.out.println("Path: " + file + " is not a directory. Skipping...");
        continue;
      }
      long startTimeNano = System.nanoTime();
      converter.convert(file);
      long endTimeNano = System.nanoTime();
      long latency = (endTimeNano - startTimeNano) / (1000 * 1000);
      System.out.println("Converting segment: " + file + " took " + latency + " milliseconds");
    }
  }
}
