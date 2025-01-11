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
package org.apache.pinot.plugin.ingestion.batch.common;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("serial")
public class SegmentGenerationJobUtils implements Serializable {
  private SegmentGenerationJobUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerationJobUtils.class);

  /**
   * Always use local directory sequence id unless explicitly config: "use.global.directory.sequence.id".
   *
   */
  public static boolean useGlobalDirectorySequenceId(SegmentNameGeneratorSpec spec) {
    if (spec == null || spec.getConfigs() == null) {
      return false;
    }
    String useGlobalDirectorySequenceId =
        spec.getConfigs().get(SegmentGenerationTaskRunner.USE_GLOBAL_DIRECTORY_SEQUENCE_ID);
    if (useGlobalDirectorySequenceId == null) {
      String useLocalDirectorySequenceId =
          spec.getConfigs().get(SegmentGenerationTaskRunner.DEPRECATED_USE_LOCAL_DIRECTORY_SEQUENCE_ID);
      if (useLocalDirectorySequenceId != null) {
        return !Boolean.parseBoolean(useLocalDirectorySequenceId);
      }
    }
    return Boolean.parseBoolean(useGlobalDirectorySequenceId);
  }

  public static void createSegmentMetadataTarGz(File localSegmentDir, File localMetadataTarFile)
      throws Exception {
    List<File> metadataFiles = new ArrayList<>();
    Files.walkFileTree(localSegmentDir.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) {
        if (file.getFileName().toString().equals(V1Constants.MetadataKeys.METADATA_FILE_NAME)
            || file.getFileName().toString().equals(V1Constants.SEGMENT_CREATION_META)) {
          metadataFiles.add(file.toFile());
        }
        return FileVisitResult.CONTINUE;
      }
    });
    LOGGER.info("Tarring metadata files from: [{}] to: {}", metadataFiles, localMetadataTarFile);
    TarCompressionUtils.createCompressedTarFile(metadataFiles.toArray(new File[0]), localMetadataTarFile);
  }

  public static void moveLocalTarFileToRemote(File localMetadataTarFile, URI outputMetadataTarURI, boolean overwrite)
      throws Exception {
    LOGGER.info("Trying to move metadata tar file from: [{}] to [{}]", localMetadataTarFile, outputMetadataTarURI);
    PinotFS outputPinotFS = PinotFSFactory.create(outputMetadataTarURI.getScheme());
    if (!overwrite && outputPinotFS.exists(outputMetadataTarURI)) {
      LOGGER.warn("Not overwrite existing output metadata tar file: {}", outputPinotFS.exists(outputMetadataTarURI));
    } else {
      outputPinotFS.copyFromLocalFile(localMetadataTarFile, outputMetadataTarURI);
    }
    FileUtils.deleteQuietly(localMetadataTarFile);
  }

  /**
   * Move all files from the <sourceDir> to the <destDir>, but don't delete existing contents of destDir.
   * If <overwrite> is true, and the source file exists in the destination directory, then replace it, otherwise
   * log a warning and continue. We assume that source and destination directories are on the same filesystem,
   * so that move() can be used.
   *
   * @param fs
   * @param sourceDir
   * @param destDir
   * @param overwrite
   * @throws IOException
   * @throws URISyntaxException
   */
  public static void moveFiles(PinotFS fs, URI sourceDir, URI destDir, boolean overwrite)
          throws IOException, URISyntaxException {
    for (String sourcePath : fs.listFiles(sourceDir, true)) {
      URI sourceFileUri = SegmentGenerationUtils.getFileURI(sourcePath, sourceDir);
      String sourceFilename = SegmentGenerationUtils.getFileName(sourceFileUri);
      URI destFileUri =
              SegmentGenerationUtils.getRelativeOutputPath(sourceDir, sourceFileUri, destDir).resolve(sourceFilename);

      if (!overwrite && fs.exists(destFileUri)) {
        LOGGER.warn("Can't overwrite existing output segment tar file: {}", destFileUri);
      } else {
        fs.move(sourceFileUri, destFileUri, true);
      }
    }
  }
}
