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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.filesystem.FileMetadata;
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

  // Key used to pass the serialized SegmentGenerationJobSpec through a distributed job framework
  public static final String SEGMENT_GENERATION_JOB_SPEC = "segmentGenerationJobSpec";

  // Field names in the executionFrameworkSpec/extraConfigs section shared across ingestion frameworks
  public static final String DEPENDENCY_JAR_DIR = "dependencyJarDir";
  public static final String STAGING_DIR = "stagingDir";
  /// Optional extraConfigs key controlling the number of parallel staging to deep-store copy threads.
  /// Default is [#DEFAULT_STAGING_COPY_PARALLELISM]. Cap is [#MAX_STAGING_COPY_PARALLELISM].
  public static final String STAGING_COPY_PARALLELISM = "stagingCopyParallelism";
  public static final int DEFAULT_STAGING_COPY_PARALLELISM = 10;
  public static final int MAX_STAGING_COPY_PARALLELISM = 64;

  /// Always use local directory sequence id unless explicitly config: "use.global.directory.sequence.id".
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

  /// Move all files from the <sourceDir> to the <destDir>, but don't delete existing contents of destDir.
  /// If <overwrite> is true, and the source file exists in the destination directory, then replace it, otherwise
  /// log a warning and continue. We assume that source and destination directories are on the same filesystem,
  /// so that move() can be used.
  /// Uses [#DEFAULT_STAGING_COPY_PARALLELISM] worker threads.
  ///
  /// The shared [PinotFS] instance must support concurrent `move` of distinct paths (true for
  /// `LocalPinotFS` and typical remote implementations).
  ///
  /// @param fs filesystem used for both source and destination
  /// @param sourceDir source directory URI
  /// @param destDir destination directory URI
  /// @param overwrite whether to overwrite existing destination files
  /// @throws IOException on listing or move failure
  /// @throws URISyntaxException on URI construction failure
  public static void moveFiles(PinotFS fs, URI sourceDir, URI destDir, boolean overwrite)
      throws IOException, URISyntaxException {
    moveFiles(fs, sourceDir, destDir, overwrite, DEFAULT_STAGING_COPY_PARALLELISM);
  }

  /// Move all files from the <sourceDir> to the <destDir> using up to `parallelism` threads.
  /// Directories in the source listing are skipped; parent directories on the destination are created by
  /// [PinotFS#move]. Relative path layout under `sourceDir` is preserved.
  ///
  /// `parallelism` is clamped to at most [#MAX_STAGING_COPY_PARALLELISM] and to the number of files to
  /// move, so direct callers of this overload cannot exceed the cap that
  /// [#getStagingCopyParallelism] enforces.
  ///
  /// On partial failure, remaining in-flight moves are allowed to finish, then an [IOException] is thrown
  /// with any additional failures attached as suppressed exceptions. On interruption, all outstanding
  /// moves are cancelled, the interrupt status is restored, and an [IOException] is thrown.
  ///
  /// @param fs filesystem used for both source and destination (must be safe for concurrent move of
  ///     distinct paths)
  /// @param sourceDir source directory URI
  /// @param destDir destination directory URI
  /// @param overwrite whether to overwrite existing destination files
  /// @param parallelism number of concurrent move workers; values <= 1 run serially
  /// @throws IOException on listing or move failure
  /// @throws URISyntaxException on URI construction failure
  public static void moveFiles(PinotFS fs, URI sourceDir, URI destDir, boolean overwrite, int parallelism)
      throws IOException, URISyntaxException {
    List<URI> sourceFileUris = listSourceFiles(fs, sourceDir);
    if (sourceFileUris.isEmpty()) {
      return;
    }
    int effectiveParallelism =
        Math.max(1, Math.min(Math.min(parallelism, MAX_STAGING_COPY_PARALLELISM), sourceFileUris.size()));
    LOGGER.info("Moving {} files from [{}] to [{}] with parallelism {}", sourceFileUris.size(), sourceDir, destDir,
        effectiveParallelism);
    if (effectiveParallelism == 1) {
      for (URI sourceFileUri : sourceFileUris) {
        moveOneFile(fs, sourceDir, sourceFileUri, destDir, overwrite);
      }
      return;
    }

    ExecutorService executor = Executors.newFixedThreadPool(effectiveParallelism, r -> {
      Thread t = new Thread(r, "pinot-staging-copy");
      t.setDaemon(true);
      return t;
    });
    try {
      List<Future<Void>> futures = new ArrayList<>(sourceFileUris.size());
      for (URI sourceFileUri : sourceFileUris) {
        futures.add(executor.submit(() -> {
          moveOneFile(fs, sourceDir, sourceFileUri, destDir, overwrite);
          return null;
        }));
      }
      IOException firstFailure = null;
      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          // Cancel every outstanding move and stop waiting instead of blocking on the remaining futures.
          futures.forEach(f -> f.cancel(true));
          Thread.currentThread().interrupt();
          IOException interruptedFailure =
              new IOException("Interrupted while moving files from " + sourceDir + " to " + destDir, e);
          if (firstFailure == null) {
            firstFailure = interruptedFailure;
          } else {
            firstFailure.addSuppressed(interruptedFailure);
          }
          break;
        } catch (Exception e) {
          Throwable cause = e.getCause() != null ? e.getCause() : e;
          if (firstFailure == null) {
            firstFailure = cause instanceof IOException ? (IOException) cause
                : new IOException("Failed to move files from " + sourceDir + " to " + destDir, cause);
          } else {
            firstFailure.addSuppressed(cause);
          }
        }
      }
      if (firstFailure != null) {
        throw firstFailure;
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /// Resolve staging-copy parallelism from job `executionFrameworkSpec.extraConfigs`.
  /// Missing/invalid/non-positive values fall back to [#DEFAULT_STAGING_COPY_PARALLELISM].
  /// Values above [#MAX_STAGING_COPY_PARALLELISM] are capped.
  public static int getStagingCopyParallelism(Map<String, String> extraConfigs) {
    if (extraConfigs == null) {
      return DEFAULT_STAGING_COPY_PARALLELISM;
    }
    String value = extraConfigs.get(STAGING_COPY_PARALLELISM);
    if (value == null || value.isEmpty()) {
      return DEFAULT_STAGING_COPY_PARALLELISM;
    }
    try {
      int parallelism = Integer.parseInt(value.trim());
      if (parallelism < 1) {
        LOGGER.warn("Invalid {}={}, using default {}", STAGING_COPY_PARALLELISM, value,
            DEFAULT_STAGING_COPY_PARALLELISM);
        return DEFAULT_STAGING_COPY_PARALLELISM;
      }
      if (parallelism > MAX_STAGING_COPY_PARALLELISM) {
        LOGGER.warn("Capping {}={} to max {}", STAGING_COPY_PARALLELISM, parallelism, MAX_STAGING_COPY_PARALLELISM);
        return MAX_STAGING_COPY_PARALLELISM;
      }
      return parallelism;
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid {}={}, using default {}", STAGING_COPY_PARALLELISM, value, DEFAULT_STAGING_COPY_PARALLELISM);
      return DEFAULT_STAGING_COPY_PARALLELISM;
    }
  }

  private static List<URI> listSourceFiles(PinotFS fs, URI sourceDir)
      throws IOException, URISyntaxException {
    List<URI> sourceFileUris = new ArrayList<>();
    try {
      // Recursive listings include directory entries on several implementations, so always filter them out.
      for (FileMetadata fileMetadata : fs.listFilesWithMetadata(sourceDir, true)) {
        if (!fileMetadata.isDirectory()) {
          sourceFileUris.add(SegmentGenerationUtils.getFileURI(fileMetadata.getFilePath(), sourceDir));
        }
      }
    } catch (UnsupportedOperationException e) {
      // The PinotFS SPI default throws this when the implementation has no metadata listing: fall back to
      // listFiles() plus one isDirectory() stat per entry. IOExceptions are left to propagate.
      sourceFileUris.clear();
      for (String sourcePath : fs.listFiles(sourceDir, true)) {
        URI sourceFileUri = SegmentGenerationUtils.getFileURI(sourcePath, sourceDir);
        if (!fs.isDirectory(sourceFileUri)) {
          sourceFileUris.add(sourceFileUri);
        }
      }
    }
    return sourceFileUris;
  }

  private static void moveOneFile(PinotFS fs, URI sourceDir, URI sourceFileUri, URI destDir, boolean overwrite)
      throws IOException, URISyntaxException {
    String sourceFilename = SegmentGenerationUtils.getFileName(sourceFileUri);
    URI destFileUri =
        SegmentGenerationUtils.getRelativeOutputPath(sourceDir, sourceFileUri, destDir).resolve(sourceFilename);
    if (!overwrite && fs.exists(destFileUri)) {
      LOGGER.warn("Can't overwrite existing output segment tar file: {}", destFileUri);
      return;
    }
    // The exists() check above is only a cheap short-circuit; passing the flag down lets PinotFS.move reject a
    // destination that showed up in between, which parallel moves make more likely.
    if (!fs.move(sourceFileUri, destFileUri, overwrite)) {
      LOGGER.warn("Skipped moving {} to {}, move returned false (destination may already exist)", sourceFileUri,
          destFileUri);
    }
  }
}
