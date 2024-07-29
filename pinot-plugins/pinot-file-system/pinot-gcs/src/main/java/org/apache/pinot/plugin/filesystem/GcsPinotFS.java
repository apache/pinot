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
package org.apache.pinot.plugin.filesystem;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;


public class GcsPinotFS extends BasePinotFS {
  public static final String PROJECT_ID = "projectId";
  public static final String GCP_KEY = "gcpKey";
  public static final String JSON_KEY = "jsonKey";

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsPinotFS.class);
  // See https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
  private static final int BATCH_LIMIT = 100;
  private Storage _storage;

  @Override
  public void init(PinotConfiguration config) {
    Credentials credentials = null;
    try {
      StorageOptions.Builder storageBuilder = StorageOptions.newBuilder();
      if (!Strings.isNullOrEmpty(config.getProperty(PROJECT_ID))) {
        LOGGER.info("Configs are: {}, {}", PROJECT_ID, config.getProperty(PROJECT_ID));
        String projectId = config.getProperty(PROJECT_ID);
        storageBuilder.setProjectId(projectId);

        if (!Strings.isNullOrEmpty(config.getProperty(GCP_KEY))) {
          String gcpKey = config.getProperty(GCP_KEY);
          credentials = GoogleCredentials.fromStream(Files.newInputStream(Paths.get(gcpKey)));
        } else if (!Strings.isNullOrEmpty(config.getProperty(JSON_KEY))) {
          String decodedJsonKey = config.getProperty(JSON_KEY);
          Base64.Decoder decoder = Base64.getDecoder();
          try {
            byte[] decodedBytes = decoder.decode(decodedJsonKey);
            decodedJsonKey = new String(decodedBytes);
          } catch (IllegalArgumentException e) {
            // Ignore. Json key was not Base64 encoded.
            LOGGER.info("Failed to decode jsonKey, using as is");
          }
          credentials = GoogleCredentials.fromStream(IOUtils.toInputStream(decodedJsonKey, StandardCharsets.UTF_8));
        }
      }
      if (credentials == null) {
        LOGGER.info("Configs using default credential");
        credentials = GoogleCredentials.getApplicationDefault();
      }
      _storage = storageBuilder.setCredentials(credentials).build().getService();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public final boolean mkdir(URI uri)
      throws IOException {
    LOGGER.info("mkdir {}", uri);
    try {
      GcsUri gcsUri = new GcsUri(uri);
      // Prefix always returns a path with trailing /
      String directoryPath = gcsUri.getPrefix();
      // Do not create a bucket, different permissions are required
      // The bucket should already exist
      if (directoryPath.equals(GcsUri.DELIMITER)) {
        return true;
      }
      if (existsDirectoryOrBucket(gcsUri)) {
        return true;
      }
      Blob blob =
          _storage.create(BlobInfo.newBuilder(BlobId.of(gcsUri.getBucketName(), directoryPath)).build(), new byte[0]);
      return blob.exists();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    LOGGER.info("Deleting uri {} force {}", segmentUri, forceDelete);
    return delete(new GcsUri(segmentUri), forceDelete);
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    GcsUri srcGcsUri = new GcsUri(srcUri);
    GcsUri dstGcsUri = new GcsUri(dstUri);
    if (copy(srcGcsUri, dstGcsUri)) {
      // Only delete if all files were successfully copied
      return delete(srcGcsUri, true);
    }
    return false;
  }

  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
      throws IOException {
    LOGGER.info("Copying uri {} to uri {}", srcUri, dstUri);
    return copy(new GcsUri(srcUri), new GcsUri(dstUri));
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    if (fileUri == null) {
      return false;
    }
    return exists(new GcsUri(fileUri));
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    try {
      GcsUri gcsUri = new GcsUri(fileUri);
      checkState(!isPathTerminatedByDelimiter(gcsUri), "URI is a directory");
      Blob blob = getBlob(gcsUri);
      checkState(existsBlob(blob), "File '%s' does not exist", fileUri);
      return blob.getSize();
    } catch (Exception t) {
      throw new IOException(t);
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    return listFilesFromGcsUri(new GcsUri(fileUri), recursive);
  }

  private String[] listFilesFromGcsUri(GcsUri gcsFileUri, boolean recursive)
      throws IOException {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    String prefix = gcsFileUri.getPrefix();
    String bucketName = gcsFileUri.getBucketName();
    visitFiles(gcsFileUri, recursive, blob -> {
      if (!blob.getName().equals(prefix)) {
        builder.add(GcsUri.createGcsUri(bucketName, blob.getName()).toString());
      }
    });
    String[] listedFiles = builder.build().toArray(new String[0]);
    LOGGER.info("Listed {} files from URI: {}, is recursive: {}", listedFiles.length, gcsFileUri, recursive);
    return listedFiles;
  }

  @Override
  public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
      throws IOException {
    ImmutableList.Builder<FileMetadata> listBuilder = ImmutableList.builder();
    GcsUri gcsFileUri = new GcsUri(fileUri);
    String prefix = gcsFileUri.getPrefix();
    String bucketName = gcsFileUri.getBucketName();
    visitFiles(gcsFileUri, recursive, blob -> {
      if (!blob.getName().equals(prefix)) {
        // Note: isDirectory flag is only set when listing with BlobListOption.currentDirectory() i.e non-recursively.
        // For simplicity, we check if a path is directory by checking if it ends with '/', as done in S3PinotFS.
        boolean isDirectory = blob.getName().endsWith(GcsUri.DELIMITER);
        FileMetadata.Builder fileBuilder =
            new FileMetadata.Builder().setFilePath(GcsUri.createGcsUri(bucketName, blob.getName()).toString())
                .setLength(blob.getSize()).setIsDirectory(isDirectory);
        if (!isDirectory) {
          // Note: if it's a directory, updateTime is set to null, and calling this getter leads to NPE.
          // public Long getUpdateTime() { return updateTime; }. So skip this for directory.
          fileBuilder.setLastModifiedTime(blob.getUpdateTime());
        }
        listBuilder.add(fileBuilder.build());
      }
    });
    ImmutableList<FileMetadata> listedFiles = listBuilder.build();
    LOGGER.info("Listed {} files from URI: {}, is recursive: {}", listedFiles.size(), gcsFileUri, recursive);
    return listedFiles;
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    LOGGER.info("Copy {} to local {}", srcUri, dstFile.getAbsolutePath());
    checkState(!dstFile.isDirectory(), "File '%s' must not be a directory", dstFile);
    FileUtils.forceMkdir(dstFile.getParentFile());
    Blob blob = getBlob(new GcsUri(srcUri));
    checkState(existsBlob(blob), "File '%s' does not exists", srcUri);
    blob.downloadTo(dstFile.toPath());
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    LOGGER.info("Copying file {} to uri {}", srcFile.getAbsolutePath(), dstUri);
    GcsUri dstGcsUri = new GcsUri(dstUri);
    checkState(!isPathTerminatedByDelimiter(dstGcsUri), "Path '%s' must be a filename", dstGcsUri);
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(dstGcsUri.getBucketName(), dstGcsUri.getPath())).build();
    _storage.createFrom(blobInfo, Files.newInputStream(srcFile.toPath()));
  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    return existsDirectoryOrBucket(new GcsUri(uri));
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return getBlob(new GcsUri(uri)).getUpdateTime();
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    try {
      LOGGER.info("touch {}", uri);
      GcsUri gcsUri = new GcsUri(uri);
      Blob blob = getBlob(gcsUri);
      long updateTime = blob.getUpdateTime();
      _storage.update(blob.toBuilder().setMetadata(blob.getMetadata()).build());
      long newUpdateTime = getBlob(gcsUri).getUpdateTime();
      return newUpdateTime > updateTime;
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    try {
      Blob blob = getBlob(new GcsUri(uri));
      return Channels.newInputStream(blob.reader());
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  private Bucket getBucket(GcsUri gcsUri) {
    return _storage.get(gcsUri.getBucketName());
  }

  private Blob getBlob(GcsUri gcsUri)
      throws IOException {
    try {
      return _storage.get(BlobId.of(gcsUri.getBucketName(), gcsUri.getPath()));
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  private boolean existsBlob(Blob blob) {
    return blob != null && blob.exists();
  }

  private boolean existsFile(GcsUri gcsUri)
      throws IOException {
    return existsBlob(getBlob(gcsUri));
  }

  private boolean isPathTerminatedByDelimiter(GcsUri gcsUri) {
    return gcsUri.getPath().endsWith(GcsUri.DELIMITER);
  }

  /**
   * Returns true if this is an existing directory
   *
   * @param gcsUri
   * @return true if the directory exists
   * @throws IOException
   */
  private boolean existsDirectoryOrBucket(GcsUri gcsUri)
      throws IOException {
    String prefix = gcsUri.getPrefix();
    if (prefix.isEmpty()) {
      return true;
    }
    Blob blob = _storage.get(BlobId.of(gcsUri.getBucketName(), prefix));
    if (existsBlob(blob)) {
      return true;
    }

    try {
      // Return true if folder was not explicitly created but is a prefix of one or more files.
      // Use lazy iterable iterateAll() and verify that the iterator has elements.
      return _storage.list(gcsUri.getBucketName(), Storage.BlobListOption.prefix(prefix)).iterateAll().iterator()
          .hasNext();
    } catch (Exception t) {
      throw new IOException(t);
    }
  }

  private boolean isEmptyDirectory(GcsUri gcsUri)
      throws IOException {
    if (!existsDirectoryOrBucket(gcsUri)) {
      return false;
    }
    String prefix = gcsUri.getPrefix();
    boolean isEmpty = true;
    Page<Blob> page;
    if (prefix.equals(GcsUri.DELIMITER)) {
      page = getBucket(gcsUri).list();
    } else {
      page = _storage.list(gcsUri.getBucketName(), Storage.BlobListOption.prefix(prefix));
    }
    for (Blob blob : page.iterateAll()) {
      if (blob.getName().equals(prefix)) {
        continue;
      } else {
        isEmpty = false;
        break;
      }
    }
    return isEmpty;
  }

  private void visitFiles(GcsUri fileUri, boolean recursive, Consumer<Blob> visitor)
      throws IOException {
    try {
      String prefix = fileUri.getPrefix();
      Page<Blob> page;
      if (recursive) {
        page = _storage.list(fileUri.getBucketName(), Storage.BlobListOption.prefix(prefix));
      } else {
        page = _storage.list(fileUri.getBucketName(), Storage.BlobListOption.prefix(prefix),
            Storage.BlobListOption.currentDirectory());
      }
      page.iterateAll().forEach(visitor);
    } catch (Exception t) {
      throw new IOException(t);
    }
  }

  private boolean exists(GcsUri gcsUri)
      throws IOException {
    if (existsDirectoryOrBucket(gcsUri)) {
      return true;
    }
    if (isPathTerminatedByDelimiter(gcsUri)) {
      return false;
    }
    return existsFile(gcsUri);
  }

  private boolean delete(GcsUri segmentUri, boolean forceDelete)
      throws IOException {
    try {
      if (!exists(segmentUri)) {
        return forceDelete;
      }
      if (existsDirectoryOrBucket(segmentUri)) {
        if (!forceDelete && !isEmptyDirectory(segmentUri)) {
          return false;
        }
        String prefix = segmentUri.getPrefix();
        Page<Blob> page;
        if (prefix.equals(GcsUri.DELIMITER)) {
          page = getBucket(segmentUri).list();
        } else {
          page = _storage.list(segmentUri.getBucketName(), Storage.BlobListOption.prefix(prefix));
        }
        return batchDelete(page);
      } else {
        Blob blob = getBlob(segmentUri);
        return blob != null && blob.delete();
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception t) {
      throw new IOException(t);
    }
  }

  private boolean batchDelete(Page<Blob> page) {
    boolean deleteSucceeded = true;
    StorageBatch batch = _storage.batch();
    int batchSize = 0;
    List<StorageBatchResult<Boolean>> results = new ArrayList<>();
    for (Blob blob : page.iterateAll()) {
      results.add(batch.delete(blob.getBlobId()));
      batchSize++;
      if (batchSize >= BATCH_LIMIT) {
        batch.submit();
        deleteSucceeded &= results.stream().allMatch(r -> r != null && r.get());
        results = new ArrayList<>();
        batchSize = 0;
      }
    }
    if (batchSize > 0) {
      batch.submit();
      deleteSucceeded &= results.stream().allMatch(r -> r != null && r.get());
    }
    return deleteSucceeded;
  }

  private boolean copyFile(GcsUri srcUri, GcsUri dstUri)
      throws IOException {
    Blob blob = getBlob(srcUri);
    Blob newBlob =
        _storage.create(BlobInfo.newBuilder(BlobId.of(dstUri.getBucketName(), dstUri.getPath())).build(), new byte[0]);
    CopyWriter copyWriter = blob.copyTo(newBlob.getBlobId());
    copyWriter.getResult();
    return copyWriter.isDone() && blob.exists();
  }

  private boolean copy(GcsUri srcUri, GcsUri dstUri)
      throws IOException {
    if (!exists(srcUri)) {
      throw new IOException(String.format("Source URI '%s' does not exist", srcUri));
    }
    if (srcUri.equals(dstUri)) {
      return true;
    }
    // copy directly if source is a single file.
    if (!existsDirectoryOrBucket(srcUri)) {
      return copyFile(srcUri, dstUri);
    }
    // copy directory
    if (srcUri.hasSubpath(dstUri) || dstUri.hasSubpath(srcUri)) {
      throw new IOException(String.format("Cannot copy from or to a subdirectory: '%s' -> '%s'", srcUri, dstUri));
    }
    /**
     * If an non-empty blob exists and does not end with "/"
     * gcs will create a 0 length blob ending with "/".
     * So you can have something like a file with a subdirectory.
     * This is due to gcs behavior:
     *
     * @see https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
     */
    if (!existsDirectoryOrBucket(dstUri)) {
      mkdir(dstUri.getUri());
    }
    boolean copySucceeded = true;
    for (String directoryEntry : listFilesFromGcsUri(srcUri, true)) {
      GcsUri srcFile = new GcsUri(URI.create(directoryEntry));
      String relativeSrcPath = srcUri.relativize(srcFile);
      GcsUri dstFile = dstUri.resolve(relativeSrcPath);
      if (isPathTerminatedByDelimiter(srcFile)) {
        copySucceeded &= mkdir(dstFile.getUri());
      } else {
        copySucceeded &= copyFile(srcFile, dstFile);
      }
    }
    return copySucceeded;
  }
}
