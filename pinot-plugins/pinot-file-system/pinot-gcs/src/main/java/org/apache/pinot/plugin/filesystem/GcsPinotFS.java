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
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.pinot.plugin.filesystem.GcsUri.createGcsUri;


public class GcsPinotFS extends PinotFS {
  public static final String PROJECT_ID = "projectId";
  public static final String GCP_KEY = "gcpKey";

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsPinotFS.class);
  private static final int BUFFER_SIZE = 128 * 1024;
  // See https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
  private static final int BATCH_LIMIT = 100;
  private Storage _storage;

  @Override
  public void init(PinotConfiguration config) {
    Credentials credentials;

    try {
      StorageOptions.Builder storageBuilder = StorageOptions.newBuilder();
      if (!Strings.isNullOrEmpty(config.getProperty(PROJECT_ID)) && !Strings.isNullOrEmpty(config.getProperty(GCP_KEY))) {
        LOGGER.info("Configs are: {}, {}", PROJECT_ID, config.getProperty(PROJECT_ID));
        String projectId = config.getProperty(PROJECT_ID);
        String gcpKey = config.getProperty(GCP_KEY);
        storageBuilder.setProjectId(projectId);
        credentials = GoogleCredentials.fromStream(Files.newInputStream(Paths.get(gcpKey)));
      } else {
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
      if (existsDirectory(gcsUri)) {
        return true;
      }
      Blob blob = getBucket(gcsUri).create(directoryPath, new byte[0]);
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
      // Only delete if all files were successfully moved
      return delete(srcGcsUri, true);
    }
    return false;
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri)
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
    return listFiles(new GcsUri(fileUri), recursive);
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
    Blob blob = getBucket(dstGcsUri).create(dstGcsUri.getPath(), new byte[0]);
    WriteChannel writeChannel = blob.writer();
    writeChannel.setChunkSize(BUFFER_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    SeekableByteChannel channel = Files.newByteChannel(srcFile.toPath());
    for (int bytesRead = channel.read(buffer); bytesRead != -1; bytesRead = channel.read(buffer)) {
      buffer.flip();
      writeChannel.write(buffer);
      buffer.clear();
    }
    writeChannel.close();
  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    return existsDirectory(new GcsUri(uri));
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
      return getBucket(gcsUri).get(gcsUri.getPath());
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
  private boolean existsDirectory(GcsUri gcsUri)
      throws IOException {
    String prefix = gcsUri.getPrefix();
    if (prefix.equals(GcsUri.DELIMITER)) {
      return true;
    }
    Blob blob = getBucket(gcsUri).get(prefix);
    if (existsBlob(blob)) {
      return true;
    }

    try {
      // Return true if folder was not explicitly created but is a prefix of one or more files.
      // Use lazy iterable iterateAll() and verify that the iterator has elements.
      return getBucket(gcsUri).list(Storage.BlobListOption.prefix(prefix)).iterateAll().iterator().hasNext();
    } catch (Exception t) {
      throw new IOException(t);
    }
  }

  private boolean isEmptyDirectory(GcsUri gcsUri)
      throws IOException {
    if (!existsDirectory(gcsUri)) {
      return false;
    }
    String prefix = gcsUri.getPrefix();
    boolean isEmpty = true;
    Page<Blob> page;
    if (prefix.equals(GcsUri.DELIMITER)) {
      page = getBucket(gcsUri).list();
    } else {
      page = getBucket(gcsUri).list(Storage.BlobListOption.prefix(prefix));
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

  private String[] listFiles(GcsUri fileUri, boolean recursive)
      throws IOException {
    try {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      String prefix = fileUri.getPrefix();
      Page<Blob> page;
      if (recursive) {
        page = _storage.list(fileUri.getBucketName(), Storage.BlobListOption.prefix(prefix));
      } else {
        page = _storage.list(fileUri.getBucketName(), Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.currentDirectory());
      }
      page.iterateAll().forEach(blob -> {
        if (!blob.getName().equals(prefix)) {
          builder.add(createGcsUri(fileUri.getBucketName(), blob.getName()).toString());
        }
      });
      String[] listedFiles = builder.build().toArray(new String[0]);
      LOGGER.info("Listed {} files from URI: {}, is recursive: {}", listedFiles.length, fileUri, recursive);
      return listedFiles;
    } catch (Exception t) {
      throw new IOException(t);
    }
  }

  private boolean exists(GcsUri gcsUri)
      throws IOException {
    if (existsDirectory(gcsUri)) {
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
      if (existsDirectory(segmentUri)) {
        if (!forceDelete && !isEmptyDirectory(segmentUri)) {
          return false;
        }
        String prefix = segmentUri.getPrefix();
        Page<Blob> page;
        if (prefix.equals(GcsUri.DELIMITER)) {
          page = getBucket(segmentUri).list();
        } else {
          page = getBucket(segmentUri).list(Storage.BlobListOption.prefix(prefix));
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
    Blob newBlob = getBucket(dstUri).create(dstUri.getPath(), new byte[0]);
    CopyWriter copyWriter = blob.copyTo(newBlob.getBlobId());
    copyWriter.getResult();
    return copyWriter.isDone() && blob.exists();
  }

  private boolean copy(GcsUri srcUri, GcsUri dstUri)
      throws IOException {
    if (!exists(srcUri)) {
      throw new IOException(format("Source URI '%s' does not exist", srcUri));
    }
    if (srcUri.equals(dstUri)) {
      return true;
    }
    if (!existsDirectory(srcUri)) {
      return copyFile(srcUri, dstUri);
    }
    if (srcUri.hasSubpath(dstUri) || dstUri.hasSubpath(srcUri)) {
      throw new IOException(format("Cannot copy from or to a subdirectory: '%s' -> '%s'", srcUri, dstUri));
    }
    /**
     * If an non-empty blob exists and does not end with "/"
     * gcs will create a 0 length blob ending with "/".
     * So you can have something like a file with a subdirectory.
     * This is due to gcs behavior:
     *
     * @see https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
     */
    if (!existsDirectory(dstUri)) {
      mkdir(dstUri.getUri());
    }
    boolean copySucceeded = true;
    for (String directoryEntry : listFiles(srcUri, true)) {
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
