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
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static joptsimple.internal.Strings.isNullOrEmpty;

public class GcsPinotFS  extends PinotFS {
  public static final String PROJECT_ID = "projectId";
  public static final String GCP_KEY = "gcpKey";

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsPinotFS.class);
  private static final String SCHEME = "gs";
  private static final String DELIMITER = "/";
  private static final int BUFFER_SIZE = 128 * 1024;
  private Storage storage;

  @Override
  public void init(PinotConfiguration config) {
    Credentials credentials;

    try {
      StorageOptions.Builder storageBuilder = StorageOptions.newBuilder();
      if (!isNullOrEmpty(config.getProperty(PROJECT_ID)) && !isNullOrEmpty(config.getProperty(GCP_KEY))) {
        String projectId = config.getProperty(PROJECT_ID);
        String gcpKey = config.getProperty(GCP_KEY);
        storageBuilder.setProjectId(projectId);
        credentials = GoogleCredentials.fromStream(Files.newInputStream(Paths.get(gcpKey)));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
      storage = storageBuilder.setCredentials(credentials).build().getService();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Bucket getBucket(URI uri) {
    return storage.get(uri.getHost());
  }

  private Blob getBlob(URI uri) throws IOException {
    try {
      URI base = getBase(uri);
      String path = sanitizePath(base.relativize(uri).getPath());
      return getBucket(uri).get(path);
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }
  private boolean isPathTerminatedByDelimiter(URI uri) {
    return uri.getPath().endsWith(DELIMITER);
  }

  private String normalizeToDirectoryPrefix(URI uri) throws IOException {
    requireNonNull(uri, "uri is null");
    URI strippedUri = getBase(uri).relativize(uri);
    if (isPathTerminatedByDelimiter(strippedUri)) {
      return sanitizePath(strippedUri.getPath());
    }
    return sanitizePath(strippedUri.getPath() + DELIMITER);
  }

  private URI normalizeToDirectoryUri(URI uri) throws IOException {
    LOGGER.info("uri schema {}, host {}, sanitizePath {}", uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER));
    if (isPathTerminatedByDelimiter(uri)) {
      return uri;
    }
    try {
      LOGGER.info("uri schema {}, host {}, sanitizePath {}", uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER));
      return new URI(uri.getScheme(), uri.getHost() + "/", sanitizePath(uri.getPath() + DELIMITER), null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private String sanitizePath(String path) {
    path = path.replaceAll(DELIMITER + "+", DELIMITER);
    if (path.startsWith(DELIMITER) && !path.equals(DELIMITER)) {
      path = path.substring(1);
    }
    return path;
  }

  private URI getBase(URI uri) throws IOException {
    try {
      return new URI(SCHEME, uri.getHost(), DELIMITER, null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private boolean existsBlob(Blob blob) {
    return blob != null && blob.exists();
  }

  private boolean existsFile(URI uri) throws IOException {
      Blob blob = getBlob(uri);
      return existsBlob(blob);
  }

  /**
   * Determines if a path is a directory that is not empty
   * @param uri The path under the gcs bucket
   * @return {@code true} if the path is a non-empty directory,
   *         {@code false} otherwise
   */
  private boolean isEmptyDirectory(URI uri) throws IOException {
    if (!isDirectory(uri)) {
      return false;
    }
    String prefix = normalizeToDirectoryPrefix(uri);
    boolean isEmpty = true;
    Page<Blob> page;
    if (prefix.equals(DELIMITER)) {
      page = getBucket(uri).list();
    } else {
      page = getBucket(uri).list(Storage.BlobListOption.prefix(prefix));
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

  private boolean copyFile(URI srcUri, URI dstUri) throws IOException {
    Blob blob = getBlob(srcUri);
    Blob newBlob = getBucket(dstUri).create(sanitizePath(getBase(dstUri).relativize(dstUri).getPath()), new byte[0]);
    CopyWriter copyWriter = blob.copyTo(newBlob.getBlobId());
    copyWriter.getResult();
    return copyWriter.isDone();
  }

  @Override
  public boolean mkdir(URI uri) throws IOException {
    LOGGER.info("mkdir {}", uri);
    try {
      requireNonNull(uri, "uri is null");
      String path = normalizeToDirectoryPrefix(uri);
      // Bucket root directory already exists and cannot be created
      if (path.equals(DELIMITER)) {
        return true;
      }
      Blob blob = getBucket(uri).create(normalizeToDirectoryPrefix(uri), new byte[0]);
      return blob.exists();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete) throws IOException {
    LOGGER.info("Deleting uri {} force {}", segmentUri, forceDelete);
    try {
      if (!exists(segmentUri)) {
        return forceDelete;
      }
      if (isDirectory(segmentUri)) {
        if (!forceDelete && !isEmptyDirectory(segmentUri)) {
          return false;
        }
        String prefix = normalizeToDirectoryPrefix(segmentUri);
        Page<Blob> page;
        if (prefix.equals(DELIMITER)) {
          page = getBucket(segmentUri).list();
        } else {
          page = getBucket(segmentUri).list(Storage.BlobListOption.prefix(prefix));
        }
        boolean deleteSucceeded = true;
        for (Blob blob : page.iterateAll()) {
          deleteSucceeded &= blob.delete();
        }
        return deleteSucceeded;
      } else {
        Blob blob = getBlob(segmentUri);
        return blob != null && blob.delete();
      }
    } catch (IOException e) {
      throw e;
    } catch(Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri) throws IOException {
    if (copy(srcUri, dstUri)) {
      return  delete(srcUri, true);
    }
    return false;
  }

  /**
   * Copy srcUri to dstUri. If copy fails attempt to delete dstUri.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return {@code true} if copy succeeded otherwise return {@code false}
   * @throws IOException
   */
  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    LOGGER.info("Copying uri {} to uri {}", srcUri, dstUri);
    checkState(exists(srcUri), "Source URI '%s' does not exist", srcUri);
    if (srcUri.equals(dstUri)) {
      return true;
    }
    if (!isDirectory(srcUri)) {
      delete(dstUri, true);
      return copyFile(srcUri, dstUri);
    }
    dstUri = normalizeToDirectoryUri(dstUri);
    ImmutableList.Builder<URI> builder = ImmutableList.builder();
    Path srcPath = Paths.get(srcUri.getPath());
    try {
      boolean copySucceeded = true;
      for (String directoryEntry : listFiles(srcUri, true)) {
        URI src = new URI(srcUri.getScheme(), srcUri.getHost(), directoryEntry, null);
        String relativeSrcPath = srcPath.relativize(Paths.get(directoryEntry)).toString();
        String dstPath = dstUri.resolve(relativeSrcPath).getPath();
        URI dst = new URI(dstUri.getScheme(), dstUri.getHost(), dstPath, null);
        copySucceeded &= copyFile(src, dst);
      }
      return copySucceeded;
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    if (isDirectory(fileUri)) {
      return true;
    }
    if (isPathTerminatedByDelimiter(fileUri)) {
      return false;
    }
    return existsFile(fileUri);
  }

  @Override
  public long length(URI fileUri) throws IOException {
    try {
      checkState(!isPathTerminatedByDelimiter(fileUri), "URI is a directory");
      Blob blob = getBlob(fileUri);
      checkState(existsBlob(blob), "File '%s' does not exist", fileUri);
      return blob.getSize();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
    try {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      String prefix = normalizeToDirectoryPrefix(fileUri);
      Page<Blob> page;
      if (recursive) {
        page = storage.list(fileUri.getHost(), Storage.BlobListOption.prefix(prefix));
      } else {
        page = storage.list(fileUri.getHost(), Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.currentDirectory());
      }
      page.iterateAll()
          .forEach(blob -> {
            if (!blob.getName().equals(prefix)) {
              try {
                builder.add(new URI(SCHEME, fileUri.getHost(), DELIMITER + blob.getName(), null).toString());
              } catch (URISyntaxException e) {
                throw new RuntimeException(e);
              }
            }
      });
      String[] listedFiles = builder.build().toArray(new String[0]);
      LOGGER.info("Listed {} files from URI: {}, is recursive: {}", listedFiles.length, fileUri, recursive);
      return listedFiles;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
    LOGGER.info("Copy {} to local {}", srcUri, dstFile.getAbsolutePath());
    FileUtils.forceMkdir(dstFile.getParentFile());
    Blob blob = getBlob(srcUri);
    checkState(existsBlob(blob), "File '%s' does not exists", srcUri);
    blob.downloadTo(dstFile.toPath());
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    LOGGER.info("Copying file {} to uri {}", srcFile.getAbsolutePath(), dstUri);
    Bucket bucket = getBucket(dstUri);
    Blob blob = bucket.create(sanitizePath(dstUri.getPath()), new byte[0]);
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
  public boolean isDirectory(URI uri) throws IOException {
    String prefix = normalizeToDirectoryPrefix(uri);
    if (prefix.equals(DELIMITER)) {
      return true;
    }
    Blob blob = getBucket(uri).get(prefix);
    if (blob != null) {
      return blob.exists();
    }

    try {
      // Return true if folder was not explicitly created but is a prefix of one or more files.
      // Use lazy iterable iterateAll() and verify that the iterator has elements.
      return getBucket(uri).list(Storage.BlobListOption.prefix(prefix))
              .iterateAll()
              .iterator()
              .hasNext();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public long lastModified(URI uri) throws IOException {
    return getBlob(uri).getUpdateTime();
  }

  @Override
  public boolean touch(URI uri) throws IOException {
    try {
      Blob blob = getBlob(uri);
      long updateTime = blob.getUpdateTime();
      storage.update(blob.toBuilder().setMetadata(blob.getMetadata()).build());
      long newUpdateTime = getBlob(uri).getUpdateTime();
      return newUpdateTime > updateTime;
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(URI uri) throws IOException {
    try {
      Blob blob = getBlob(uri);
      return Channels.newInputStream(blob.reader());
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }
}
