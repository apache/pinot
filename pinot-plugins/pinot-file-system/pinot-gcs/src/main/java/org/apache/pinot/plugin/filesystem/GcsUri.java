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

import com.google.common.base.Supplier;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Suppliers.memoize;
import static java.util.Objects.requireNonNull;


public class GcsUri {
  public static final String SCHEME = "gs";
  public static final String DELIMITER = "/";

  private final URI _uri;
  private final Supplier<String> _path;
  private final Supplier<String> _prefix;
  private final Supplier<Path> _absolutePath;

  public GcsUri(URI uri) {
    requireNonNull(uri, "uri is null");
    // The authority is the bucket
    requireNonNull(uri.getAuthority(), "uri authority is null");
    // Ensure that path is not null
    checkState(!uri.isOpaque(), "URI cannot be opaque");
    // Use uri.getAuthority() instead of uri.getHost():
    // Bucket names can contain _'s: https://cloud.google.com/storage/docs/naming-buckets
    _uri = createUri(uri.getAuthority(), uri.getPath().replaceAll(DELIMITER + "+", DELIMITER));
    _path = memoize(this::calculatePath);
    _prefix = memoize(this::calculatePrefix);
    _absolutePath = memoize(this::calculateAbsolutePath);
  }

  public String getBucketName() {
    return _uri.getAuthority();
  }

  /**
   * Get gcs path.
   * The gcs path must not be absolute.
   *
   * Example: bucket.create(path)
   * @return gcs path
   */
  public String getPath() {
    return _path.get();
  }

  private String calculatePath() {
    if (isNullOrEmpty(_uri.getPath()) || _uri.getPath().equals(DELIMITER)) {
      return "";
    }
    return _uri.getPath().substring(1);
  }

  public URI getUri() {
    return _uri;
  }

  /**
   * Get gcs search prefix.
   *
   * The path must end with the delimiter to ensure
   * searches do not return false positive matches.
   * Prefixes must not be absolute.
   *
   * Example: gs://bucket/dir/subdir should have a prefix of
   * dir/subdir/ otherwise listFiles will return matches for
   * objects with paths like dir/subdirA/file1, file2, etc.
   *
   * @return the path as a prefix
   */
  public String getPrefix() {
    return _prefix.get();
  }

  private String calculatePrefix() {
    String prefix = getPath();
    if (prefix.endsWith(DELIMITER)) {
      return prefix;
    }
    return prefix + DELIMITER;
  }

  public static GcsUri createGcsUri(String bucket, String path) {
    return new GcsUri(createUri(bucket, toAbsolutePath(path)));
  }

  private static URI createUri(String bucket, String path) {
    try {
      return new URI(SCHEME, bucket, path, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static String toAbsolutePath(String path) {
    if (path.startsWith(DELIMITER)) {
      return path;
    }
    return DELIMITER + path;
  }

  private Path calculateAbsolutePath() {
    return Paths.get(toAbsolutePath(getPath()));
  }

  public GcsUri resolve(String path) {
    Path relativePath = Paths.get(path);
    checkState(!relativePath.isAbsolute(), "path is absolute");
    String resolvedPath = _absolutePath.get().resolve(relativePath).toString();
    return createGcsUri(getBucketName(), resolvedPath);
  }

  /**
   * Returns true if this path contains the given subpath.
   *
   * @param subPath
   * @return
   */
  public boolean hasSubpath(GcsUri subPath) {
    Path relativePath = _absolutePath.get().relativize(subPath._absolutePath.get());
    return !relativePath.isAbsolute() && !relativePath.startsWith("..");
  }

  /**
   * Relativize a subdirectory.
   * The subPath must be a subdirectory of this path
   *
   * @param subPath The subpath of this path
   * @return Relativized path of this subdirectory
   *
   * Example:
   * The relativized path of /dir/subdir/subsubdir/subfile to gs://bucket/dir/subdir
   * would return subsubdir/subfile.
   */
  public String relativize(GcsUri subPath) {
    Path relativePath = _absolutePath.get().relativize(subPath._absolutePath.get());
    checkState(!relativePath.isAbsolute() && !relativePath.startsWith(".."), "Path '%s' is not a subdirectory of '%s'", _absolutePath.get(),
        subPath._absolutePath.get());
    return relativePath.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof GcsUri)) {
      return false;
    }
    GcsUri that = (GcsUri) other;
    return _uri.equals(that._uri);
  }

  @Override
  public int hashCode() {
    return _uri.hashCode();
  }

  @Override
  public String toString() {
    return _uri.toString();
  }
}
