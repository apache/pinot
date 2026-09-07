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
package org.apache.pinot.segment.local.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;

/// Filesystem ownership and collection of immutable same-name segment replacements. Callers supply metadata already
/// loaded by normal controller validation; this class never reads or writes ZooKeeper. Concurrent uploads must use
/// fresh UUIDs, a bounded registration window, and refresh-only/If-Match guards. Expired URIs cannot be repushed.
public final class SegmentReplacementUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReplacementUtils.class);
  private static final String NAMESPACE = "segment-replacements-v1";
  public static final String ROOT_REFERENCES_CONFIG_KEY = "segment.replacement.output.roots.uri";
  private static final String ROOT_REFERENCES_DIR = ".segment-replacement-roots";
  static final String OBSOLETE_SUFFIX = ".obsolete";
  public static final long REGISTRATION_WINDOW_MS = TimeUnit.DAYS.toMillis(1);
  static final long RETENTION_MS = TimeUnit.DAYS.toMillis(3);

  private SegmentReplacementUtils() {
  }

  public static URI outputRoot(URI outputDir, String tableNameWithType) {
    String directory = outputDir.toString();
    return URI.create(directory + (directory.endsWith("/") ? "" : "/")
        + NAMESPACE + "/" + URIUtils.encode(tableNameWithType) + "/");
  }

  public static URI referencesDir(String dataDir, String tableNameWithType) {
    return URIUtils.getUri(dataDir, TableNameBuilder.extractRawTableName(tableNameWithType), ROOT_REFERENCES_DIR,
        URIUtils.encode(tableNameWithType) + "/");
  }

  /// Returns the registration deadline for a managed replacement URI, or null for a normal external segment URI.
  /// The controller enforces this deadline so an unreferenced output can be collected even when its CRC never changes.
  @Nullable
  public static Long registrationDeadline(String sourceUri, String tableNameWithType, String segmentName) {
    if (sourceUri == null) {
      return null;
    }
    String rawPath = URI.create(sourceUri).getRawPath();
    if (rawPath == null) {
      return null;
    }
    String[] parts = rawPath.split("/");
    int length = parts.length;
    if (length < 5 || !NAMESPACE.equals(parts[length - 5])) {
      return null;
    }
    checkState(URIUtils.decode(parts[length - 4]).equals(tableNameWithType)
            && URIUtils.decode(parts[length - 1]).equals(segmentName + ".tar.gz"),
        "Replacement URI must belong to the requested table and segment");
    checkState(UUID.fromString(parts[length - 2]).toString().equals(parts[length - 2]),
        "Invalid replacement attempt UUID");
    long deadline = Long.parseLong(parts[length - 3]);
    checkState(deadline > 0, "Invalid replacement registration deadline");
    return deadline;
  }

  /// Persist root discovery before copying an output, including roots from earlier task configurations.
  public static void rememberRoot(URI referencesDir, URI root) throws Exception {
    try (PinotFS fs = PinotFSFactory.create(referencesDir.getScheme())) {
      rememberRoot(fs, referencesDir, root);
    }
  }

  /// Called with the existing validation snapshot, including on tables with no time retention or minion schedule.
  /// A filesystem error skips collection and is retried by the next normal validation pass.
  public static void cleanup(String dataDir, String tableNameWithType, List<SegmentZKMetadata> segments) {
    if (dataDir == null) {
      return;
    }
    try {
      URI referencesDir = referencesDir(dataDir, tableNameWithType);
      try (PinotFS referenceFS = PinotFSFactory.create(referencesDir.getScheme())) {
        if (!referenceFS.exists(referencesDir)) {
          return;
        }
        Set<URI> roots = new HashSet<>();
        for (String path : referenceFS.listFiles(referencesDir, false)) {
          URI reference = SegmentPushUtils.generateSegmentTarURI(referencesDir, URI.create(path), null, null);
          if (!reference.getPath().endsWith(".root")) {
            continue;
          }
          try (InputStream input = referenceFS.open(reference)) {
            String root = new String(input.readNBytes(8193), StandardCharsets.UTF_8);
            checkState(root.length() <= 8192 && reference.getPath().endsWith(DigestUtils.sha256Hex(root) + ".root"),
                "Invalid segment replacement root reference: %s", reference);
            URI rootUri = URI.create(root);
            checkState(rootUri.getRawPath().endsWith("/" + NAMESPACE + "/" + URIUtils.encode(tableNameWithType) + "/"),
                "Replacement root must belong to the requested table: %s", root);
            roots.add(rootUri);
          }
        }
        Map<String, SegmentZKMetadata> segmentsByName = new HashMap<>();
        for (SegmentZKMetadata segment : segments) {
          segmentsByName.put(segment.getSegmentName(), segment);
        }
        for (URI root : roots) {
          try (PinotFS fs = PinotFSFactory.create(root.getScheme())) {
            cleanup(root, fs, segmentsByName, System.currentTimeMillis());
          } catch (Exception e) {
            LOGGER.warn("Unable to clean segment replacements under {}, will retry on the next validation", root, e);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Unable to clean replacements for {}, retrying on the next validation", tableNameWithType, e);
    }
  }

  @VisibleForTesting
  static void rememberRoot(PinotFS fs, URI referencesDir, URI root) throws Exception {
    String rootString = root.toString();
    checkState(rootString.length() <= 8192, "Segment replacement output root URI is too long");
    URI reference = referencesDir.resolve(DigestUtils.sha256Hex(rootString) + ".root");
    if (fs.exists(reference)) {
      try (InputStream input = fs.open(reference)) {
        if (rootString.equals(new String(input.readNBytes(8193), StandardCharsets.UTF_8))) {
          return;
        }
      }
    }
    File localReference = File.createTempFile("segment-output-root-", ".txt");
    try {
      Files.writeString(localReference.toPath(), rootString, StandardCharsets.UTF_8);
      fs.mkdir(referencesDir);
      fs.copyFromLocalFile(localReference, reference);
      try (InputStream input = fs.open(reference)) {
        checkState(rootString.equals(new String(input.readNBytes(8193), StandardCharsets.UTF_8)),
            "Failed to record segment replacement output root: %s", root);
      }
    } finally {
      FileUtils.deleteQuietly(localReference);
    }
  }

  @VisibleForTesting
  static void cleanup(URI root, PinotFS fs, Map<String, SegmentZKMetadata> segmentsByName, long nowMs)
      throws Exception {
    if (!fs.exists(root)) {
      return;
    }
    Set<URI> referencedUrls = new HashSet<>();
    for (SegmentZKMetadata segment : segmentsByName.values()) {
      if (segment.getDownloadUrl() != null) {
        referencedUrls.add(URI.create(segment.getDownloadUrl()));
      }
    }
    List<FileMetadata> files = fs.listFilesWithMetadata(root, true);
    Map<URI, FileMetadata> metadata = new HashMap<>();
    for (FileMetadata file : files) {
      metadata.put(SegmentPushUtils.generateSegmentTarURI(root, URI.create(file.getFilePath()), null, null), file);
    }
    for (FileMetadata file : files) {
      if (file.isDirectory()) {
        continue;
      }
      URI candidate = SegmentPushUtils.generateSegmentTarURI(root, URI.create(file.getFilePath()), null, null);
      String relative = root.relativize(candidate).toString();
      String[] parts = relative.split("/");
      // Only remove files in our exact <deadline>/<attempt UUID>/<segment>.tar.gz layout. Never delete directories.
      if (parts.length != 3) {
        continue;
      }
      long registrationDeadline;
      try {
        registrationDeadline = Long.parseLong(parts[0]);
        if (registrationDeadline <= 0) {
          continue;
        }
        if (!UUID.fromString(parts[1]).toString().equals(parts[1])) {
          continue;
        }
      } catch (IllegalArgumentException e) {
        continue;
      }
      if (parts[2].endsWith(".tar.gz" + OBSOLETE_SUFFIX)) {
        // Retry marker cleanup if the tar was removed but deleting its marker failed on a previous run.
        URI tar = URI.create(candidate.toString().substring(0,
            candidate.toString().length() - OBSOLETE_SUFFIX.length()));
        if (!fs.exists(tar)) {
          fs.delete(candidate, false);
        }
        continue;
      }
      if (!parts[2].endsWith(".tar.gz")) {
        continue;
      }
      String segmentName = URIUtils.decode(parts[2].substring(0, parts[2].length() - ".tar.gz".length()));
      // Registration is forbidden after the deadline. Keep outputs until then even if they are not currently live.
      // Keep outputs while the upload lock is held: a request can pause after checking its deadline.
      SegmentZKMetadata current = segmentsByName.get(segmentName);
      URI marker = URI.create(candidate + OBSOLETE_SUFFIX);
      FileMetadata obsolete = metadata.get(marker);
      boolean stillReferenced = current != null && (current.getDownloadUrl() == null
          || candidate.equals(URI.create(current.getDownloadUrl())));
      if (referencedUrls.contains(candidate) || stillReferenced || nowMs <= registrationDeadline
          || (current != null && current.getSegmentUploadStartTime() > 0)) {
        // If a previously obsolete URI is observed live again, restart the grace period before ever collecting it.
        if (obsolete != null) {
          fs.delete(marker, false);
        }
        continue;
      }
      // Age since upload is insufficient: an old object might have become obsolete only seconds ago. Start the
      // grace period at our first observation, giving existing server/minion downloads time to finish.
      if (obsolete == null) {
        if (!fs.touch(marker)) {
          LOGGER.warn("Could not mark replacement {} obsolete, retrying on the next validation", candidate);
        }
        continue;
      }
      long lastReferenceChange = current == null ? 0 : Math.max(current.getPushTime(), current.getRefreshTime());
      if (obsolete.getLastModifiedTime() <= 0
          || nowMs - Math.max(obsolete.getLastModifiedTime(), lastReferenceChange) < RETENTION_MS) {
        continue;
      }
      if (fs.delete(candidate, false)) {
        fs.delete(marker, false);
      } else {
        LOGGER.warn("Could not delete obsolete replacement {}, retrying on the next validation", candidate);
      }
    }
  }
}
