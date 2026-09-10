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
package org.apache.pinot.segment.local.upsert;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.V1Constants;


/// Hashes exact saved bytes and deterministically combines typed file contributions. Byte inequality is only a
/// candidate: equivalent Roaring memberships can have different serializations. The per-segment cache is opt-in,
/// bounded to the two snapshot files, and never retains bitmap bytes or performs file I/O.
public final class UpsertSnapshotFingerprint {
  private UpsertSnapshotFingerprint() {
  }

  public static String hash(byte[] bytes) {
    return HexFormat.of().formatHex(digest().digest(bytes));
  }

  /// Input is the entire declared population, including unknown/retained files. Sorting makes traversal order
  /// irrelevant. Missing data CRCs or file hashes cannot become a complete content signature.
  public static UpsertSnapshotMetadata.Content aggregate(List<File> files) {
    List<File> ordered = new ArrayList<>(files);
    ordered.sort(Comparator.comparing(File::segmentName).thenComparing(File::bitmapType));
    MessageDigest population = digest();
    MessageDigest content = digest();
    update(population, "upsert-snapshot-population-v1");
    update(content, "upsert-snapshot-files-v1");
    int known = 0;
    boolean validPopulation = true;
    File previous = null;
    for (File file : ordered) {
      if (file.segmentDataCrc() == null || previous != null
          && previous.segmentName().equals(file.segmentName()) && previous.bitmapType().equals(file.bitmapType())) {
        validPopulation = false;
      }
      updateIdentity(population, file);
      updateIdentity(content, file);
      update(content, file.fileFingerprint());
      if (file.segmentDataCrc() != null && file.fileFingerprint() != null) {
        known++;
      }
      previous = file;
    }
    return new UpsertSnapshotMetadata.Content(UpsertSnapshotMetadata.Content.ALGORITHM,
        UpsertSnapshotMetadata.Content.SCOPE, validPopulation ? HexFormat.of().formatHex(population.digest()) : null,
        validPopulation && known == files.size() ? HexFormat.of().formatHex(content.digest()) : null,
        files.size(), known);
  }

  private static MessageDigest digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the Java platform", e);
    }
  }

  private static void update(MessageDigest digest, @Nullable String value) {
    byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
    int length = value != null ? bytes.length : -1;
    digest.update((byte) (length >>> 24));
    digest.update((byte) (length >>> 16));
    digest.update((byte) (length >>> 8));
    digest.update((byte) length);
    digest.update(bytes);
  }

  private static void updateIdentity(MessageDigest digest, File file) {
    update(digest, file.segmentName());
    update(digest, file.segmentDataCrc());
    update(digest, file.bitmapType());
  }

  /// This transient entry is used only for aggregation/confirmation, never serialized in the partition sidecar.
  public record File(String segmentName, @Nullable String segmentDataCrc, String bitmapType,
                     @Nullable String fileFingerprint) {
    public File {
      if (segmentDataCrc != null) {
        try {
          if (Long.parseLong(segmentDataCrc) < 0) {
            segmentDataCrc = null;
          }
        } catch (NumberFormatException e) {
          segmentDataCrc = null;
        }
      }
    }
  }

  /// Per immutable-segment cache. Coarse file changes invalidate before I/O; reads can seed only an unchanged
  /// generation. Overlapping changes are conservatively unknown, including a failed/partial file replacement.
  public static final class Cache {
    private final AtomicReference<State> _state = new AtomicReference<>(new State(0, null, null, false));

    public State read() {
      return _state.get();
    }

    public State beginChange(String fileName) {
      return _state.updateAndGet(state -> state.updateFile(state.activeChanges() + 1, fileName, null));
    }

    public void endChange(State started, String fileName, @Nullable String fingerprint) {
      _state.updateAndGet(state -> {
        String known = state == started && state.activeChanges() == 1 && !state.invalidated() ? fingerprint : null;
        return state.updateFile(state.activeChanges() - 1, fileName, known);
      });
    }

    public void seedRead(State before, String fileName, String fingerprint) {
      if (before.activeChanges() == 0 && !before.invalidated()) {
        _state.compareAndSet(before, before.updateFile(0, fileName, fingerprint));
      }
    }

    public void invalidate() {
      _state.updateAndGet(state -> new State(state.activeChanges(), null, null, true));
    }
  }

  /// One coherent observation of both cached contributions. Object identity is the generation token: every change
  /// creates a new state, so a stale reader/writer cannot publish over a newer one.
  /// Active/invalidated states are unknown.
  public record State(int activeChanges, @Nullable String valid, @Nullable String queryable,
                      boolean invalidated) {
    @Nullable
    public String fingerprint(boolean queryableBitmap) {
      if (activeChanges != 0 || invalidated) {
        return null;
      }
      return queryableBitmap ? queryable : valid;
    }

    private State updateFile(int writers, String fileName, @Nullable String fingerprint) {
      if (fileName.equals(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME)) {
        return new State(writers, fingerprint, queryable, invalidated);
      }
      return new State(writers, valid, fingerprint, invalidated);
    }
  }
}
