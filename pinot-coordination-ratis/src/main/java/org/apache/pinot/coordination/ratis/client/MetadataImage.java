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
package org.apache.pinot.coordination.ratis.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangeTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;


/// Immutable client catalog image. A complete page is validated and staged before publication; an invalid record,
/// history, scope or transaction chain leaves the previous image untouched. Only changed family maps are copied.
/// Empty progress shares every record map and does not cause config decoding or listener notifications.
public final class MetadataImage {
  private final Set<RecordKind> _kinds;
  private final FeedCursor _cursor;
  private final Map<RecordKind, Map<String, CatalogRecord>> _families;
  private final Map<RecordKind, Map<UUID, String>> _objectNames;
  @Nullable
  private final ChangePage _lastPage;

  private MetadataImage(Set<RecordKind> kinds, FeedCursor cursor, Map<RecordKind, Map<String, CatalogRecord>> families,
      Map<RecordKind, Map<UUID, String>> objectNames, @Nullable ChangePage lastPage) {
    _kinds = Set.copyOf(kinds);
    _cursor = cursor;
    _families = Map.copyOf(families);
    _objectNames = Map.copyOf(objectNames);
    _lastPage = lastPage;
  }

  /// Explicitly installs a complete image, including a new history after the caller has authorized that reset.
  public static MetadataImage fromSnapshot(FeedScope scope, CatalogImage snapshot)
      throws IOException {
    require(scope != null && scope.kinds() != null && !scope.kinds().isEmpty(), "A nonempty feed scope is required");
    MetadataImage image = fromSnapshot(scope.kinds(), snapshot);
    require(scope.scopeId().equals(image.cursor().scopeId()), "Metadata snapshot belongs to another scope");
    return image;
  }

  /// Pins the opaque scope issued by an authenticated snapshot service while independently enforcing the requested
  /// kind filter. The caller must verify snapshot completeness and authorization before installing the image.
  public static MetadataImage fromSnapshot(Set<RecordKind> kinds, CatalogImage snapshot)
      throws IOException {
    require(kinds != null && !kinds.isEmpty() && kinds.stream().allMatch(Objects::nonNull),
        "A nonempty kind filter is required");
    require(snapshot != null && snapshot.records() != null, "A complete catalog snapshot is required");
    validateCursor(snapshot.cursor());
    Map<RecordKind, Map<String, CatalogRecord>> families = new EnumMap<>(RecordKind.class);
    Map<RecordKind, Map<UUID, String>> objectNames = new EnumMap<>(RecordKind.class);
    Set<UUID> objectIds = new HashSet<>();
    for (CatalogRecord record : snapshot.records()) {
      validateRecord(kinds, record, snapshot.cursor().index(), false);
      require(!record.deleted(), "A catalog image must contain only active records");
      require(objectIds.add(record.objectId()), "Duplicate object ID in catalog image");
      Map<String, CatalogRecord> family = families.computeIfAbsent(record.kind(), ignored -> new HashMap<>());
      require(family.putIfAbsent(record.name(), record) == null, "Duplicate record name in catalog image");
      objectNames.computeIfAbsent(record.kind(), ignored -> new HashMap<>()).put(record.objectId(), record.name());
    }
    families.replaceAll((kind, records) -> Map.copyOf(records));
    objectNames.replaceAll((kind, names) -> Map.copyOf(names));
    return new MetadataImage(kinds, snapshot.cursor(), families, objectNames, null);
  }

  /// Advances through complete transactions. An exact replay of the last bounded page is a no-op. Other stale pages,
  /// conflicting replays and gaps are rejected; reconnect from [#cursor()] or explicitly install a fresh snapshot.
  public MetadataImage apply(ChangePage page)
      throws IOException {
    require(page != null && page.status() == Status.OK, "Metadata feed page is unavailable: "
        + (page == null ? "null" : page.status()));
    if (page.equals(_lastPage)) {
      return this;
    }
    validateCursor(page.from());
    validateCursor(page.next());
    require(_cursor.equals(page.from()), "Metadata feed cursor is stale, gapped, or belongs to another history");
    require(_cursor.historyId().equals(page.next().historyId()), "Metadata history changed; install a snapshot");
    require(_cursor.scopeId().equals(page.next().scopeId()),
        "Metadata authorization scope changed; install a snapshot");
    require(page.retentionFloor() >= 0 && page.retentionFloor() <= _cursor.index(), "Metadata feed cursor expired");
    require(page.transactions() != null, "Metadata page must contain complete transactions");
    Map<RecordKind, Map<String, CatalogRecord>> families = new EnumMap<>(RecordKind.class);
    families.putAll(_families);
    Map<RecordKind, Map<UUID, String>> objectNames = new EnumMap<>(RecordKind.class);
    objectNames.putAll(_objectNames);
    Set<RecordKind> copied = new HashSet<>();
    long previous = _cursor.index();
    for (ChangeTransaction transaction : page.transactions()) {
      require(transaction != null && transaction.previousIndex() == previous && transaction.index() > previous
          && transaction.records() != null, "Metadata transaction chain has a gap or regression");
      Set<RecordKey> names = new HashSet<>();
      Set<UUID> objectIds = new HashSet<>();
      for (CatalogRecord record : transaction.records()) {
        validateRecord(_kinds, record, transaction.index(), true);
        require(names.add(new RecordKey(record.kind(), record.name())) && objectIds.add(record.objectId()),
            "Metadata transaction repeats an object or name");
        if (copied.add(record.kind())) {
          families.put(record.kind(), new HashMap<>(families.getOrDefault(record.kind(), Map.of())));
          objectNames.put(record.kind(), new HashMap<>(objectNames.getOrDefault(record.kind(), Map.of())));
        }
        Map<String, CatalogRecord> family = families.get(record.kind());
        CatalogRecord current = family.get(record.name());
        if (current == null) {
          require(!record.deleted() && record.version() == 1, "Metadata change requires a missing prior object");
          for (Map<UUID, String> namesById : objectNames.values()) {
            require(!namesById.containsKey(record.objectId()), "Metadata create reuses an active object ID");
          }
        } else {
          require(current.objectId().equals(record.objectId()) && current.version() < Long.MAX_VALUE
              && record.version() == current.version() + 1, "Metadata object incarnation or version has a gap");
        }
        if (record.deleted()) {
          family.remove(record.name());
          objectNames.get(record.kind()).remove(record.objectId());
        } else {
          family.put(record.name(), record);
          objectNames.get(record.kind()).put(record.objectId(), record.name());
        }
      }
      previous = transaction.index();
    }
    require(previous == page.next().index(), "Metadata page cursor does not cover its complete transaction chain");
    require(!page.transactions().isEmpty() || !page.hasMore(), "An empty metadata page must not request more data");
    for (RecordKind kind : copied) {
      families.put(kind, Map.copyOf(families.get(kind)));
      objectNames.put(kind, Map.copyOf(objectNames.get(kind)));
    }
    return new MetadataImage(_kinds, page.next(), families, objectNames, page);
  }

  public FeedCursor cursor() {
    return _cursor;
  }

  public Map<String, CatalogRecord> records(RecordKind kind) {
    return _families.getOrDefault(Objects.requireNonNull(kind, "kind"), Map.of());
  }

  private static void validateCursor(FeedCursor cursor)
      throws IOException {
    require(cursor != null && cursor.historyId() != null && cursor.index() >= 0
        && cursor.scopeId() != null && !cursor.scopeId().isBlank(),
        "Metadata cursor has an invalid history, revision or scope");
  }

  private static void validateRecord(Set<RecordKind> kinds, CatalogRecord record, long index, boolean change)
      throws IOException {
    require(record != null && record.kind() != null && kinds.contains(record.kind()) && record.name() != null
        && !record.name().isBlank() && record.objectId() != null && record.version() > 0 && record.schemaVersion() == 1
        && record.lastModifiedIndex() > 0 && record.lastModifiedIndex() <= index
        && (!change || record.lastModifiedIndex() == index), "Invalid or out-of-scope metadata record");
    JsonNode value = record.value();
    require(record.deleted() ? value == null || value.isNull() : value != null && value.isObject(),
        "Metadata record has an invalid typed value");
    require(record.kind() != RecordKind.CLUSTER_CONFIG || MetadataProtocol.CLUSTER_CONFIG_NAME.equals(record.name()),
        "Cluster config record has an invalid name");
  }

  private static void require(boolean valid, String message)
      throws IOException {
    if (!valid) {
      throw new IOException(message);
    }
  }

  private record RecordKey(RecordKind kind, String name) {
  }
}
