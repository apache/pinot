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
package org.apache.pinot.broker.querylog;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.QueryLogSystemTableUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Keeps an in-memory copy of recent query log records and evaluates {@code SELECT} queries against it.
 */
public class QueryLogSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogSystemTable.class);

  private static final QueryLogSystemTable INSTANCE = new QueryLogSystemTable();

  public static final String FULL_TABLE_NAME = QueryLogSystemTableUtils.FULL_TABLE_NAME;

  private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  private final AtomicBoolean _initialized = new AtomicBoolean(false);

  private volatile boolean _enabled;
  private volatile int _maxEntries;
  private volatile long _retentionMs;
  private volatile int _defaultLimit;
  private volatile QueryLogStore _store;
  private volatile String _storageType;

  private QueryLogSystemTable() {
  }

  public static QueryLogSystemTable getInstance() {
    return INSTANCE;
  }

  public void initIfNeeded(PinotConfiguration config) {
    if (_initialized.compareAndSet(false, true)) {
      _enabled = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED,
          CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_ENABLED);
      _maxEntries = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES,
          CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES);
      _retentionMs = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_RETENTION_MS,
          CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_RETENTION_MS);
      _defaultLimit = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT,
          CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_DEFAULT_LIMIT);
      _storageType = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_STORAGE,
          CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_STORAGE);
      if (_enabled) {
        try {
          if ("disk".equalsIgnoreCase(_storageType)) {
            String directory = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_DIR,
                CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_DISK_DIR);
            long maxBytes = config.getProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_MAX_BYTES,
                CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_DISK_MAX_BYTES);
            long segmentBytes = config.getProperty(
                CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_DISK_SEGMENT_BYTES,
                CommonConstants.Broker.DEFAULT_QUERY_LOG_SYSTEM_TABLE_DISK_SEGMENT_BYTES);
            segmentBytes = Math.min(segmentBytes, maxBytes);
            _store =
                new DiskBackedQueryLogStore(Paths.get(directory), maxBytes, segmentBytes, _maxEntries, _retentionMs);
          } else {
            _store = new InMemoryQueryLogStore(_maxEntries, _retentionMs);
          }
        } catch (IOException e) {
          LOGGER.error("Failed to initialize query log system table storage", e);
          _enabled = false;
          _store = null;
        }
      }
      LOGGER.info(
          "Initialized query log system table: enabled={}, storage={}, maxEntries={}, retentionMs={}, defaultLimit={}",
          _enabled, _storageType, _maxEntries, _retentionMs, _defaultLimit);
    }
  }

  public boolean isEnabled() {
    return _enabled;
  }

  public void append(QueryLogRecord record) {
    if (!_enabled || record == null || _store == null) {
      return;
    }
    _lock.writeLock().lock();
    try {
      _store.append(record);
    } catch (IOException e) {
      LOGGER.warn("Failed to append query log record", e);
    } finally {
      _lock.writeLock().unlock();
    }
  }

  @Nullable
  public BrokerResponse handleIfSystemTable(SqlNodeAndOptions sqlNodeAndOptions)
      throws BadQueryRequestException {
    if (!_enabled) {
      return null;
    }
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    if (!QueryLogSystemTableUtils.isQueryLogSystemTableQuery(sqlNode)) {
      return null;
    }
    ParsedQuery parsedQuery = parse(sqlNode);
    if (parsedQuery == null) {
      return null;
    }
    return execute(parsedQuery);
  }

  private ParsedQuery parse(SqlNode sqlNode)
      throws BadQueryRequestException {
    SqlNode workingNode = sqlNode;
    SqlNodeList outerOrderBy = null;
    SqlNode outerOffset = null;
    SqlNode outerFetch = null;
    if (workingNode instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) workingNode;
      outerOrderBy = orderBy.orderList;
      outerOffset = orderBy.offset;
      outerFetch = orderBy.fetch;
      workingNode = orderBy.query;
    }

    if (!(workingNode instanceof SqlSelect)) {
      return null;
    }
    SqlSelect select = (SqlSelect) workingNode;
    if (select.getHaving() != null) {
      throw new BadQueryRequestException("HAVING is not supported for system.query_log");
    }
    if (select.isDistinct()) {
      throw new BadQueryRequestException("DISTINCT is not supported for system.query_log");
    }

    SqlNodeList selectList = select.getSelectList();
    SqlNode where = select.getWhere();
    SqlNodeList orderList = outerOrderBy != null ? outerOrderBy : select.getOrderList();
    SqlNode offset = outerOffset != null ? outerOffset : select.getOffset();
    SqlNode fetch = outerFetch != null ? outerFetch : select.getFetch();
    SqlNodeList groupBy = select.getGroup();

    return new ParsedQuery(selectList, where, orderList, offset, fetch, groupBy);
  }

  private BrokerResponse execute(ParsedQuery query)
      throws BadQueryRequestException {
    long startTimeMs = System.currentTimeMillis();
    AggregationQuery aggregationQuery = resolveAggregationQuery(query._selectList, query._groupBy);
    List<SelectedColumn> selections = null;
    Map<String, QueryLogColumn> aliasLookup;
    if (aggregationQuery == null) {
      aliasLookup = new HashMap<>();
      selections = resolveSelectList(query._selectList);
      for (SelectedColumn selectedColumn : selections) {
        aliasLookup.put(selectedColumn._outputName.toLowerCase(Locale.ROOT), selectedColumn._column);
      }
    } else {
      aliasLookup = Collections.emptyMap();
    }

    Predicate<QueryLogRecord> predicate = buildPredicate(query._whereClause, aliasLookup);
    if (aggregationQuery != null) {
      return executeAggregation(aggregationQuery, predicate, query._orderBy, query._offsetNode, query._fetchNode);
    }

    List<Ordering> orderings = parseOrderings(query._orderBy, aliasLookup, selections);
    int offset = parseNonNegativeInt(query._offsetNode, "OFFSET", 0);
    int limit = parseNonNegativeInt(query._fetchNode, "LIMIT", _defaultLimit);

    List<QueryLogRecord> rows;
    try {
      rows = getFilteredRecords(predicate);
    } catch (IOException e) {
      throw new BadQueryRequestException("Failed to read query log storage", e);
    }
    // If no ORDER BY clause is specified, default to ordering by timestampMs DESC.
    Comparator<QueryLogRecord> comparator = orderings.isEmpty()
        ? Comparator.comparingLong(QueryLogRecord::getLogTimestampMs).reversed()
        : buildComparator(orderings);
    rows.sort(comparator);

    int fromIndex = Math.min(offset, rows.size());
    int toIndex = limit < 0 ? rows.size() : Math.min(rows.size(), fromIndex + limit);
    List<QueryLogRecord> window = rows.subList(fromIndex, toIndex);

    String[] columnNames = selections.stream().map(selection -> selection._outputName).toArray(String[]::new);
    DataSchema.ColumnDataType[] columnDataTypes = selections.stream()
        .map(selection -> selection._column._dataType).toArray(DataSchema.ColumnDataType[]::new);

    List<Object[]> tableRows = new ArrayList<>(window.size());
    for (QueryLogRecord record : window) {
      Object[] row = new Object[selections.size()];
      for (int i = 0; i < selections.size(); i++) {
        row[i] = selections.get(i)._column.extract(record);
      }
      tableRows.add(row);
    }

    ResultTable resultTable = new ResultTable(new DataSchema(columnNames, columnDataTypes), tableRows);
    BrokerResponseNative response = new BrokerResponseNative();
    response.setResultTable(resultTable);
    response.setNumRowsResultSet(resultTable.getRows().size());
    response.setTablesQueried(Set.of(QueryLogSystemTableUtils.FULL_TABLE_NAME));
    response.setTimeUsedMs(System.currentTimeMillis() - startTimeMs);
    return response;
  }

  private List<QueryLogRecord> getFilteredRecords(@Nullable Predicate<QueryLogRecord> predicate)
      throws IOException {
    if (_store == null) {
      return Collections.emptyList();
    }
    _lock.readLock().lock();
    try {
      List<QueryLogRecord> records = _store.getRecords();
      if (records.isEmpty()) {
        return Collections.emptyList();
      }
      List<QueryLogRecord> filtered = new ArrayList<>(records);
      if (_retentionMs > 0) {
        long cutoff = System.currentTimeMillis() - _retentionMs;
        filtered.removeIf(record -> record.getLogTimestampMs() < cutoff);
      }
      if (filtered.isEmpty()) {
        return Collections.emptyList();
      }
      if (_maxEntries > 0 && filtered.size() > _maxEntries) {
        filtered = new ArrayList<>(filtered.subList(filtered.size() - _maxEntries, filtered.size()));
      }
      if (predicate != null) {
        filtered.removeIf(record -> !predicate.test(record));
      }
      return filtered;
    } finally {
      _lock.readLock().unlock();
    }
  }

  private interface QueryLogStore extends AutoCloseable {
    void append(QueryLogRecord record)
        throws IOException;

    List<QueryLogRecord> getRecords()
        throws IOException;

    @Override
    void close()
        throws IOException;
  }

  private static final class InMemoryQueryLogStore implements QueryLogStore {
    private final Deque<QueryLogRecord> _records = new ArrayDeque<>();
    private final int _maxEntries;
    private final long _retentionMs;

    InMemoryQueryLogStore(int maxEntries, long retentionMs) {
      _maxEntries = maxEntries;
      _retentionMs = retentionMs;
    }

    @Override
    public void append(QueryLogRecord record) {
      if (_retentionMs > 0) {
        long cutoff = record.getLogTimestampMs() - _retentionMs;
        while (!_records.isEmpty() && _records.peekFirst().getLogTimestampMs() < cutoff) {
          _records.removeFirst();
        }
      }
      _records.addLast(record);
      if (_maxEntries > 0) {
        while (_records.size() > _maxEntries) {
          _records.removeFirst();
        }
      }
    }

    @Override
    public List<QueryLogRecord> getRecords() {
      return new ArrayList<>(_records);
    }

    @Override
    public void close() {
      _records.clear();
    }
  }

  private static final class DiskBackedQueryLogStore implements QueryLogStore {
    private static final String SEGMENT_PREFIX = "segment_";

    private final Path _directory;
    private final long _maxBytes;
    private final long _segmentBytes;
    private final int _maxEntries;
    private final long _retentionMs;
    private final List<Path> _segments = new ArrayList<>();
    private final List<Integer> _segmentRecordCounts = new ArrayList<>();
    private DataOutputStream _currentOutput;
    private Path _currentSegment;
    private long _currentSegmentBytes;
    private long _totalBytes;
    private long _totalRecords;
    private final AtomicInteger _segmentCounter;

    DiskBackedQueryLogStore(Path directory, long maxBytes, long segmentBytes, int maxEntries, long retentionMs)
        throws IOException {
      _directory = directory;
      _maxBytes = Math.max(1L, maxBytes);
      _segmentBytes = Math.max(1L, segmentBytes);
      _maxEntries = maxEntries;
      _retentionMs = retentionMs;
      Files.createDirectories(_directory);
      try (Stream<Path> stream = Files.list(_directory)) {
        stream.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().startsWith(SEGMENT_PREFIX))
            .sorted()
            .forEach(path -> {
              _segments.add(path);
              _segmentRecordCounts.add(estimateRecordCount(path));
              try {
                _totalBytes += Files.size(path);
              } catch (IOException e) {
                LOGGER.warn("Failed to compute size for query log segment {}", path, e);
              }
            });
      }
      _totalRecords = _segmentRecordCounts.stream().mapToLong(i -> i).sum();
      _segmentCounter = new AtomicInteger(_segments.size());
      pruneByTimeIfNeeded(); // Ensure old segments are cleaned up on startup
      pruneByRowCountIfNeeded();
      pruneBySizeIfNeeded();
      openNewSegment();
    }

    @Override
    public void append(QueryLogRecord record)
        throws IOException {
      byte[] payload = QueryLogRecordSerDe.serialize(record);
      int entrySize = Integer.BYTES + payload.length;
      // Rotate segment if either full by size, or if retention requires time-roll
      // Skip rotation for empty segments to avoid creating unnecessary empty segment files.
      // We only time-rotate when the current segment already contains data.
      if (_currentSegmentBytes > 0 && shouldRotateForTime(record.getLogTimestampMs())) {
        openNewSegment();
      }
      if (_currentSegmentBytes + entrySize > _segmentBytes) {
        openNewSegment();
      }
      _currentOutput.writeInt(payload.length);
      _currentOutput.write(payload);
      _currentOutput.flush();
      _currentSegmentBytes += entrySize;
      _totalBytes += entrySize;
      // Update counts
      if (!_segmentRecordCounts.isEmpty()) {
        int lastIdx = _segmentRecordCounts.size() - 1;
        _segmentRecordCounts.set(lastIdx, _segmentRecordCounts.get(lastIdx) + 1);
      }
      _totalRecords += 1;
      // Apply pruning policies
      pruneByTimeIfNeeded();
      pruneByRowCountIfNeeded();
      pruneBySizeIfNeeded();
    }

    @Override
    public List<QueryLogRecord> getRecords()
        throws IOException {
      if (_segments.isEmpty()) {
        return Collections.emptyList();
      }
      List<QueryLogRecord> records = new ArrayList<>();
      for (Path segment : _segments) {
        records.addAll(readSegment(segment));
      }
      return records;
    }

    @Override
    public void close()
        throws IOException {
      closeCurrentOutput();
      _segments.clear();
      _segmentRecordCounts.clear();
      _totalBytes = 0;
      _currentSegmentBytes = 0;
      _totalRecords = 0;
    }

    private void openNewSegment()
        throws IOException {
      closeCurrentOutput();
      _currentSegment = _directory.resolve(String.format("%s%d_%d.log", SEGMENT_PREFIX, System.currentTimeMillis(),
          _segmentCounter.getAndIncrement()));
      Files.createFile(_currentSegment);
      _segments.add(_currentSegment);
      _segmentRecordCounts.add(0);
      _currentSegmentBytes = 0;
      _currentOutput = new DataOutputStream(new BufferedOutputStream(
          Files.newOutputStream(_currentSegment, StandardOpenOption.APPEND)));
    }

    private void closeCurrentOutput()
        throws IOException {
      if (_currentOutput != null) {
        _currentOutput.flush();
        _currentOutput.close();
        _currentOutput = null;
      }
    }

    private void pruneBySizeIfNeeded()
        throws IOException {
      while (_totalBytes > _maxBytes && _segments.size() > 1) {
        Path oldest = _segments.remove(0);
        int removedCount = _segmentRecordCounts.remove(0);
        try {
          long size = Files.size(oldest);
          Files.deleteIfExists(oldest);
          _totalBytes -= size;
          _totalRecords -= removedCount;
        } catch (IOException e) {
          LOGGER.warn("Failed to delete old query log segment {}", oldest, e);
          break;
        }
      }
      if (_totalBytes > _maxBytes && _segments.size() == 1) {
        LOGGER.warn("Query log disk storage exceeds maxBytes {} but cannot delete active segment", _maxBytes);
      }
    }

    private void pruneByRowCountIfNeeded()
        throws IOException {
      if (_maxEntries <= 0) {
        return;
      }
      while (_totalRecords > _maxEntries && _segments.size() > 1) {
        Path oldest = _segments.remove(0);
        int removedCount = _segmentRecordCounts.remove(0);
        try {
          long size = Files.size(oldest);
          Files.deleteIfExists(oldest);
          _totalBytes -= size;
          _totalRecords -= removedCount;
        } catch (IOException e) {
          LOGGER.warn("Failed to delete old query log segment {}", oldest, e);
          break;
        }
      }
      if (_totalRecords > _maxEntries && _segments.size() == 1) {
        LOGGER.warn("Query log disk storage exceeds maxEntries {} but cannot delete within active segment",
            _maxEntries);
      }
    }

    private void pruneByTimeIfNeeded()
        throws IOException {
      if (_retentionMs <= 0) {
        return;
      }
      long cutoff = System.currentTimeMillis() - _retentionMs;
      while (_segments.size() > 1 && segmentCreationTime(_segments.get(0)) < cutoff) {
        Path oldest = _segments.remove(0);
        int removedCount = _segmentRecordCounts.remove(0);
        try {
          long size = Files.size(oldest);
          Files.deleteIfExists(oldest);
          _totalBytes -= size;
          _totalRecords -= removedCount;
        } catch (IOException e) {
          LOGGER.warn("Failed to delete old query log segment {}", oldest, e);
          break;
        }
      }
      // do not delete the only active segment even if older than cutoff; it will rotate on next append if needed
    }

    private boolean shouldRotateForTime(long recordTimestampMs) {
      if (_retentionMs <= 0) {
        return false;
      }
      long cutoff = recordTimestampMs - _retentionMs;
      return segmentCreationTime(_currentSegment) < cutoff;
    }

    private long segmentCreationTime(Path segment) {
      // segment filename: segment_<timestamp>_<counter>.log
      String name = segment.getFileName().toString();
      try {
        int underscore = name.indexOf('_');
        int secondUnderscore = name.indexOf('_', underscore + 1);
        String ts = name.substring(underscore + 1, secondUnderscore);
        return Long.parseLong(ts);
      } catch (Exception e) {
        // fallback to file creation time if parse fails
        return 0L;
      }
    }

    private int estimateRecordCount(Path segment) {
      // Fast count by scanning int-length prefixes
      int count = 0;
      if (!Files.exists(segment)) {
        return 0;
      }
      try (DataInputStream input = new DataInputStream(
          new BufferedInputStream(Files.newInputStream(segment, StandardOpenOption.READ)))) {
        while (true) {
          int length;
          try {
            length = input.readInt();
          } catch (EOFException e) {
            break;
          }
          if (length <= 0) {
            break;
          }
          long skipped = input.skipBytes(length);
          if (skipped < length) {
            break;
          }
          count++;
        }
      } catch (IOException e) {
        // ignore count failures; return best-effort
      }
      return count;
    }

    private List<QueryLogRecord> readSegment(Path segment)
        throws IOException {
      if (!Files.exists(segment)) {
        return Collections.emptyList();
      }
      List<QueryLogRecord> records = new ArrayList<>();
      try (DataInputStream input = new DataInputStream(
          new BufferedInputStream(Files.newInputStream(segment, StandardOpenOption.READ)))) {
        while (true) {
          int length;
          try {
            length = input.readInt();
          } catch (EOFException e) {
            break;
          }
          if (length <= 0) {
            break;
          }
          byte[] payload = new byte[length];
          try {
            input.readFully(payload);
          } catch (EOFException e) {
            LOGGER.warn("Truncated query log segment {}, ignoring tail", segment);
            break;
          }
          try {
            records.add(QueryLogRecordSerDe.deserialize(payload));
          } catch (IOException e) {
            LOGGER.warn("Failed to deserialize query log record from {}", segment, e);
          }
        }
      }
      return records;
    }
  }

  private static List<SelectedColumn> resolveSelectList(@Nullable SqlNodeList selectList)
      throws BadQueryRequestException {
    if (selectList == null || selectList.size() == 0) {
      return allColumns();
    }
    if (selectList.size() == 1 && isStar(selectList.get(0))) {
      return allColumns();
    }

    List<SelectedColumn> selectedColumns = new ArrayList<>(selectList.size());
    Set<String> usedAliases = new HashSet<>();
    for (SqlNode node : selectList) {
      SqlNode expression = node;
      String alias = null;
      if (node instanceof SqlBasicCall && node.getKind() == SqlKind.AS) {
        SqlBasicCall asCall = (SqlBasicCall) node;
        expression = asCall.operand(0);
        SqlNode aliasNode = asCall.operand(1);
        if (aliasNode instanceof SqlIdentifier) {
          alias = ((SqlIdentifier) aliasNode).getSimple();
        } else {
          throw new BadQueryRequestException("Alias must be an identifier: " + aliasNode);
        }
      }

      if (!(expression instanceof SqlIdentifier)) {
        throw new BadQueryRequestException("Only column references are supported in SELECT list: " + expression);
      }

      QueryLogColumn column = QueryLogColumn.fromIdentifier((SqlIdentifier) expression);
      String outputName = alias != null ? alias : column._columnName;
      String lower = outputName.toLowerCase(Locale.ROOT);
      if (usedAliases.contains(lower)) {
        throw new BadQueryRequestException("Duplicate column/alias in SELECT list: " + outputName);
      }
      usedAliases.add(lower);
      selectedColumns.add(new SelectedColumn(column, outputName));
    }
    return selectedColumns;
  }

  private static List<SelectedColumn> allColumns() {
    List<SelectedColumn> columns = new ArrayList<>(QueryLogColumn.values().length);
    for (QueryLogColumn column : QueryLogColumn.values()) {
      columns.add(new SelectedColumn(column, column._columnName));
    }
    return columns;
  }

  private static List<QueryLogColumn> resolveGroupByColumns(@Nullable SqlNodeList groupBy)
      throws BadQueryRequestException {
    if (groupBy == null || groupBy.isEmpty()) {
      return Collections.emptyList();
    }
    List<QueryLogColumn> columns = new ArrayList<>(groupBy.size());
    Set<String> seen = new HashSet<>();
    for (SqlNode node : groupBy) {
      if (!(node instanceof SqlIdentifier)) {
        throw new BadQueryRequestException("GROUP BY only supports column references, found: " + node);
      }
      QueryLogColumn column = QueryLogColumn.fromIdentifier((SqlIdentifier) node);
      String key = column._columnName.toLowerCase(Locale.ROOT);
      if (!seen.add(key)) {
        throw new BadQueryRequestException("Duplicate column in GROUP BY: " + column._columnName);
      }
      columns.add(column);
    }
    return columns;
  }

  @Nullable
  private static AggregationQuery resolveAggregationQuery(@Nullable SqlNodeList selectList,
      @Nullable SqlNodeList groupBy)
      throws BadQueryRequestException {
    if (selectList == null || selectList.isEmpty()) {
      return null;
    }
    List<QueryLogColumn> groupByColumns = resolveGroupByColumns(groupBy);
    Map<QueryLogColumn, Integer> groupIndex = new HashMap<>();
    for (int i = 0; i < groupByColumns.size(); i++) {
      groupIndex.put(groupByColumns.get(i), i);
    }
    List<ResultProjection> projections = new ArrayList<>(selectList.size());
    boolean seenAggregation = false;
    for (SqlNode node : selectList) {
      SqlNode expression = node;
      String alias = null;
      if (node instanceof SqlBasicCall && node.getKind() == SqlKind.AS) {
        SqlBasicCall asCall = (SqlBasicCall) node;
        expression = asCall.operand(0);
        SqlNode aliasNode = asCall.operand(1);
        if (aliasNode instanceof SqlIdentifier) {
          alias = ((SqlIdentifier) aliasNode).getSimple();
        } else {
          throw new BadQueryRequestException("Alias must be an identifier: " + aliasNode);
        }
      }
      AggregationSelection aggregationSelection = parseAggregationSelection(expression, alias);
      if (aggregationSelection != null) {
        seenAggregation = true;
        projections.add(aggregationSelection);
        continue;
      }
      if (groupByColumns.isEmpty()) {
        if (seenAggregation) {
          throw new BadQueryRequestException("Cannot mix aggregation functions with raw columns without GROUP BY");
        }
        return null;
      }
      if (!(expression instanceof SqlIdentifier)) {
        throw new BadQueryRequestException(
            "GROUP BY queries must select grouping columns or aggregations, found: " + expression);
      }
      QueryLogColumn column = QueryLogColumn.fromIdentifier((SqlIdentifier) expression);
      Integer index = groupIndex.get(column);
      if (index == null) {
        throw new BadQueryRequestException("Column " + column._columnName + " must appear in the GROUP BY clause");
      }
      String outputName = alias != null ? alias : column._columnName;
      projections.add(new GroupBySelection(column, outputName, index));
    }
    if (!seenAggregation && groupByColumns.isEmpty()) {
      return null;
    }
    if (!groupByColumns.isEmpty() && !seenAggregation) {
      throw new BadQueryRequestException("GROUP BY queries must include at least one aggregation");
    }
    return new AggregationQuery(projections, groupByColumns);
  }

  @Nullable
  private static AggregationSelection parseAggregationSelection(SqlNode expression, @Nullable String alias)
      throws BadQueryRequestException {
    if (!(expression instanceof SqlBasicCall)) {
      return null;
    }
    SqlBasicCall call = (SqlBasicCall) expression;
    AggregationFunction function = AggregationFunction.fromSqlCall(call);
    if (function == null) {
      return null;
    }
    if (call.getFunctionQuantifier() != null) {
      throw new BadQueryRequestException("DISTINCT aggregations are not supported for system.query_log");
    }
    QueryLogColumn column = null;
    Double percentile = null;
    switch (function) {
      case COUNT:
        if (call.getOperandList().isEmpty()) {
          break;
        }
        SqlNode operand = call.operand(0);
        if (operand instanceof SqlIdentifier && ((SqlIdentifier) operand).isStar()) {
          break;
        }
        if (!(operand instanceof SqlIdentifier)) {
          throw new BadQueryRequestException("COUNT only supports column references or *");
        }
        column = QueryLogColumn.fromIdentifier((SqlIdentifier) operand);
        break;
      case SUM:
      case AVG:
      case MIN:
      case MAX:
        if (call.operandCount() != 1 || !(call.operand(0) instanceof SqlIdentifier)) {
          throw new BadQueryRequestException(function + " requires a single column reference");
        }
        column = QueryLogColumn.fromIdentifier((SqlIdentifier) call.operand(0));
        function.validateColumn(column);
        break;
      case PERCENTILEEST:
        if (call.operandCount() != 2 || !(call.operand(0) instanceof SqlIdentifier)) {
          throw new BadQueryRequestException("PERCENTILEEST requires a column reference and percentile literal");
        }
        column = QueryLogColumn.fromIdentifier((SqlIdentifier) call.operand(0));
        function.validateColumn(column);
        SqlNode percentileNode = call.operand(1);
        if (!(percentileNode instanceof SqlNumericLiteral)) {
          throw new BadQueryRequestException("PERCENTILEEST percentile must be a numeric literal between 0 and 100");
        }
        double percentileValue = ((SqlNumericLiteral) percentileNode).bigDecimalValue().doubleValue();
        if (percentileValue < 0d || percentileValue > 100d) {
          throw new BadQueryRequestException("PERCENTILEEST percentile must be between 0 and 100");
        }
        percentile = percentileValue;
        break;
      default:
        break;
    }
    String outputName = alias != null ? alias : expression.toString();
    return new AggregationSelection(function, column, outputName, expression.toString().toLowerCase(Locale.ROOT),
        percentile);
  }

  private BrokerResponse executeAggregation(AggregationQuery aggregationQuery, Predicate<QueryLogRecord> predicate,
      @Nullable SqlNodeList orderBy, @Nullable SqlNode offsetNode, @Nullable SqlNode fetchNode)
      throws BadQueryRequestException {
    long startTimeMs = System.currentTimeMillis();
    List<QueryLogRecord> rows;
    try {
      rows = getFilteredRecords(predicate);
    } catch (IOException e) {
      throw new BadQueryRequestException("Failed to read query log storage", e);
    }

    Map<GroupKey, List<QueryLogRecord>> groupedRows = groupRecords(rows, aggregationQuery._groupByColumns);
    List<Object[]> tableRows = new ArrayList<>(groupedRows.size());
    for (Map.Entry<GroupKey, List<QueryLogRecord>> entry : groupedRows.entrySet()) {
      Object[] values = new Object[aggregationQuery._projections.size()];
      for (int i = 0; i < aggregationQuery._projections.size(); i++) {
        values[i] = aggregationQuery._projections.get(i).compute(entry.getValue(), entry.getKey()._values);
      }
      tableRows.add(values);
    }

    List<AggregateOrdering> aggregateOrderings = parseAggregateOrderings(orderBy, aggregationQuery);
    if (!aggregateOrderings.isEmpty()) {
      tableRows.sort(buildAggregateComparator(aggregateOrderings));
    }

    int offset = parseNonNegativeInt(offsetNode, "OFFSET", 0);
    int limit = parseNonNegativeInt(fetchNode, "LIMIT", _defaultLimit);
    int fromIndex = Math.min(offset, tableRows.size());
    int toIndex = limit < 0 ? tableRows.size() : Math.min(tableRows.size(), fromIndex + limit);
    List<Object[]> window = new ArrayList<>(tableRows.subList(fromIndex, toIndex));

    String[] columnNames = new String[aggregationQuery._projections.size()];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[aggregationQuery._projections.size()];
    for (int i = 0; i < aggregationQuery._projections.size(); i++) {
      ResultProjection projection = aggregationQuery._projections.get(i);
      columnNames[i] = projection.getOutputName();
      columnDataTypes[i] = projection.getResultType();
    }
    ResultTable resultTable = new ResultTable(new DataSchema(columnNames, columnDataTypes), window);
    BrokerResponseNative response = new BrokerResponseNative();
    response.setResultTable(resultTable);
    response.setNumRowsResultSet(resultTable.getRows().size());
    response.setTablesQueried(Set.of(QueryLogSystemTableUtils.FULL_TABLE_NAME));
    response.setTimeUsedMs(System.currentTimeMillis() - startTimeMs);
    return response;
  }

  private static boolean isStar(@Nullable SqlNode node) {
    if (node instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) node;
      return identifier.isStar() || (identifier.names.size() == 1 && "*".equals(identifier.getSimple()));
    }
    return false;
  }

  private static final class GroupKey {
    private static final GroupKey EMPTY = new GroupKey(new Object[0]);

    private final Object[] _values;
    private final int _hashCode;

    private GroupKey(Object[] values) {
      _values = values;
      _hashCode = Arrays.deepHashCode(values);
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof GroupKey)) {
        return false;
      }
      GroupKey other = (GroupKey) obj;
      return Arrays.deepEquals(_values, other._values);
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }

  private static int parseNonNegativeInt(@Nullable SqlNode node, String clause, int defaultValue)
      throws BadQueryRequestException {
    if (node == null) {
      return defaultValue;
    }
    if (!(node instanceof SqlNumericLiteral)) {
      throw new BadQueryRequestException(clause + " must be a numeric literal");
    }
    try {
      int value = ((SqlNumericLiteral) node).intValue(true);
      if (value < 0) {
        throw new BadQueryRequestException(clause + " cannot be negative");
      }
      return value;
    } catch (ArithmeticException e) {
      throw new BadQueryRequestException(clause + " is out of range", e);
    }
  }

  private static List<Ordering> parseOrderings(@Nullable SqlNodeList orderList,
      Map<String, QueryLogColumn> aliasLookup, List<SelectedColumn> selections)
      throws BadQueryRequestException {
    if (orderList == null || orderList.isEmpty()) {
      return Collections.emptyList();
    }
    List<Ordering> orderings = new ArrayList<>(orderList.size());
    for (SqlNode orderNode : orderList) {
      boolean ascending = true;
      SqlNode expression = orderNode;
      if (orderNode instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) orderNode;
        if (call.getKind() == SqlKind.DESCENDING) {
          ascending = false;
          expression = call.operand(0);
        }
      }
      QueryLogColumn column = resolveColumnForOrdering(expression, aliasLookup, selections);
      if (!column.isComparable()) {
        throw new BadQueryRequestException("Cannot ORDER BY column " + column._columnName + ": not comparable");
      }
      orderings.add(new Ordering(column, ascending));
    }
    return orderings;
  }

  private static QueryLogColumn resolveColumnForOrdering(SqlNode expression, Map<String, QueryLogColumn> aliasLookup,
      List<SelectedColumn> selections)
      throws BadQueryRequestException {
    if (expression instanceof SqlNumericLiteral) {
      int ordinal = parseNonNegativeInt(expression, "ORDER BY", -1);
      if (ordinal <= 0 || ordinal > selections.size()) {
        throw new BadQueryRequestException("ORDER BY position " + ordinal + " is out of range");
      }
      return selections.get(ordinal - 1)._column;
    }
    if (!(expression instanceof SqlIdentifier)) {
      throw new BadQueryRequestException("ORDER BY only supports column references, found: " + expression);
    }
    String name = ((SqlIdentifier) expression).getSimple().toLowerCase(Locale.ROOT);
    QueryLogColumn column = aliasLookup.get(name);
    if (column != null) {
      return column;
    }
    return QueryLogColumn.fromIdentifier((SqlIdentifier) expression);
  }

  private static Comparator<QueryLogRecord> buildComparator(List<Ordering> orderings) {
    Comparator<QueryLogRecord> comparator =
        Comparator.comparing(record -> (Comparable) orderings.get(0)._column.extractComparable(record),
            Comparator.nullsLast(Comparator.naturalOrder()));
    if (!orderings.get(0)._ascending) {
      comparator = comparator.reversed();
    }
    for (int i = 1; i < orderings.size(); i++) {
      Ordering ordering = orderings.get(i);
      Comparator<QueryLogRecord> next = Comparator.comparing(
          record -> (Comparable) ordering._column.extractComparable(record),
          Comparator.nullsLast(Comparator.naturalOrder()));
      if (!ordering._ascending) {
        next = next.reversed();
      }
      comparator = comparator.thenComparing(next);
    }
    return comparator;
  }

  private Map<GroupKey, List<QueryLogRecord>> groupRecords(List<QueryLogRecord> rows,
      List<QueryLogColumn> groupByColumns) {
    Map<GroupKey, List<QueryLogRecord>> grouped = new LinkedHashMap<>();
    if (groupByColumns.isEmpty()) {
      grouped.put(GroupKey.EMPTY, rows);
      return grouped;
    }
    for (QueryLogRecord record : rows) {
      Object[] values = new Object[groupByColumns.size()];
      for (int i = 0; i < groupByColumns.size(); i++) {
        values[i] = groupByColumns.get(i).extract(record);
      }
      GroupKey key = new GroupKey(values);
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
    }
    return grouped;
  }

  private static List<AggregateOrdering> parseAggregateOrderings(@Nullable SqlNodeList orderList,
      AggregationQuery aggregationQuery)
      throws BadQueryRequestException {
    if (orderList == null || orderList.isEmpty()) {
      return Collections.emptyList();
    }
    List<AggregateOrdering> orderings = new ArrayList<>(orderList.size());
    for (SqlNode orderNode : orderList) {
      boolean ascending = true;
      SqlNode expression = orderNode;
      if (orderNode instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) orderNode;
        if (call.getKind() == SqlKind.DESCENDING) {
          ascending = false;
          expression = call.operand(0);
        }
      }
      int columnIndex = resolveAggregateOrderIndex(expression, aggregationQuery);
      orderings.add(new AggregateOrdering(columnIndex, ascending));
    }
    return orderings;
  }

  private static int resolveAggregateOrderIndex(SqlNode expression, AggregationQuery aggregationQuery)
      throws BadQueryRequestException {
    if (expression instanceof SqlNumericLiteral) {
      int ordinal = parseNonNegativeInt(expression, "ORDER BY", -1);
      if (ordinal <= 0 || ordinal > aggregationQuery._projections.size()) {
        throw new BadQueryRequestException("ORDER BY position " + ordinal + " is out of range");
      }
      return ordinal - 1;
    }
    Integer index = aggregationQuery.getColumnIndex(expression.toString().toLowerCase(Locale.ROOT));
    if (index != null) {
      return index;
    }
    if (expression instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) expression;
      index = aggregationQuery.getColumnIndex(identifier.getSimple().toLowerCase(Locale.ROOT));
      if (index != null) {
        return index;
      }
    }
    throw new BadQueryRequestException("ORDER BY expression must reference the SELECT list: " + expression);
  }

  private static Comparator<Object[]> buildAggregateComparator(List<AggregateOrdering> orderings) {
    Comparator<Object[]> comparator = Comparator.comparing(
        row -> (Comparable) row[orderings.get(0)._columnIndex], Comparator.nullsLast(Comparator.naturalOrder()));
    if (!orderings.get(0)._ascending) {
      comparator = comparator.reversed();
    }
    for (int i = 1; i < orderings.size(); i++) {
      AggregateOrdering ordering = orderings.get(i);
      Comparator<Object[]> next =
          Comparator.comparing(row -> (Comparable) row[ordering._columnIndex], Comparator.nullsLast(
              Comparator.naturalOrder()));
      if (!ordering._ascending) {
        next = next.reversed();
      }
      comparator = comparator.thenComparing(next);
    }
    return comparator;
  }

  private static Predicate<QueryLogRecord> buildPredicate(@Nullable SqlNode node,
      Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    if (node == null) {
      return record -> true;
    }
    if (!(node instanceof SqlBasicCall)) {
      throw new BadQueryRequestException("Unsupported WHERE clause: " + node);
    }
    SqlBasicCall call = (SqlBasicCall) node;
    SqlKind kind = call.getKind();
    switch (kind) {
      case AND:
        return buildPredicate(call.operand(0), aliasLookup).and(buildPredicate(call.operand(1), aliasLookup));
      case OR:
        return buildPredicate(call.operand(0), aliasLookup).or(buildPredicate(call.operand(1), aliasLookup));
      case NOT:
        return buildPredicate(call.operand(0), aliasLookup).negate();
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return comparisonPredicate(kind, call.operand(0), call.operand(1), aliasLookup);
      case LIKE:
        return likePredicate(call, aliasLookup);
      case IN:
        return inPredicate(call, aliasLookup, false);
      case NOT_IN:
        return inPredicate(call, aliasLookup, true);
      case IS_NULL:
      case IS_NOT_NULL:
        return nullCheckPredicate(kind, call.operand(0), aliasLookup);
      case BETWEEN:
        return betweenPredicate(call, aliasLookup);
      default:
        throw new BadQueryRequestException("Unsupported WHERE expression: " + node);
    }
  }

  private static Predicate<QueryLogRecord> comparisonPredicate(SqlKind kind, SqlNode left, SqlNode right,
      Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    ColumnWithLiteral columnWithLiteral = resolveColumnAndLiteral(left, right, aliasLookup);
    QueryLogColumn column = columnWithLiteral._column;
    if (!column.isComparable()) {
      throw new BadQueryRequestException("Column " + column._columnName + " is not comparable");
    }
    Comparable<?> literal = column.parseComparableLiteral(columnWithLiteral._literalNode);
    if (literal == null) {
      throw new BadQueryRequestException("Literal value cannot be NULL in comparison. Use IS NULL instead.");
    }

    SqlKind effectiveKind = columnWithLiteral._swapped ? invert(kind) : kind;
    return record -> {
      Comparable value = column.extractComparable(record);
      if (value == null) {
        return false;
      }
      int cmp = value.compareTo(literal);
      switch (effectiveKind) {
        case EQUALS:
          return cmp == 0;
        case NOT_EQUALS:
          return cmp != 0;
        case GREATER_THAN:
          return cmp > 0;
        case GREATER_THAN_OR_EQUAL:
          return cmp >= 0;
        case LESS_THAN:
          return cmp < 0;
        case LESS_THAN_OR_EQUAL:
          return cmp <= 0;
        default:
          return false;
      }
    };
  }

  private static SqlKind invert(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      default:
        return kind;
    }
  }

  private static Predicate<QueryLogRecord> likePredicate(SqlBasicCall call,
      Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    ColumnWithLiteral column = resolveColumnAndLiteral(call.operand(0), call.operand(1), aliasLookup);
    if (column._swapped) {
      throw new BadQueryRequestException("LIKE expressions must have a column on the left side");
    }
    if (column._column._valueType != ValueType.STRING) {
      throw new BadQueryRequestException("LIKE only supports STRING columns");
    }
    Object literalValue = column._column.parseLiteral(column._literalNode);
    if (!(literalValue instanceof String)) {
      throw new BadQueryRequestException("LIKE pattern must be a string literal");
    }
    char escapeChar = '\\';
    if (call.operandCount() == 3) {
      Object escapeValue = column._column.parseLiteral(call.operand(2));
      if (!(escapeValue instanceof String) || ((String) escapeValue).length() != 1) {
        throw new BadQueryRequestException("LIKE escape clause must be a single character literal");
      }
      escapeChar = ((String) escapeValue).charAt(0);
    }
    Pattern pattern = buildLikePattern((String) literalValue, escapeChar);
    return record -> {
      Object val = column._column.extract(record);
      return val instanceof String && pattern.matcher((String) val).matches();
    };
  }

  private static Pattern buildLikePattern(String pattern, char escapeChar)
      throws BadQueryRequestException {
    StringBuilder regex = new StringBuilder("^");
    boolean escaping = false;
    for (int i = 0; i < pattern.length(); i++) {
      char ch = pattern.charAt(i);
      if (escaping) {
        regex.append(Pattern.quote(String.valueOf(ch)));
        escaping = false;
        continue;
      }
      if (ch == escapeChar) {
        escaping = true;
        continue;
      }
      switch (ch) {
        case '%':
          regex.append(".*");
          break;
        case '_':
          regex.append('.');
          break;
        default:
          regex.append(Pattern.quote(String.valueOf(ch)));
          break;
      }
    }
    if (escaping) {
      throw new BadQueryRequestException("LIKE pattern cannot end with escape character");
    }
    regex.append('$');
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }

  private static Predicate<QueryLogRecord> inPredicate(SqlBasicCall call, Map<String, QueryLogColumn> aliasLookup,
      boolean negate)
      throws BadQueryRequestException {
    SqlNode left = call.operand(0);
    SqlNode right = call.operand(1);
    if (!(right instanceof SqlNodeList)) {
      throw new BadQueryRequestException("IN clause must be followed by a list of literals");
    }
    ColumnWithLiteral column = resolveColumnAndLiteral(left, null, aliasLookup);
    if (column._swapped) {
      throw new BadQueryRequestException("IN clause must have column reference on the left");
    }
    SqlNodeList list = (SqlNodeList) right;
    List<Object> literalValues = new ArrayList<>(list.size());
    for (SqlNode sqlNode : list) {
      literalValues.add(column._column.parseLiteral(sqlNode));
    }
    return record -> {
      Object value = column._column.extract(record);
      boolean contains = literalValues.stream().anyMatch(val -> Objects.equals(val, value));
      return negate ? !contains : contains;
    };
  }

  private static Predicate<QueryLogRecord> nullCheckPredicate(SqlKind kind, SqlNode operand,
      Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    QueryLogColumn column = resolveColumn(operand, aliasLookup);
    if (kind == SqlKind.IS_NULL) {
      return record -> column.extract(record) == null;
    } else {
      return record -> column.extract(record) != null;
    }
  }

  private static Predicate<QueryLogRecord> betweenPredicate(SqlBasicCall call,
      Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    SqlNode valueNode = call.operand(0);
    SqlNode lowerNode = call.operand(1);
    SqlNode upperNode = call.operand(2);
    QueryLogColumn column = resolveColumn(valueNode, aliasLookup);
    if (!column.isComparable()) {
      throw new BadQueryRequestException("BETWEEN only supported on comparable columns");
    }
    Comparable<?> lower = column.parseComparableLiteral(lowerNode);
    Comparable<?> upper = column.parseComparableLiteral(upperNode);
    return record -> {
      Comparable value = column.extractComparable(record);
      if (value == null) {
        return false;
      }
      return value.compareTo(lower) >= 0 && value.compareTo(upper) <= 0;
    };
  }

  private static ColumnWithLiteral resolveColumnAndLiteral(SqlNode left, @Nullable SqlNode right,
      Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    QueryLogColumn leftColumn = isColumnReference(left) ? resolveColumn(left, aliasLookup) : null;
    QueryLogColumn rightColumn = right != null && isColumnReference(right) ? resolveColumn(right, aliasLookup) : null;

    if (leftColumn != null && right != null) {
      return new ColumnWithLiteral(leftColumn, right, false);
    }
    if (rightColumn != null) {
      return new ColumnWithLiteral(rightColumn, left, true);
    }
    if (leftColumn != null && right == null) {
      return new ColumnWithLiteral(leftColumn, null, false);
    }
    throw new BadQueryRequestException("Expected column reference in expression: " + left + ", " + right);
  }

  private static boolean isColumnReference(SqlNode node) {
    return node instanceof SqlIdentifier;
  }

  private static QueryLogColumn resolveColumn(SqlNode node, Map<String, QueryLogColumn> aliasLookup)
      throws BadQueryRequestException {
    if (!(node instanceof SqlIdentifier)) {
      throw new BadQueryRequestException("Expected column reference but found: " + node);
    }
    SqlIdentifier identifier = (SqlIdentifier) node;
    QueryLogColumn column = aliasLookup.get(identifier.getSimple().toLowerCase(Locale.ROOT));
    if (column != null) {
      return column;
    }
    return QueryLogColumn.fromIdentifier(identifier);
  }

  @VisibleForTesting
  void reset() {
    _lock.writeLock().lock();
    try {
      if (_store != null) {
        try {
          _store.close();
        } catch (IOException e) {
          LOGGER.warn("Failed to close query log store", e);
        }
        _store = null;
      }
      _initialized.set(false);
      _enabled = false;
      _storageType = null;
    } finally {
      _lock.writeLock().unlock();
    }
  }

  private static final class ParsedQuery {
    private final SqlNodeList _selectList;
    private final SqlNode _whereClause;
    private final SqlNodeList _orderBy;
    private final SqlNode _offsetNode;
    private final SqlNode _fetchNode;
    private final SqlNodeList _groupBy;

    private ParsedQuery(SqlNodeList selectList, SqlNode whereClause, SqlNodeList orderBy, SqlNode offsetNode,
        SqlNode fetchNode, SqlNodeList groupBy) {
      _selectList = selectList;
      _whereClause = whereClause;
      _orderBy = orderBy;
      _offsetNode = offsetNode;
      _fetchNode = fetchNode;
      _groupBy = groupBy;
    }
  }

  private static final class SelectedColumn {
    private final QueryLogColumn _column;
    private final String _outputName;

    private SelectedColumn(QueryLogColumn column, String outputName) {
      _column = column;
      _outputName = outputName;
    }
  }

  private interface ResultProjection {
    String getOutputName();

    DataSchema.ColumnDataType getResultType();

    Object compute(List<QueryLogRecord> records, @Nullable Object[] groupValues);

    String getExpressionKey();
  }

  private static final class AggregationQuery {
    private final List<ResultProjection> _projections;
    private final List<QueryLogColumn> _groupByColumns;
    private final Map<String, Integer> _nameToIndex;

    private AggregationQuery(List<ResultProjection> projections, List<QueryLogColumn> groupByColumns) {
      _projections = projections;
      _groupByColumns = groupByColumns;
      _nameToIndex = new HashMap<>();
      for (int i = 0; i < projections.size(); i++) {
        ResultProjection projection = projections.get(i);
        _nameToIndex.put(projection.getOutputName().toLowerCase(Locale.ROOT), i);
        String expressionKey = projection.getExpressionKey();
        if (expressionKey != null) {
          _nameToIndex.put(expressionKey, i);
        }
      }
    }

    @Nullable
    Integer getColumnIndex(String key) {
      return _nameToIndex.get(key.toLowerCase(Locale.ROOT));
    }
  }

  private static final class AggregationSelection implements ResultProjection {
    private final AggregationFunction _function;
    private final QueryLogColumn _column;
    private final String _outputName;
    private final String _expressionKey;
    private final Double _percentile;

    private AggregationSelection(AggregationFunction function, @Nullable QueryLogColumn column, String outputName,
        String expressionKey, @Nullable Double percentile) {
      _function = function;
      _column = column;
      _outputName = outputName;
      _expressionKey = expressionKey;
      _percentile = percentile;
    }

    @Override
    public String getOutputName() {
      return _outputName;
    }

    @Override
    public DataSchema.ColumnDataType getResultType() {
      return _function.getResultType(_column);
    }

    @Override
    public Object compute(List<QueryLogRecord> records, @Nullable Object[] groupValues) {
      return _function.compute(records, _column, _percentile);
    }

    @Override
    public String getExpressionKey() {
      return _expressionKey;
    }
  }

  private static final class GroupBySelection implements ResultProjection {
    private final QueryLogColumn _column;
    private final String _outputName;
    private final int _groupIndex;

    private GroupBySelection(QueryLogColumn column, String outputName, int groupIndex) {
      _column = column;
      _outputName = outputName;
      _groupIndex = groupIndex;
    }

    @Override
    public String getOutputName() {
      return _outputName;
    }

    @Override
    public DataSchema.ColumnDataType getResultType() {
      return _column._dataType;
    }

    @Override
    public Object compute(List<QueryLogRecord> records, @Nullable Object[] groupValues) {
      if (groupValues == null || _groupIndex >= groupValues.length) {
        return null;
      }
      return groupValues[_groupIndex];
    }

    @Override
    public String getExpressionKey() {
      return _column._columnName.toLowerCase(Locale.ROOT);
    }
  }

  private static final class Ordering {
    private final QueryLogColumn _column;
    private final boolean _ascending;

    private Ordering(QueryLogColumn column, boolean ascending) {
      _column = column;
      _ascending = ascending;
    }
  }

  private static final class AggregateOrdering {
    private final int _columnIndex;
    private final boolean _ascending;

    private AggregateOrdering(int columnIndex, boolean ascending) {
      _columnIndex = columnIndex;
      _ascending = ascending;
    }
  }

  private enum AggregationFunction {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,
    PERCENTILEEST;

    static AggregationFunction fromKind(SqlKind kind) {
      switch (kind) {
        case COUNT:
          return COUNT;
        case SUM:
          return SUM;
        case AVG:
          return AVG;
        case MIN:
          return MIN;
        case MAX:
          return MAX;
        default:
          return null;
      }
    }

    static AggregationFunction fromSqlCall(SqlBasicCall call) {
      AggregationFunction function = fromKind(call.getKind());
      if (function != null) {
        return function;
      }
      String operatorName = call.getOperator().getName();
      if ("PERCENTILEEST".equalsIgnoreCase(operatorName) || "PERCENTILE".equalsIgnoreCase(operatorName)) {
        return PERCENTILEEST;
      }
      switch (operatorName.toUpperCase()) {
        case "COUNT":
          return COUNT;
        case "SUM":
          return SUM;
        case "AVG":
          return AVG;
        case "MIN":
          return MIN;
        case "MAX":
          return MAX;
        default:
          return null;
      }
    }

    void validateColumn(@Nullable QueryLogColumn column)
        throws BadQueryRequestException {
      switch (this) {
        case SUM:
        case AVG:
        case PERCENTILEEST:
          if (column == null || !column.isNumeric()) {
            throw new BadQueryRequestException(this + " requires a numeric column");
          }
          break;
        case MIN:
        case MAX:
          if (column == null || !column.isComparable()) {
            throw new BadQueryRequestException(this + " requires a comparable column");
          }
          break;
        default:
          break;
      }
    }

    DataSchema.ColumnDataType getResultType(@Nullable QueryLogColumn column) {
      switch (this) {
        case COUNT:
        case SUM:
          return DataSchema.ColumnDataType.LONG;
        case AVG:
        case PERCENTILEEST:
          return DataSchema.ColumnDataType.DOUBLE;
        case MIN:
        case MAX:
          return column != null ? column._dataType : DataSchema.ColumnDataType.UNKNOWN;
        default:
          return DataSchema.ColumnDataType.UNKNOWN;
      }
    }

    Object compute(List<QueryLogRecord> records, @Nullable QueryLogColumn column, @Nullable Double percentile) {
      switch (this) {
        case COUNT:
          if (column == null) {
            return (long) records.size();
          }
          long count = 0;
          for (QueryLogRecord record : records) {
            if (column.extract(record) != null) {
              count++;
            }
          }
          return count;
        case SUM: {
          long sum = 0;
          boolean hasValue = false;
          for (QueryLogRecord record : records) {
            Number number = column.extractNumber(record);
            if (number != null) {
              sum += number.longValue();
              hasValue = true;
            }
          }
          return hasValue ? sum : null;
        }
        case AVG: {
          double sum = 0D;
          long valueCount = 0;
          for (QueryLogRecord record : records) {
            Number number = column.extractNumber(record);
            if (number != null) {
              sum += number.doubleValue();
              valueCount++;
            }
          }
          return valueCount == 0 ? null : sum / valueCount;
        }
        case MIN: {
          Comparable min = null;
          for (QueryLogRecord record : records) {
            Comparable value = column.extractComparable(record);
            if (value != null && (min == null || value.compareTo(min) < 0)) {
              min = value;
            }
          }
          return min;
        }
        case MAX: {
          Comparable max = null;
          for (QueryLogRecord record : records) {
            Comparable value = column.extractComparable(record);
            if (value != null && (max == null || value.compareTo(max) > 0)) {
              max = value;
            }
          }
          return max;
        }
        case PERCENTILEEST: {
          if (column == null || percentile == null) {
            return null;
          }
          List<Double> values = new ArrayList<>();
          for (QueryLogRecord record : records) {
            Number number = column.extractNumber(record);
            if (number != null) {
              values.add(number.doubleValue());
            }
          }
          if (values.isEmpty()) {
            return null;
          }
          Collections.sort(values);
          double normalized = percentile / 100d;
          double rank = normalized * (values.size() - 1);
          int lower = (int) Math.floor(rank);
          int upper = (int) Math.ceil(rank);
          if (lower == upper) {
            return values.get(lower);
          }
          double fraction = rank - lower;
          return values.get(lower) + (values.get(upper) - values.get(lower)) * fraction;
        }
        default:
          return null;
      }
    }
  }

  private static final class ColumnWithLiteral {
    private final QueryLogColumn _column;
    private final SqlNode _literalNode;
    private final boolean _swapped;

    private ColumnWithLiteral(QueryLogColumn column, SqlNode literalNode, boolean swapped) {
      _column = column;
      _literalNode = literalNode;
      _swapped = swapped;
    }
  }

  private enum ValueType {
    LONG,
    INT,
    STRING,
    BOOLEAN,
    TIMESTAMP,
    STRING_ARRAY,
    INT_ARRAY
  }

  private enum QueryLogColumn {
    TIMESTAMP_MS("timestampMs", DataSchema.ColumnDataType.TIMESTAMP, ValueType.TIMESTAMP) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getLogTimestampMs();
      }
    },
    REQUEST_ID("requestId", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRequestId();
      }
    },
    TABLE_NAME("tableName", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getTableName();
      }
    },
    BROKER_ID("brokerId", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getBrokerId();
      }
    },
    CLIENT_IP("clientIp", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getClientIp();
      }
    },
    QUERY("query", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getQuery();
      }
    },
    QUERY_ENGINE("queryEngine", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getQueryEngine();
      }
    },
    REQUEST_ARRIVAL_TIME_MS("requestArrivalTimeMs", DataSchema.ColumnDataType.TIMESTAMP, ValueType.TIMESTAMP) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRequestArrivalTimeMs();
      }
    },
    TIME_MS("timeMs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getTimeMs();
      }
    },
    BROKER_REDUCE_TIME_MS("brokerReduceTimeMs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getBrokerReduceTimeMs();
      }
    },
    NUM_DOCS_SCANNED("numDocsScanned", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumDocsScanned();
      }
    },
    TOTAL_DOCS("totalDocs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getTotalDocs();
      }
    },
    NUM_ENTRIES_SCANNED_IN_FILTER("numEntriesScannedInFilter", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumEntriesScannedInFilter();
      }
    },
    NUM_ENTRIES_SCANNED_POST_FILTER("numEntriesScannedPostFilter", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumEntriesScannedPostFilter();
      }
    },
    NUM_SEGMENTS_QUERIED("numSegmentsQueried", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumSegmentsQueried();
      }
    },
    NUM_SEGMENTS_PROCESSED("numSegmentsProcessed", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumSegmentsProcessed();
      }
    },
    NUM_SEGMENTS_MATCHED("numSegmentsMatched", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumSegmentsMatched();
      }
    },
    NUM_CONSUMING_SEGMENTS_QUERIED("numConsumingSegmentsQueried", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumConsumingSegmentsQueried();
      }
    },
    NUM_CONSUMING_SEGMENTS_PROCESSED("numConsumingSegmentsProcessed", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumConsumingSegmentsProcessed();
      }
    },
    NUM_CONSUMING_SEGMENTS_MATCHED("numConsumingSegmentsMatched", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumConsumingSegmentsMatched();
      }
    },
    NUM_UNAVAILABLE_SEGMENTS("numUnavailableSegments", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumUnavailableSegments();
      }
    },
    MIN_CONSUMING_FRESHNESS_TIME_MS("minConsumingFreshnessTimeMs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getMinConsumingFreshnessTimeMs();
      }
    },
    NUM_SERVERS_RESPONDED("numServersResponded", DataSchema.ColumnDataType.INT, ValueType.INT) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumServersResponded();
      }
    },
    NUM_SERVERS_QUERIED("numServersQueried", DataSchema.ColumnDataType.INT, ValueType.INT) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumServersQueried();
      }
    },
    GROUPS_TRIMMED("groupsTrimmed", DataSchema.ColumnDataType.BOOLEAN, ValueType.BOOLEAN) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.isGroupsTrimmed();
      }
    },
    GROUP_LIMIT_REACHED("groupLimitReached", DataSchema.ColumnDataType.BOOLEAN, ValueType.BOOLEAN) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.isGroupLimitReached();
      }
    },
    GROUP_WARNING_LIMIT_REACHED("groupWarningLimitReached", DataSchema.ColumnDataType.BOOLEAN, ValueType.BOOLEAN) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.isGroupWarningLimitReached();
      }
    },
    NUM_EXCEPTIONS("numExceptions", DataSchema.ColumnDataType.INT, ValueType.INT) {
      @Override
      Object extract(QueryLogRecord record) {
        return (int) record.getNumExceptions();
      }
    },
    EXCEPTIONS("exceptions", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getExceptions();
      }
    },
    SERVER_STATS("serverStats", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getServerStats();
      }
    },
    OFFLINE_TOTAL_CPU_TIME_NS("offlineTotalCpuTimeNs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineTotalCpuTimeNs();
      }
    },
    OFFLINE_THREAD_CPU_TIME_NS("offlineThreadCpuTimeNs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineThreadCpuTimeNs();
      }
    },
    OFFLINE_SYSTEM_ACTIVITIES_CPU_TIME_NS("offlineSystemActivitiesCpuTimeNs", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineSystemActivitiesCpuTimeNs();
      }
    },
    OFFLINE_RESPONSE_SERIALIZATION_CPU_TIME_NS("offlineResponseSerializationCpuTimeNs",
        DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineResponseSerializationCpuTimeNs();
      }
    },
    REALTIME_TOTAL_CPU_TIME_NS("realtimeTotalCpuTimeNs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeTotalCpuTimeNs();
      }
    },
    REALTIME_THREAD_CPU_TIME_NS("realtimeThreadCpuTimeNs", DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeThreadCpuTimeNs();
      }
    },
    REALTIME_SYSTEM_ACTIVITIES_CPU_TIME_NS("realtimeSystemActivitiesCpuTimeNs", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeSystemActivitiesCpuTimeNs();
      }
    },
    REALTIME_RESPONSE_SERIALIZATION_CPU_TIME_NS("realtimeResponseSerializationCpuTimeNs",
        DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeResponseSerializationCpuTimeNs();
      }
    },
    OFFLINE_TOTAL_MEM_ALLOCATED_BYTES("offlineTotalMemAllocatedBytes", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineTotalMemAllocatedBytes();
      }
    },
    OFFLINE_THREAD_MEM_ALLOCATED_BYTES("offlineThreadMemAllocatedBytes", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineThreadMemAllocatedBytes();
      }
    },
    OFFLINE_RESPONSE_SER_MEM_ALLOCATED_BYTES("offlineResponseSerMemAllocatedBytes",
        DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineResponseSerMemAllocatedBytes();
      }
    },
    REALTIME_TOTAL_MEM_ALLOCATED_BYTES("realtimeTotalMemAllocatedBytes", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeTotalMemAllocatedBytes();
      }
    },
    REALTIME_THREAD_MEM_ALLOCATED_BYTES("realtimeThreadMemAllocatedBytes", DataSchema.ColumnDataType.LONG,
        ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeThreadMemAllocatedBytes();
      }
    },
    REALTIME_RESPONSE_SER_MEM_ALLOCATED_BYTES("realtimeResponseSerMemAllocatedBytes",
        DataSchema.ColumnDataType.LONG, ValueType.LONG) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeResponseSerMemAllocatedBytes();
      }
    },
    POOLS("pools", DataSchema.ColumnDataType.INT_ARRAY, ValueType.INT_ARRAY) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getPools();
      }
    },
    PARTIAL_RESULT("partialResult", DataSchema.ColumnDataType.BOOLEAN, ValueType.BOOLEAN) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.isPartialResult();
      }
    },
    RLS_FILTERS_APPLIED("rlsFiltersApplied", DataSchema.ColumnDataType.BOOLEAN, ValueType.BOOLEAN) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.isRlsFiltersApplied();
      }
    },
    NUM_ROWS_RESULT_SET("numRowsResultSet", DataSchema.ColumnDataType.INT, ValueType.INT) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getNumRowsResultSet();
      }
    },
    TABLES_QUERIED("tablesQueried", DataSchema.ColumnDataType.STRING_ARRAY, ValueType.STRING_ARRAY) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getTablesQueried();
      }
    },
    TRACE_INFO("traceInfo", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getTraceInfoJson();
      }
    },
    FANOUT_TYPE("fanoutType", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getFanoutType();
      }
    },
    OFFLINE_SERVER_TENANT("offlineServerTenant", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getOfflineServerTenant();
      }
    },
    REALTIME_SERVER_TENANT("realtimeServerTenant", DataSchema.ColumnDataType.STRING, ValueType.STRING) {
      @Override
      Object extract(QueryLogRecord record) {
        return record.getRealtimeServerTenant();
      }
    };

    private static final Map<String, QueryLogColumn> NAME_TO_COLUMN;

    static {
      Map<String, QueryLogColumn> lookup = new HashMap<>();
      for (QueryLogColumn column : values()) {
        lookup.put(column._columnName.toLowerCase(Locale.ROOT), column);
      }
      NAME_TO_COLUMN = Collections.unmodifiableMap(lookup);
    }

    private final String _columnName;
    private final DataSchema.ColumnDataType _dataType;
    private final ValueType _valueType;

    QueryLogColumn(String columnName, DataSchema.ColumnDataType dataType, ValueType valueType) {
      _columnName = columnName;
      _dataType = dataType;
      _valueType = valueType;
    }

    abstract Object extract(QueryLogRecord record);

    Comparable extractComparable(QueryLogRecord record) {
      Object value = extract(record);
      return value instanceof Comparable ? (Comparable) value : null;
    }

    Number extractNumber(QueryLogRecord record) {
      Object value = extract(record);
      return value instanceof Number ? (Number) value : null;
    }

    boolean isComparable() {
      return _valueType != ValueType.STRING_ARRAY && _valueType != ValueType.INT_ARRAY;
    }

    boolean isNumeric() {
      return _valueType == ValueType.LONG || _valueType == ValueType.INT || _valueType == ValueType.TIMESTAMP;
    }

    Object parseLiteral(@Nullable SqlNode literal)
        throws BadQueryRequestException {
      if (literal == null) {
        return null;
      }
      switch (_valueType) {
        case STRING:
          if (literal instanceof SqlLiteral) {
            return ((SqlLiteral) literal).getValueAs(String.class);
          }
          break;
        case LONG:
        case TIMESTAMP:
          if (literal instanceof SqlNumericLiteral) {
            return ((SqlNumericLiteral) literal).longValue(true);
          }
          break;
        case INT:
          if (literal instanceof SqlNumericLiteral) {
            return ((SqlNumericLiteral) literal).intValue(true);
          }
          break;
        case BOOLEAN:
          if (literal instanceof SqlLiteral) {
            return ((SqlLiteral) literal).booleanValue();
          }
          break;
        default:
          throw new BadQueryRequestException("Column " + _columnName + " does not support literal comparisons");
      }
      throw new BadQueryRequestException("Unsupported literal for column " + _columnName + ": " + literal);
    }

    Comparable parseComparableLiteral(@Nullable SqlNode literal)
        throws BadQueryRequestException {
      Object value = parseLiteral(literal);
      return value instanceof Comparable ? (Comparable) value : null;
    }

    static QueryLogColumn fromIdentifier(SqlIdentifier identifier)
        throws BadQueryRequestException {
      String name = identifier.names.get(identifier.names.size() - 1);
      return fromName(unquoteIdentifier(name));
    }

    static QueryLogColumn fromName(String name)
        throws BadQueryRequestException {
      QueryLogColumn column = NAME_TO_COLUMN.get(name.toLowerCase(Locale.ROOT));
      if (column == null) {
        throw new BadQueryRequestException("Unknown column '" + name + "' for system.query_log");
      }
      return column;
    }

    private static String unquoteIdentifier(String part) {
      if (part == null || part.length() < 2) {
        return part;
      }
      char first = part.charAt(0);
      char last = part.charAt(part.length() - 1);
      if ((first == '`' && last == '`') || (first == '"' && last == '"') || (first == '[' && last == ']')
          || (first == '\'' && last == '\'')) {
        return part.substring(1, part.length() - 1);
      }
      return part;
    }
  }
}
