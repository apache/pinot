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
package org.apache.pinot.broker.stats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.ColumnValueType;
import org.apache.pinot.query.planner.spi.stats.SegmentColumnStatsRow;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.StatsAggregations;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// SQLite-backed implementation of [StatsStore].
///
/// ### Threading model
/// Uses a single shared writer [Connection] guarded by `synchronized` on
/// `_writeLock`, plus a small pool (the configured pool size) of read-only connections
/// served via a blocking queue. Reads from multiple threads proceed concurrently (each
/// borrows a connection from the pool, uses it, then returns it). The writer connection sets
/// `PRAGMA journal_mode=WAL` so readers never block writers.
///
/// ### Corruption handling
/// [#init()] attempts to open the database and apply its schema. On any failure it logs a
/// warning, deletes the DB file and its WAL/SHM siblings, then retries once from scratch.
/// Only a second consecutive failure is propagated as [StatsStoreException].
public class SqliteStatsStore implements StatsStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteStatsStore.class);

  /// Default number of pooled readers when the operator sets none.
  static final int DEFAULT_READ_POOL_SIZE = 4;

  /// Default wait for a pooled reader. Deliberately short: this sits on the query-planning path,
  /// where a late estimate is worth less than a fast plan without one, and the caller degrades to
  /// heuristics rather than failing.
  static final long DEFAULT_READ_BORROW_TIMEOUT_MS = 50;

  private static final String DB_FILE_NAME = "broker-stats.sqlite";

  /// Bump when [#SCHEMA_DDL] changes. Note that the persisted vocabulary is wider than the DDL:
  /// `value_type` stores [ColumnValueType] names, which a newer build may extend without touching
  /// the DDL. That case does not need a bump, because an unrecognized name resolves to `null` and
  /// degrades to untrusted bounds rather than failing the read.
  ///
  /// There is no migration path on purpose: every row here is
  /// derived from the ZooKeeper segment metadata the broker re-reads at startup, so a store whose
  /// schema does not match is discarded and rebuilt rather than migrated. That keeps schema
  /// changes, corruption and an unreadable file on one recovery path.
  private static final int SCHEMA_VERSION = 2;

  private static final String[] SCHEMA_DDL = {
      "CREATE TABLE segment_stats ("
          + "table_name TEXT NOT NULL, segment_name TEXT NOT NULL, crc INTEGER NOT NULL, "
          + "total_docs INTEGER NOT NULL, size_bytes INTEGER NOT NULL, start_time_ms INTEGER NOT NULL, "
          + "end_time_ms INTEGER NOT NULL, consuming INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, "
          + "PRIMARY KEY (table_name, segment_name))",
      "CREATE TABLE segment_col_stats ("
          + "table_name TEXT NOT NULL, segment_name TEXT NOT NULL, column_name TEXT NOT NULL, "
          + "ndv INTEGER NOT NULL, min_value TEXT, max_value TEXT, min_trusted INTEGER NOT NULL, "
          + "avg_bytes REAL NOT NULL, null_fraction REAL NOT NULL, value_type TEXT, "
          + "updated_at_ms INTEGER NOT NULL, "
          + "PRIMARY KEY (table_name, segment_name, column_name))",
      // getColumnStats filters by (table_name, column_name); column_name is the third key component
      // so the primary key cannot serve it.
      "CREATE INDEX idx_segment_col_stats_col ON segment_col_stats(table_name, column_name)",
      "CREATE INDEX idx_segment_stats_time ON segment_stats(table_name, start_time_ms, end_time_ms)",
  };

  // SQL constants
  private static final String SQL_UPSERT_SEGMENT =
      "INSERT INTO segment_stats(table_name,segment_name,crc,total_docs,size_bytes,"
          + "start_time_ms,end_time_ms,consuming,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?) "
          + "ON CONFLICT(table_name,segment_name) DO UPDATE SET "
          + "crc=excluded.crc,total_docs=excluded.total_docs,size_bytes=excluded.size_bytes,"
          + "start_time_ms=excluded.start_time_ms,end_time_ms=excluded.end_time_ms,"
          + "consuming=excluded.consuming,updated_at_ms=excluded.updated_at_ms";

  private static final String SQL_UPSERT_COL =
      "INSERT INTO segment_col_stats(table_name,segment_name,column_name,ndv,min_value,"
          + "max_value,min_trusted,avg_bytes,null_fraction,value_type,updated_at_ms) "
          + "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
          + "ON CONFLICT(table_name,segment_name,column_name) DO UPDATE SET "
          + "ndv=excluded.ndv,min_value=excluded.min_value,max_value=excluded.max_value,"
          + "min_trusted=excluded.min_trusted,avg_bytes=excluded.avg_bytes,"
          + "null_fraction=excluded.null_fraction,value_type=excluded.value_type,"
          + "updated_at_ms=excluded.updated_at_ms";

  private static final String SQL_DELETE_SEGMENT =
      "DELETE FROM segment_stats WHERE table_name=? AND segment_name=?";

  private static final String SQL_DELETE_COL =
      "DELETE FROM segment_col_stats WHERE table_name=? AND segment_name=?";

  private static final String SQL_PURGE_TABLE_SEG =
      "DELETE FROM segment_stats WHERE table_name=?";

  private static final String SQL_PURGE_TABLE_COL =
      "DELETE FROM segment_col_stats WHERE table_name=?";

  private static final String SQL_PURGE_ALL_SEG = "DELETE FROM segment_stats";
  private static final String SQL_PURGE_ALL_COL = "DELETE FROM segment_col_stats";

  private final Path _dbDirectory;
  private final Path _dbPath;

  private final int _readPoolSize;
  private final long _readBorrowTimeoutMs;

  /// Write connection — all mutations go through this; guarded by _writeLock.
  /// Volatile because [#openConnections()] and [#closeConnections()] assign it outside the
  /// `_writeLock` that guards every other access, during init and rebuild.
  private volatile Connection _writeConn;
  /// Serialises writers. Needed even though SQLite is itself thread-safe, because a write here is
  /// a multi-statement transaction (`addBatch` / `executeBatch` / `commit`) and the transaction
  /// boundary is per-CONNECTION state: two threads interleaving on this shared connection would
  /// let one thread's `commit()` publish another's half-written batch.
  ///
  /// It does NOT make readers wait. The database runs in WAL mode, so a writer never blocks a
  /// reader and a reader never blocks the writer; this lock is only ever contended between
  /// concurrent writers.
  private final Object _writeLock = new Object();

  /// Pool of read-only connections. Each reader borrows a connection, uses it,
  /// then returns it via `offer()`. Sized at the configured pool size.
  private final ArrayBlockingQueue<PooledReader> _readPool;

  /// Per-table write counter backing the rollup cache; see [#aggregate(String)].
  private final Map<String, AtomicLong> _versions = new ConcurrentHashMap<>();

  /// Cached per-table rollups, each stamped with the [#_versions] value it was computed from.
  private final Map<String, CachedAggregate> _aggregates = new ConcurrentHashMap<>();

  private volatile boolean _closed = false;

  /// Constructs a new `SqliteStatsStore` that stores its database in the given directory.
  /// The database file will be `<dbDirectory>/broker-stats.sqlite`.
  ///
  /// @param dbDirectory directory in which to store the database file; created if absent
  public SqliteStatsStore(Path dbDirectory) {
    this(dbDirectory, DEFAULT_READ_POOL_SIZE, DEFAULT_READ_BORROW_TIMEOUT_MS);
  }

  /// @param dbDirectory          directory in which to store the database file; created if absent
  /// @param readPoolSize         number of pooled readers; the pool is a bound, so this caps how
  ///                             many estimates can be served concurrently
  /// @param readBorrowTimeoutMs  how long a reader waits for a pooled connection before giving up
  ///                             and reporting no statistics
  public SqliteStatsStore(Path dbDirectory, int readPoolSize, long readBorrowTimeoutMs) {
    _dbDirectory = dbDirectory;
    _dbPath = dbDirectory.resolve(DB_FILE_NAME);
    _readPoolSize = readPoolSize > 0 ? readPoolSize : DEFAULT_READ_POOL_SIZE;
    _readBorrowTimeoutMs = readBorrowTimeoutMs > 0 ? readBorrowTimeoutMs : DEFAULT_READ_BORROW_TIMEOUT_MS;
    _readPool = new ArrayBlockingQueue<>(_readPoolSize);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void init()
      throws StatsStoreException {
    try {
      openAndApplySchema();
    } catch (Exception firstEx) {
      LOGGER.warn(
          "Failed to open stats store at {}; deleting and retrying from scratch. Cause: {}",
          _dbPath, firstEx.getMessage(), firstEx);
      closeConnections();
      deleteDbFiles();
      try {
        openAndApplySchema();
      } catch (Exception secondEx) {
        throw new StatsStoreException(
            "Cannot initialise SqliteStatsStore at " + _dbPath, secondEx);
      }
    }
  }

  private void openAndApplySchema()
      throws Exception {
    try {
      openConnections();
    } catch (Exception e) {
      // Leave nothing bound to a database init() may be about to delete: a surviving connection
      // would keep the old inode alive and serve reads from a file nothing writes to any more.
      closeConnections();
      throw e;
    }
  }

  private void openConnections()
      throws Exception {
    Files.createDirectories(_dbDirectory);
    String jdbcUrl = "jdbc:sqlite:" + _dbPath.toAbsolutePath();

    // Open the shared writer connection.
    // Set WAL and synchronous PRAGMAs with autoCommit=true (WAL mode change cannot be done
    // inside a transaction), then switch to manual-commit mode for subsequent writes.
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try (Statement st = conn.createStatement()) {
      st.execute("PRAGMA journal_mode=WAL");
      st.execute("PRAGMA synchronous=NORMAL");
    }
    applySchema(conn);
    conn.setAutoCommit(false);
    _writeConn = conn;

    // Open read-only connections for the pool
    for (int i = 0; i < _readPoolSize; i++) {
      Connection rConn = DriverManager.getConnection(jdbcUrl);
      rConn.setAutoCommit(true);
      try (Statement st = rConn.createStatement()) {
        st.execute("PRAGMA journal_mode=WAL");
        st.execute("PRAGMA synchronous=NORMAL");
      }
      PooledReader reader;
      try {
        reader = new PooledReader(rConn);
      } catch (SQLException e) {
        // Nothing owns rConn yet, and closing it closes any statement compiled before the failure.
        closeQuietly(rConn);
        throw e;
      }
      if (!_readPool.offer(reader)) {
        reader.close();
      }
    }
  }

  /// Closes and forgets every connection this store holds. Safe to call repeatedly.
  private void closeConnections() {
    // A rebuild drops the file the cached rollups were computed from, so they cannot outlive it.
    _aggregates.clear();
    _versions.clear();
    closeQuietly(_writeConn);
    _writeConn = null;
    PooledReader pooled;
    while ((pooled = _readPool.poll()) != null) {
      // Closes the cached statements as well as the connection.
      pooled.close();
    }
  }

  /// Creates the schema on an empty database, or verifies that an existing one matches
  /// [#SCHEMA_VERSION].
  ///
  /// SQLite's own `user_version` pragma carries the version, so no bookkeeping table and no
  /// migration library is needed for what is a derived, rebuildable cache. A mismatch throws, which
  /// [#init()] turns into delete-and-recreate.
  private static void applySchema(Connection conn)
      throws SQLException {
    try (Statement st = conn.createStatement()) {
      int version;
      try (ResultSet rs = st.executeQuery("PRAGMA user_version")) {
        version = rs.next() ? rs.getInt(1) : 0;
      }
      if (version == SCHEMA_VERSION) {
        return;
      }
      if (version != 0) {
        throw new SQLException("Stats store schema version " + version + " does not match expected "
            + SCHEMA_VERSION + "; the store will be rebuilt");
      }
      for (String ddl : SCHEMA_DDL) {
        st.execute(ddl);
      }
      st.execute("PRAGMA user_version=" + SCHEMA_VERSION);
    }
  }

  /// Deletes the SQLite DB file and its WAL / SHM siblings if they exist.
  private void deleteDbFiles() {
    tryDelete(_dbPath);
    tryDelete(_dbDirectory.resolve(DB_FILE_NAME + "-wal"));
    tryDelete(_dbDirectory.resolve(DB_FILE_NAME + "-shm"));
  }

  private static void tryDelete(Path p) {
    try {
      Files.deleteIfExists(p);
    } catch (IOException e) {
      LOGGER.warn("Could not delete {}: {}", p, e.getMessage());
    }
  }

  @Override
  public void close() {
    _closed = true;
    synchronized (_writeLock) {
      closeQuietly(_writeConn);
      _writeConn = null;
    }
    PooledReader r;
    while ((r = _readPool.poll()) != null) {
      r.close();
    }
  }

  /// Returns the writer, or throws if the store closed between [#checkOpen()] and acquiring the
  /// lock. Without this a writer that passed the flag check would NPE on a nulled connection, and
  /// an NPE is neither rolled back here nor caught by the listeners that call this.
  private Connection writeConnection()
      throws StatsStoreException {
    if (_writeConn == null) {
      throw new StatsStoreException("SqliteStatsStore is closed");
    }
    return _writeConn;
  }

  private static void closeQuietly(@Nullable Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOGGER.debug("Error closing connection", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Write operations
  // ---------------------------------------------------------------------------

  @Override
  public void upsertSegmentStats(String tableNameWithType, List<SegmentStatsRow> rows)
      throws StatsStoreException {
    checkOpen();
    long now = System.currentTimeMillis();
    synchronized (_writeLock) {
      try {
        try (PreparedStatement ps = writeConnection().prepareStatement(SQL_UPSERT_SEGMENT)) {
          for (SegmentStatsRow row : rows) {
            ps.setString(1, tableNameWithType);
            ps.setString(2, row.segmentName());
            ps.setLong(3, row.crc());
            ps.setLong(4, row.totalDocs());
            ps.setLong(5, row.sizeBytes());
            ps.setLong(6, row.startTimeMs());
            ps.setLong(7, row.endTimeMs());
            ps.setInt(8, row.consuming() ? 1 : 0);
            ps.setLong(9, now);
            ps.addBatch();
          }
          ps.executeBatch();
        }
        writeConnection().commit();
        invalidate(tableNameWithType);
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
        invalidate(tableNameWithType);
        throw new StatsStoreException("upsertSegmentStats failed for " + tableNameWithType, e);
      }
    }
  }

  @Override
  public void upsertSegmentColumnStats(String tableNameWithType, List<SegmentColumnStatsRow> rows)
      throws StatsStoreException {
    checkOpen();
    long now = System.currentTimeMillis();
    synchronized (_writeLock) {
      try {
        try (PreparedStatement ps = writeConnection().prepareStatement(SQL_UPSERT_COL)) {
          for (SegmentColumnStatsRow row : rows) {
            ps.setString(1, tableNameWithType);
            ps.setString(2, row.segmentName());
            ps.setString(3, row.columnName());
            ps.setLong(4, row.ndv());
            ps.setString(5, row.minValue());
            ps.setString(6, row.maxValue());
            ps.setInt(7, row.minTrusted() ? 1 : 0);
            ps.setDouble(8, row.avgBytesPerValue());
            ps.setDouble(9, row.nullFraction());
            ps.setString(10, row.valueType() == null ? null : row.valueType().name());
            ps.setLong(11, now);
            ps.addBatch();
          }
          ps.executeBatch();
        }
        writeConnection().commit();
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
        throw new StatsStoreException(
            "upsertSegmentColumnStats failed for " + tableNameWithType, e);
      }
    }
  }

  @Override
  public void removeSegments(String tableNameWithType, Collection<String> segmentNames)
      throws StatsStoreException {
    checkOpen();
    if (segmentNames.isEmpty()) {
      return;
    }
    synchronized (_writeLock) {
      try {
        try (PreparedStatement psSeg = writeConnection().prepareStatement(SQL_DELETE_SEGMENT);
            PreparedStatement psCol = writeConnection().prepareStatement(SQL_DELETE_COL)) {
          for (String seg : segmentNames) {
            psSeg.setString(1, tableNameWithType);
            psSeg.setString(2, seg);
            psSeg.addBatch();
            psCol.setString(1, tableNameWithType);
            psCol.setString(2, seg);
            psCol.addBatch();
          }
          psSeg.executeBatch();
          psCol.executeBatch();
        }
        writeConnection().commit();
        invalidate(tableNameWithType);
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
        invalidate(tableNameWithType);
        throw new StatsStoreException("removeSegments failed for " + tableNameWithType, e);
      }
    }
  }

  @Override
  public boolean hasConsumingSegments(String tableNameWithType)
      throws StatsStoreException {
    return aggregate(tableNameWithType)._hasConsuming;
  }


  @Override
  public Set<String> getTables()
      throws StatsStoreException {
    checkOpen();
    PooledReader reader = borrowReader();
    try {
      return reader.listTables();
    } catch (SQLException e) {
      throw new StatsStoreException("getTables failed", e);
    } finally {
      returnReader(reader);
    }
  }

  @Override
  public void purgeTable(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    synchronized (_writeLock) {
      try {
        try (PreparedStatement psSeg = writeConnection().prepareStatement(SQL_PURGE_TABLE_SEG);
            PreparedStatement psCol = writeConnection().prepareStatement(SQL_PURGE_TABLE_COL)) {
          psSeg.setString(1, tableNameWithType);
          psSeg.executeUpdate();
          psCol.setString(1, tableNameWithType);
          psCol.executeUpdate();
        }
        writeConnection().commit();
        // Drop the cache entry outright rather than only stamping it: nothing is left to roll up.
        _aggregates.remove(tableNameWithType);
        _versions.remove(tableNameWithType);
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
        invalidate(tableNameWithType);
        throw new StatsStoreException("purgeTable failed for " + tableNameWithType, e);
      }
    }
  }

  @Override
  public void purgeAll()
      throws StatsStoreException {
    checkOpen();
    synchronized (_writeLock) {
      try {
        try (Statement st = writeConnection().createStatement()) {
          st.execute(SQL_PURGE_ALL_SEG);
          st.execute(SQL_PURGE_ALL_COL);
        }
        writeConnection().commit();
        _aggregates.clear();
        _versions.clear();
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
        invalidateAll();
        throw new StatsStoreException("purgeAll failed", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Read operations
  // ---------------------------------------------------------------------------

  @Override
  public Map<String, Long> getSegmentCrcs(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    PooledReader reader = borrowReader();
    try {
      return reader.segmentCrcs(tableNameWithType);
    } catch (SQLException e) {
      throw new StatsStoreException("getSegmentCrcs failed for " + tableNameWithType, e);
    } finally {
      returnReader(reader);
    }
  }

  @Override
  @Nullable
  public TableStatistics getTableStats(String tableNameWithType)
      throws StatsStoreException {
    return aggregate(tableNameWithType)._stats;
  }


  /// Returns per-column statistics aggregated across all non-consuming segments for the given
  /// table and column, or `null` if no rows exist.
  ///
  /// #### NDV
  /// Returned as `MAX(ndv)` over segments with [StatConfidence#ESTIMATED]. The
  /// true value lies in `[MAX(ndv), min(SUM(ndv), tableRowCount)]`; we report the lower
  /// bound (MAX) because the upper bound is not representable as a single value in the contract.
  ///
  /// #### Min/Max
  /// Compared numerically when both values parse as [Double], else lexically. Comparison
  /// is done in Java (not SQL) to avoid SQLite TEXT-affinity ordering issues (e.g. "9" > "10").
  @Override
  @Nullable
  public ColumnStatistics getColumnStats(String tableNameWithType, String columnName)
      throws StatsStoreException {
    checkOpen();
    PooledReader reader = borrowReader();
    try {
      return reader.columnStats(tableNameWithType, columnName);
    } catch (SQLException e) {
      throw new StatsStoreException(
          "getColumnStats failed for " + tableNameWithType + "." + columnName, e);
    } finally {
      returnReader(reader);
    }
  }

  @Override
  public OptionalLong estimateRowsInTimeRange(String tableNameWithType, long startMs, long endMs)
      throws StatsStoreException {
    checkOpen();
    PooledReader reader = borrowReader();
    try {
      return reader.rowsInTimeRange(tableNameWithType, startMs, endMs);
    } catch (SQLException e) {
      throw new StatsStoreException(
          "estimateRowsInTimeRange failed for " + tableNameWithType, e);
    } finally {
      returnReader(reader);
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void checkOpen()
      throws StatsStoreException {
    if (_closed) {
      throw new StatsStoreException("SqliteStatsStore is closed");
    }
  }

  private PooledReader borrowReader()
      throws StatsStoreException {
    PooledReader reader = _readPool.poll();
    if (reader != null) {
      return reader;
    }
    // The pool is a bound, not a hint. Opening a connection per waiting reader would make the
    // exhausted case -- the normal one on a broker with more planning threads than pooled
    // connections -- the most expensive path: every read would pay a full database open plus its
    // own page cache, and nothing would cap the file descriptors a burst can hold open. Waiting
    // briefly instead keeps the cost bounded, and the caller already degrades to no statistics
    // rather than failing a query.
    try {
      reader = _readPool.poll(_readBorrowTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StatsStoreException("Interrupted while waiting for a read connection", e);
    }
    if (reader == null) {
      throw new StatsStoreException(
          "Timed out after " + _readBorrowTimeoutMs + "ms waiting for one of " + _readPoolSize
              + " read connections");
    }
    // A closed store must not hand out a connection it is in the middle of tearing down.
    checkOpen();
    return reader;
  }

  private void returnReader(@Nullable PooledReader reader) {
    if (reader == null) {
      return;
    }
    // Do not re-pool into a store that closed while this read was in flight, or close() would
    // leave a live connection -- and its statements -- behind.
    if (_closed || !_readPool.offer(reader)) {
      reader.close();
    }
  }

  /// A pooled read connection together with the statements compiled on it.
  ///
  /// SQLite compiles each statement into a VDBE program, and that compilation runs the query
  /// planner -- so re-preparing on every call re-does index selection. Measured on this schema:
  /// preparing is ~58% of total query cost for a table with 50 segments, where compilation
  /// dominates execution, falling to ~4% at 1000 segments. Most tables sit at the small end and
  /// the planner issues several of these per compile, so the statements are kept with their
  /// connection rather than rebuilt.
  ///
  /// Not thread-safe, and does not need to be: an instance is owned by exactly one thread between
  /// [#borrowReader()] and [#returnReader].
  /// A pooled read-only connection together with every prepared statement the store reads through.
  ///
  /// The statements are compiled once, when the reader is created, and reused for the life of the
  /// connection. SQLite's `prepare` compiles SQL to bytecode, query plan included, and for a table
  /// of a few dozen segments that compilation costs more than running the resulting plan; pooling
  /// the connection without its statements would pay it again on every read.
  ///
  /// Every read the store performs is a method here, so the set of statements a reader holds is
  /// fixed and each one has exactly one caller.
  ///
  /// Not thread-safe, and does not need to be: a reader is owned by the single thread that borrowed
  /// it from the pool until that thread returns it.
  private static final class PooledReader {

    // Both tables are consulted: a table could in principle have column rows without segment rows.
    private static final String SQL_LIST_TABLES =
        "SELECT table_name FROM segment_stats UNION SELECT table_name FROM segment_col_stats";

    private static final String SQL_GET_CRCS =
        "SELECT segment_name,crc FROM segment_stats WHERE table_name=?";

    /// Rollup and the consuming flag in one scan: both are asked for on every plan, and splitting
    /// them into two statements walked the same rows twice.
    private static final String SQL_TABLE_STATS =
        "SELECT SUM(CASE WHEN consuming=0 THEN total_docs ELSE 0 END),"
            + "SUM(CASE WHEN consuming=0 THEN size_bytes ELSE 0 END),"
            + "MAX(CASE WHEN consuming=0 THEN updated_at_ms ELSE 0 END),"
            + "SUM(CASE WHEN consuming=0 THEN 1 ELSE 0 END),"
            + "MAX(consuming) FROM segment_stats WHERE table_name=?";

    private static final String SQL_COL_STATS =
        "SELECT s.total_docs,c.ndv,c.min_value,c.max_value,c.min_trusted,c.avg_bytes,c.null_fraction,"
            + "c.value_type "
            + "FROM segment_col_stats c "
            + "JOIN segment_stats s ON s.table_name=c.table_name AND s.segment_name=c.segment_name "
            + "WHERE c.table_name=? AND c.column_name=? AND s.consuming=0";

    // The overlap predicate mirrors the Java-side check in rowsInTimeRange: it prunes segments that
    // cannot overlap [startMs, endMs) inside SQLite instead of materializing them over JDBC only to
    // be skipped. Segments with unknown times (-1 sentinels) must be retained -- they are included
    // conservatively by the Java logic.
    //
    // The UNION ALL arm emits a sentinel row (start_time_ms = -2, an otherwise impossible value)
    // whenever the table has any committed stats at all. It lets the caller distinguish "no segment
    // overlaps the range" (a real estimate of 0) from "no stats for this table" (empty) within a
    // SINGLE statement -- i.e. a single consistent snapshot; splitting this into a second existence
    // query would race with concurrent stats writes.
    private static final String SQL_TIME_RANGE =
        "SELECT total_docs,start_time_ms,end_time_ms "
            + "FROM segment_stats WHERE table_name=? AND consuming=0 "
            + "AND (start_time_ms=-1 OR end_time_ms=-1 OR (end_time_ms>? AND start_time_ms<?)) "
            + "UNION ALL SELECT -1,-2,-2 WHERE EXISTS("
            + "SELECT 1 FROM segment_stats WHERE table_name=? AND consuming=0)";

    private final Connection _conn;
    private final PreparedStatement _listTables;
    private final PreparedStatement _segmentCrcs;
    private final PreparedStatement _tableStats;
    private final PreparedStatement _columnStats;
    private final PreparedStatement _timeRange;

    /// @param conn the connection this reader owns from here on
    /// @throws SQLException if any statement fails to compile; the caller still owns `conn` and
    ///                      must close it, which also closes whatever compiled before the failure
    PooledReader(Connection conn)
        throws SQLException {
      _conn = conn;
      _listTables = conn.prepareStatement(SQL_LIST_TABLES);
      _segmentCrcs = conn.prepareStatement(SQL_GET_CRCS);
      _tableStats = conn.prepareStatement(SQL_TABLE_STATS);
      _columnStats = conn.prepareStatement(SQL_COL_STATS);
      _timeRange = conn.prepareStatement(SQL_TIME_RANGE);
    }

    Set<String> listTables()
        throws SQLException {
      Set<String> tables = new HashSet<>();
      try (ResultSet rs = _listTables.executeQuery()) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
      return tables;
    }

    Map<String, Long> segmentCrcs(String tableNameWithType)
        throws SQLException {
      Map<String, Long> result = new HashMap<>();
      _segmentCrcs.clearParameters();
      _segmentCrcs.setString(1, tableNameWithType);
      try (ResultSet rs = _segmentCrcs.executeQuery()) {
        while (rs.next()) {
          result.put(rs.getString(1), rs.getLong(2));
        }
      }
      return result;
    }

    CachedAggregate tableStats(String tableNameWithType, long version)
        throws SQLException {
      _tableStats.clearParameters();
      _tableStats.setString(1, tableNameWithType);
      try (ResultSet rs = _tableStats.executeQuery()) {
        // Aggregates always return one row, holding SQL NULLs when the table has none.
        if (!rs.next()) {
          return new CachedAggregate(version, null, false);
        }
        long totalDocs = rs.getLong(1);
        long sizeBytes = rs.getLong(2);
        long maxUpdatedAt = rs.getLong(3);
        long committed = rs.getLong(4);
        boolean hasConsuming = rs.getLong(5) == 1;
        if (committed == 0) {
          return new CachedAggregate(version, null, hasConsuming);
        }
        return new CachedAggregate(version, TableStatistics.builder()
            .rowCount(totalDocs, StatConfidence.EXACT)
            .tableSizeBytes(sizeBytes, StatConfidence.EXACT)
            .updatedAtMs(maxUpdatedAt)
            .build(), hasConsuming);
      }
    }

    @Nullable
    ColumnStatistics columnStats(String tableNameWithType, String columnName)
        throws SQLException {
      StatsAggregations.ColumnStatsAccumulator accumulator = new StatsAggregations.ColumnStatsAccumulator();
      _columnStats.clearParameters();
      _columnStats.setString(1, tableNameWithType);
      _columnStats.setString(2, columnName);
      try (ResultSet rs = _columnStats.executeQuery()) {
        while (rs.next()) {
          accumulator.add(rs.getLong(1), SegmentColumnStatsRow.builder()
              .segmentName("")
              .columnName(columnName)
              .ndv(rs.getLong(2))
              .bounds(rs.getString(3), rs.getString(4), ColumnValueType.fromName(rs.getString(8)))
              .minTrusted(rs.getInt(5) != 0)
              .avgBytesPerValue(rs.getDouble(6))
              .nullFraction(rs.getDouble(7))
              .build());
        }
      }
      return accumulator.isEmpty() ? null : accumulator.build(columnName);
    }

    OptionalLong rowsInTimeRange(String tableNameWithType, long startMs, long endMs)
        throws SQLException {
      long totalRows = 0;
      boolean hasAnyRow = false;

      _timeRange.clearParameters();
      _timeRange.setString(1, tableNameWithType);
      _timeRange.setLong(2, startMs);
      _timeRange.setLong(3, endMs);
      _timeRange.setString(4, tableNameWithType);
      try (ResultSet rs = _timeRange.executeQuery()) {
        while (rs.next()) {
          long docs = rs.getLong(1);
          long segStart = rs.getLong(2);
          long segEnd = rs.getLong(3);

          hasAnyRow = true;

          // Existence sentinel (see SQL_TIME_RANGE): committed stats exist, contributes 0 rows
          if (segStart == -2) {
            continue;
          }

          // The SQL predicate already prunes non-overlapping segments; the shared helper decides
          // how much of a surviving segment counts (see StatsAggregations#overlapRows).
          totalRows += StatsAggregations.overlapRows(docs, segStart, segEnd, startMs, endMs);
        }
      }

      return hasAnyRow ? OptionalLong.of(totalRows) : OptionalLong.empty();
    }

    void close() {
      // Closing the connection would close these too, but closing them explicitly keeps one
      // failure from hiding the rest.
      for (PreparedStatement ps : List.of(_listTables, _segmentCrcs, _tableStats, _columnStats, _timeRange)) {
        try {
          ps.close();
        } catch (SQLException e) {
          LOGGER.debug("Closing pooled statement failed", e);
        }
      }
      closeQuietly(_conn);
    }
  }

  private static void rollbackQuietly(@Nullable Connection conn) {
    if (conn == null) {
      return;
    }
    try {
      conn.rollback();
    } catch (SQLException e) {
      LOGGER.debug("Rollback failed", e);
    }
  }

  /// Returns the cached rollup for a table, recomputing it only when a write has landed since the
  /// cached copy was taken.
  ///
  /// Query planning asks for these on every compile while writes arrive at segment-push cadence,
  /// so without a cache every plan would scan all of a table's segment rows -- on a table with
  /// hundreds of thousands of segments that lands directly in planning latency. The version stamp
  /// is what makes caching safe without holding the write lock: a rollup computed from rows that
  /// changed underneath it is used for this call but not published.
  private CachedAggregate aggregate(String tableNameWithType)
      throws StatsStoreException {
    AtomicLong version = _versions.computeIfAbsent(tableNameWithType, k -> new AtomicLong());
    long observed = version.get();
    CachedAggregate cached = _aggregates.get(tableNameWithType);
    if (cached != null && cached._version == observed) {
      return cached;
    }
    CachedAggregate computed = computeAggregate(tableNameWithType, observed);
    if (version.get() == observed) {
      _aggregates.put(tableNameWithType, computed);
    }
    return computed;
  }

  private CachedAggregate computeAggregate(String tableNameWithType, long version)
      throws StatsStoreException {
    checkOpen();
    PooledReader reader = borrowReader();
    try {
      return reader.tableStats(tableNameWithType, version);
    } catch (SQLException e) {
      throw new StatsStoreException("getTableStats failed for " + tableNameWithType, e);
    } finally {
      returnReader(reader);
    }
  }

  /// Marks a table's cached rollup stale. Called from the write paths, which hold `_writeLock`.
  private void invalidate(String tableNameWithType) {
    _versions.computeIfAbsent(tableNameWithType, k -> new AtomicLong()).incrementAndGet();
    _aggregates.remove(tableNameWithType);
  }

  /// Marks every table's cached rollup stale, for writes that span tables.
  private void invalidateAll() {
    _aggregates.clear();
    _versions.values().forEach(AtomicLong::incrementAndGet);
  }

  private static final class CachedAggregate {
    private final long _version;
    @Nullable
    private final TableStatistics _stats;
    private final boolean _hasConsuming;

    CachedAggregate(long version, @Nullable TableStatistics stats, boolean hasConsuming) {
      _version = version;
      _stats = stats;
      _hasConsuming = hasConsuming;
    }
  }
}
