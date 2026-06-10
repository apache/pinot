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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SQLite-backed implementation of {@link StatsStore}.
 *
 * <h3>Threading model</h3>
 * <p>Uses a single shared writer {@link Connection} guarded by {@code synchronized} on
 * {@code _writeLock}, plus a small pool ({@value #READ_POOL_SIZE}) of read-only connections
 * served via a blocking queue. Reads from multiple threads proceed concurrently (each
 * borrows a connection from the pool, uses it, then returns it). The writer connection sets
 * {@code PRAGMA journal_mode=WAL} so readers never block writers.
 *
 * <h3>Corruption handling</h3>
 * <p>{@link #init()} attempts to open and migrate the database. On any failure it logs a
 * warning, deletes the DB file and its WAL/SHM siblings, then retries once from scratch.
 * Only a second consecutive failure is propagated as {@link StatsStoreException}.
 */
public class SqliteStatsStore implements StatsStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteStatsStore.class);

  /** Number of read connections in the shared pool. */
  static final int READ_POOL_SIZE = 4;

  private static final String DB_FILE_NAME = "broker-stats.sqlite";
  private static final String MIGRATION_LOCATION = "classpath:db/broker-stats-migration";

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
          + "max_value,min_trusted,avg_bytes,null_fraction,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?) "
          + "ON CONFLICT(table_name,segment_name,column_name) DO UPDATE SET "
          + "ndv=excluded.ndv,min_value=excluded.min_value,max_value=excluded.max_value,"
          + "min_trusted=excluded.min_trusted,avg_bytes=excluded.avg_bytes,"
          + "null_fraction=excluded.null_fraction,updated_at_ms=excluded.updated_at_ms";

  private static final String SQL_DELETE_SEGMENT =
      "DELETE FROM segment_stats WHERE table_name=? AND segment_name=?";

  private static final String SQL_DELETE_COL =
      "DELETE FROM segment_col_stats WHERE table_name=? AND segment_name=?";

  private static final String SQL_GET_CRCS =
      "SELECT segment_name,crc FROM segment_stats WHERE table_name=?";

  private static final String SQL_TABLE_STATS =
      "SELECT SUM(total_docs),SUM(size_bytes),MAX(updated_at_ms),COUNT(*) "
          + "FROM segment_stats WHERE table_name=? AND consuming=0";

  private static final String SQL_COL_STATS =
      "SELECT s.total_docs,c.ndv,c.min_value,c.max_value,c.min_trusted,c.avg_bytes,c.null_fraction "
          + "FROM segment_col_stats c "
          + "JOIN segment_stats s ON s.table_name=c.table_name AND s.segment_name=c.segment_name "
          + "WHERE c.table_name=? AND c.column_name=? AND s.consuming=0";

  private static final String SQL_TIME_RANGE =
      "SELECT total_docs,start_time_ms,end_time_ms "
          + "FROM segment_stats WHERE table_name=? AND consuming=0";

  private static final String SQL_PURGE_TABLE_SEG =
      "DELETE FROM segment_stats WHERE table_name=?";

  private static final String SQL_PURGE_TABLE_COL =
      "DELETE FROM segment_col_stats WHERE table_name=?";

  private static final String SQL_PURGE_ALL_SEG = "DELETE FROM segment_stats";
  private static final String SQL_PURGE_ALL_COL = "DELETE FROM segment_col_stats";

  private final Path _dbDirectory;
  private final Path _dbPath;

  /** Write connection — all mutations go through this; guarded by _writeLock. */
  private Connection _writeConn;
  private final Object _writeLock = new Object();

  /**
   * Pool of read-only connections. Each reader borrows a connection, uses it,
   * then returns it via {@code offer()}. Sized at {@value #READ_POOL_SIZE}.
   */
  private final java.util.concurrent.ArrayBlockingQueue<Connection> _readPool =
      new java.util.concurrent.ArrayBlockingQueue<>(READ_POOL_SIZE);

  private volatile boolean _closed = false;

  /**
   * Constructs a new {@code SqliteStatsStore} that stores its database in the given directory.
   * The database file will be {@code <dbDirectory>/broker-stats.sqlite}.
   *
   * @param dbDirectory directory in which to store the database file; created if absent
   */
  public SqliteStatsStore(Path dbDirectory) {
    _dbDirectory = dbDirectory;
    _dbPath = dbDirectory.resolve(DB_FILE_NAME);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void init()
      throws StatsStoreException {
    try {
      openAndMigrate();
    } catch (Exception firstEx) {
      LOGGER.warn(
          "Failed to open stats store at {}; deleting and retrying from scratch. Cause: {}",
          _dbPath, firstEx.getMessage(), firstEx);
      deleteDbFiles();
      try {
        openAndMigrate();
      } catch (Exception secondEx) {
        throw new StatsStoreException(
            "Cannot initialise SqliteStatsStore at " + _dbPath, secondEx);
      }
    }
  }

  private void openAndMigrate()
      throws Exception {
    Files.createDirectories(_dbDirectory);
    String jdbcUrl = "jdbc:sqlite:" + _dbPath.toAbsolutePath();

    // Run Flyway migrations first (uses its own connection internally)
    Flyway flyway = Flyway.configure()
        .dataSource(jdbcUrl, null, null)
        .locations(MIGRATION_LOCATION)
        .load();
    flyway.migrate();

    // Open the shared writer connection.
    // Set WAL and synchronous PRAGMAs with autoCommit=true (WAL mode change cannot be done
    // inside a transaction), then switch to manual-commit mode for subsequent writes.
    Connection conn = DriverManager.getConnection(jdbcUrl);
    try (Statement st = conn.createStatement()) {
      st.execute("PRAGMA journal_mode=WAL");
      st.execute("PRAGMA synchronous=NORMAL");
    }
    conn.setAutoCommit(false);
    _writeConn = conn;

    // Open read-only connections for the pool
    for (int i = 0; i < READ_POOL_SIZE; i++) {
      Connection rConn = DriverManager.getConnection(jdbcUrl);
      rConn.setAutoCommit(true);
      try (Statement st = rConn.createStatement()) {
        st.execute("PRAGMA journal_mode=WAL");
        st.execute("PRAGMA synchronous=NORMAL");
      }
      _readPool.offer(rConn);
    }
  }

  /** Deletes the SQLite DB file and its WAL / SHM siblings if they exist. */
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
    Connection c;
    while ((c = _readPool.poll()) != null) {
      closeQuietly(c);
    }
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
        try (PreparedStatement ps = _writeConn.prepareStatement(SQL_UPSERT_SEGMENT)) {
          for (SegmentStatsRow row : rows) {
            ps.setString(1, tableNameWithType);
            ps.setString(2, row.getSegmentName());
            ps.setLong(3, row.getCrc());
            ps.setLong(4, row.getTotalDocs());
            ps.setLong(5, row.getSizeBytes());
            ps.setLong(6, row.getStartTimeMs());
            ps.setLong(7, row.getEndTimeMs());
            ps.setInt(8, row.isConsuming() ? 1 : 0);
            ps.setLong(9, now);
            ps.addBatch();
          }
          ps.executeBatch();
        }
        _writeConn.commit();
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
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
        try (PreparedStatement ps = _writeConn.prepareStatement(SQL_UPSERT_COL)) {
          for (SegmentColumnStatsRow row : rows) {
            ps.setString(1, tableNameWithType);
            ps.setString(2, row.getSegmentName());
            ps.setString(3, row.getColumnName());
            ps.setLong(4, row.getNdv());
            ps.setString(5, row.getMinValue());
            ps.setString(6, row.getMaxValue());
            ps.setInt(7, row.isMinTrusted() ? 1 : 0);
            ps.setDouble(8, row.getAvgBytesPerValue());
            ps.setDouble(9, row.getNullFraction());
            ps.setLong(10, now);
            ps.addBatch();
          }
          ps.executeBatch();
        }
        _writeConn.commit();
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
        try (PreparedStatement psSeg = _writeConn.prepareStatement(SQL_DELETE_SEGMENT);
            PreparedStatement psCol = _writeConn.prepareStatement(SQL_DELETE_COL)) {
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
        _writeConn.commit();
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
        throw new StatsStoreException("removeSegments failed for " + tableNameWithType, e);
      }
    }
  }

  @Override
  public void purgeTable(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    synchronized (_writeLock) {
      try {
        try (PreparedStatement psSeg = _writeConn.prepareStatement(SQL_PURGE_TABLE_SEG);
            PreparedStatement psCol = _writeConn.prepareStatement(SQL_PURGE_TABLE_COL)) {
          psSeg.setString(1, tableNameWithType);
          psSeg.executeUpdate();
          psCol.setString(1, tableNameWithType);
          psCol.executeUpdate();
        }
        _writeConn.commit();
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
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
        try (Statement st = _writeConn.createStatement()) {
          st.execute(SQL_PURGE_ALL_SEG);
          st.execute(SQL_PURGE_ALL_COL);
        }
        _writeConn.commit();
      } catch (SQLException e) {
        rollbackQuietly(_writeConn);
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
    Connection conn = borrowReadConn();
    try {
      Map<String, Long> result = new HashMap<>();
      try (PreparedStatement ps = conn.prepareStatement(SQL_GET_CRCS)) {
        ps.setString(1, tableNameWithType);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            result.put(rs.getString(1), rs.getLong(2));
          }
        }
      }
      return result;
    } catch (SQLException e) {
      throw new StatsStoreException("getSegmentCrcs failed for " + tableNameWithType, e);
    } finally {
      returnReadConn(conn);
    }
  }

  @Override
  @Nullable
  public TableStatistics getTableStats(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    Connection conn = borrowReadConn();
    try {
      try (PreparedStatement ps = conn.prepareStatement(SQL_TABLE_STATS)) {
        ps.setString(1, tableNameWithType);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return null;
          }
          long totalDocs = rs.getLong(1);
          long sizeBytes = rs.getLong(2);
          long maxUpdatedAt = rs.getLong(3);
          long count = rs.getLong(4);
          if (count == 0) {
            return null;
          }
          return TableStatistics.builder()
              .rowCount(totalDocs, StatConfidence.EXACT)
              .tableSizeBytes(sizeBytes, StatConfidence.EXACT)
              .updatedAtMs(maxUpdatedAt)
              .build();
        }
      }
    } catch (SQLException e) {
      throw new StatsStoreException("getTableStats failed for " + tableNameWithType, e);
    } finally {
      returnReadConn(conn);
    }
  }

  /**
   * Returns per-column statistics aggregated across all non-consuming segments for the given
   * table and column, or {@code null} if no rows exist.
   *
   * <h4>NDV</h4>
   * <p>Returned as {@code MAX(ndv)} over segments with {@link StatConfidence#ESTIMATED}. The
   * true value lies in {@code [MAX(ndv), min(SUM(ndv), tableRowCount)]}; we report the lower
   * bound (MAX) because the upper bound is not representable as a single value in the contract.
   *
   * <h4>Min/Max</h4>
   * <p>Compared numerically when both values parse as {@link Double}, else lexically. Comparison
   * is done in Java (not SQL) to avoid SQLite TEXT-affinity ordering issues (e.g. "9" &gt; "10").
   */
  @Override
  @Nullable
  public ColumnStatistics getColumnStats(String tableNameWithType, String columnName)
      throws StatsStoreException {
    checkOpen();
    Connection conn = borrowReadConn();
    try {
      List<long[]> docsNdvTrusted = new ArrayList<>();
      List<String[]> minMax = new ArrayList<>();
      List<double[]> avgBytesNullFrac = new ArrayList<>();

      try (PreparedStatement ps = conn.prepareStatement(SQL_COL_STATS)) {
        ps.setString(1, tableNameWithType);
        ps.setString(2, columnName);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            long docs = rs.getLong(1);
            long ndv = rs.getLong(2);
            String minVal = rs.getString(3);
            String maxVal = rs.getString(4);
            boolean minTrusted = rs.getInt(5) != 0;
            double avgBytes = rs.getDouble(6);
            double nullFrac = rs.getDouble(7);

            docsNdvTrusted.add(new long[]{docs, ndv, minTrusted ? 1L : 0L});
            minMax.add(new String[]{minVal, maxVal});
            avgBytesNullFrac.add(new double[]{avgBytes, nullFrac});
          }
        }
      }

      if (docsNdvTrusted.isEmpty()) {
        return null;
      }

      // Aggregate
      long maxNdv = -1;
      boolean anyUntrustedMin = false;
      long totalDocs = 0;
      double weightedAvgBytes = 0;
      double weightedNullFrac = 0;
      String globalMin = null;
      String globalMax = null;

      for (int i = 0; i < docsNdvTrusted.size(); i++) {
        long docs = docsNdvTrusted.get(i)[0];
        long ndv = docsNdvTrusted.get(i)[1];
        boolean trusted = docsNdvTrusted.get(i)[2] != 0;
        double avgBytes = avgBytesNullFrac.get(i)[0];
        double nullFrac = avgBytesNullFrac.get(i)[1];
        String minVal = minMax.get(i)[0];
        String maxVal = minMax.get(i)[1];

        if (ndv > maxNdv) {
          maxNdv = ndv;
        }
        if (!trusted) {
          anyUntrustedMin = true;
        }
        totalDocs += docs;
        weightedAvgBytes += avgBytes * docs;
        weightedNullFrac += nullFrac * docs;
        globalMin = minOf(globalMin, minVal);
        globalMax = maxOf(globalMax, maxVal);
      }

      double finalAvgBytes = totalDocs > 0 ? weightedAvgBytes / totalDocs : -1;
      double finalNullFrac = totalDocs > 0 ? weightedNullFrac / totalDocs : -1;

      // Build comparable min/max values
      Comparable<?> minComparable = toComparable(globalMin);
      Comparable<?> maxComparable = toComparable(globalMax);

      return ColumnStatistics.builder()
          .columnName(columnName)
          .ndv(maxNdv, StatConfidence.ESTIMATED)
          .minValue(minComparable)
          .maxValue(maxComparable)
          .minTrusted(!anyUntrustedMin)
          .avgBytesPerValue(finalAvgBytes)
          .nullFraction(finalNullFrac)
          .build();
    } catch (SQLException e) {
      throw new StatsStoreException(
          "getColumnStats failed for " + tableNameWithType + "." + columnName, e);
    } finally {
      returnReadConn(conn);
    }
  }

  @Override
  public OptionalLong estimateRowsInTimeRange(String tableNameWithType, long startMs, long endMs)
      throws StatsStoreException {
    checkOpen();
    Connection conn = borrowReadConn();
    try {
      long totalRows = 0;
      boolean hasAnyRow = false;

      try (PreparedStatement ps = conn.prepareStatement(SQL_TIME_RANGE)) {
        ps.setString(1, tableNameWithType);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            long docs = rs.getLong(1);
            long segStart = rs.getLong(2);
            long segEnd = rs.getLong(3);

            hasAnyRow = true;

            // Unknown times (-1) → conservative: include all docs
            if (segStart == -1 || segEnd == -1) {
              totalRows += docs;
              continue;
            }

            // Check overlap with [startMs, endMs)
            if (segEnd <= startMs || segStart >= endMs) {
              // No overlap
              continue;
            }

            if (segStart >= startMs && segEnd <= endMs) {
              // Full overlap
              totalRows += docs;
            } else {
              // Partial overlap: linear interpolation
              long segDuration = segEnd - segStart;
              if (segDuration <= 0) {
                // Zero-length segment: include it if the point is in range
                if (segStart >= startMs && segStart < endMs) {
                  totalRows += docs;
                }
              } else {
                long overlapStart = Math.max(startMs, segStart);
                long overlapEnd = Math.min(endMs, segEnd);
                double fraction = (double) (overlapEnd - overlapStart) / segDuration;
                totalRows += Math.round(docs * fraction);
              }
            }
          }
        }
      }

      return hasAnyRow ? OptionalLong.of(totalRows) : OptionalLong.empty();
    } catch (SQLException e) {
      throw new StatsStoreException(
          "estimateRowsInTimeRange failed for " + tableNameWithType, e);
    } finally {
      returnReadConn(conn);
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

  private Connection borrowReadConn()
      throws StatsStoreException {
    Connection conn = _readPool.poll();
    if (conn == null) {
      // Pool exhausted — fall back to taking from the write connection path is not safe;
      // instead open a temporary connection.
      try {
        String jdbcUrl = "jdbc:sqlite:" + _dbPath.toAbsolutePath();
        conn = DriverManager.getConnection(jdbcUrl);
        conn.setAutoCommit(true);
      } catch (SQLException e) {
        throw new StatsStoreException("Cannot open fallback read connection", e);
      }
    }
    return conn;
  }

  private void returnReadConn(Connection conn) {
    if (conn == null) {
      return;
    }
    // Try to return to pool; if pool is full (e.g. fallback connection), close it
    if (!_readPool.offer(conn)) {
      closeQuietly(conn);
    }
  }

  private static void rollbackQuietly(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      LOGGER.debug("Rollback failed", e);
    }
  }

  /**
   * Returns the numerically-or-lexically smaller of {@code a} and {@code b}.
   * {@code null} is treated as "unknown" — the other value wins.
   */
  @Nullable
  private static String minOf(@Nullable String a, @Nullable String b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return compareValues(a, b) <= 0 ? a : b;
  }

  /**
   * Returns the numerically-or-lexically larger of {@code a} and {@code b}.
   * {@code null} is treated as "unknown" — the other value wins.
   */
  @Nullable
  private static String maxOf(@Nullable String a, @Nullable String b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return compareValues(a, b) >= 0 ? a : b;
  }

  /**
   * Compares two string-serialized values. If both parse as {@link Double}, uses numeric
   * comparison; otherwise falls back to lexical comparison. This avoids TEXT-affinity
   * ordering issues in SQLite (e.g. "9" &gt; "10" lexically but "9" &lt; "10" numerically).
   */
  private static int compareValues(String a, String b) {
    try {
      double da = Double.parseDouble(a);
      double db = Double.parseDouble(b);
      return Double.compare(da, db);
    } catch (NumberFormatException e) {
      return a.compareTo(b);
    }
  }

  /**
   * Converts a string-serialized value to a {@link Comparable} for use in
   * {@link ColumnStatistics}. Returns a {@link Double} if the string parses as a number,
   * otherwise returns the original {@link String}.
   */
  @Nullable
  private static Comparable<?> toComparable(@Nullable String value) {
    if (value == null) {
      return null;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return value;
    }
  }
}
