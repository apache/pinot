/*
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

package org.apache.pinot.thirdeye.datalayer;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tool to run database scripts
 */
public class ScriptRunner {

  private static final String DEFAULT_DELIMITER = ";";

  private final Connection connection;
  private final boolean autoCommit;

  private PrintWriter logWriter = new PrintWriter(System.out);
  private PrintWriter errorLogWriter = new PrintWriter(System.err);
  private String delimiter = DEFAULT_DELIMITER;

  /**
   * Default constructor
   */
  public ScriptRunner(Connection connection, boolean autoCommit) {
    this.connection = connection;
    this.autoCommit = autoCommit;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * Setter for logWriter property
   *
   * @param logWriter - the new value of the logWriter property
   */
  public void setLogWriter(PrintWriter logWriter) {
    this.logWriter = logWriter;
  }

  /**
   * Setter for errorLogWriter property
   *
   * @param errorLogWriter - the new value of the errorLogWriter property
   */
  public void setErrorLogWriter(PrintWriter errorLogWriter) {
    this.errorLogWriter = errorLogWriter;
  }

  /**
   * Runs an SQL script (read in using the Reader parameter)
   *
   * @param reader - the source of the script
   */
  public void runScript(Reader reader) throws IOException, SQLException {
    try {
      boolean originalAutoCommit = connection.getAutoCommit();
      try {
        if (originalAutoCommit != this.autoCommit) {
          connection.setAutoCommit(this.autoCommit);
        }
        runScript(connection, reader);
      } finally {
        connection.setAutoCommit(originalAutoCommit);
      }
    } catch (IOException | SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Error running script.  Cause: " + e, e);
    }
  }

  /**
   * Runs an SQL script (read in using the Reader parameter) using the connection passed in
   *
   * @param conn   - the connection to use for the script
   * @param reader - the source of the script
   *
   * @throws SQLException if any SQL errors occur
   * @throws IOException  if there is an error reading from the Reader
   */
  private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
    StringBuffer command = null;
    try {
      final LineNumberReader lineReader = new LineNumberReader(reader);
      String line = null;
      while ((line = lineReader.readLine()) != null) {
        if (command == null) {
          command = new StringBuffer();
        }
        if (line.startsWith("--")) {
          continue;
        }
        command.append(line).append(" ");
        if (line.endsWith(";")) {
          final String sql = sanitize(command.toString());

          conn.prepareStatement(sql).executeUpdate();
          this.logWriter.println(sql);
          command = null;
        }
      }
      if (!autoCommit) {
        conn.commit();
      }
    } catch (SQLException e) {
      // TODO spyne Ignores errors and keeps progressing. Could be dangerous.
      e.fillInStackTrace();
      printlnError("Error executing: " + command);
      printlnError(e);
    } catch (IOException e) {
      e.fillInStackTrace();
      printlnError("Error executing: " + command);
      printlnError(e);
      throw e;
    } finally {
      conn.rollback();
      flush();
    }
  }

  /**
   * TODO spyne fix mega hack. TE unit tests fail when parsing ENGINE=InnoDB in create schema
   *
   * The create-schema script is used to create schema in the testing env which uses H2. However,
   * H2 db doesn't have an InnoDB engine and raises error.
   * Fix: When connecting to H2, remove InnoDB engine config from SQL
   *
   *
   * @param sql SQL statement to be executed.
   * @return sanitized sql statement
   * @throws SQLException if it fails to get metadata
   */
  private String sanitize(final String sql) throws SQLException {
    final String url = connection.getMetaData().getURL();
    if (url != null && url.startsWith("jdbc:h2:")) {
      return sql.replaceAll("ENGINE[ ]*=[ ]*InnoDB", "");
    }
    return sql;
  }

  private String getDelimiter() {
    return delimiter;
  }

  private void print(Object o) {
    if (logWriter != null) {
      logWriter.print(o);
    }
  }

  private void println(Object o) {
    if (logWriter != null) {
      logWriter.println(o);
    }
  }

  private void printlnError(Object o) {
    if (errorLogWriter != null) {
      errorLogWriter.println(o);
    }
  }

  private void flush() {
    if (logWriter != null) {
      logWriter.flush();
    }
    if (errorLogWriter != null) {
      errorLogWriter.flush();
    }
  }
}
