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
package org.apache.pinot.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;


@CommandLine.Command(name = "pinot-cli", mixinStandardHelpOptions = true, version = "1.0",
    description = "Interactive and batch CLI for Apache Pinot")
public class PinotCli implements Callable<Integer> {

  @CommandLine.Option(names = {"-u", "--url"}, required = true,
      description = "JDBC URL. e.g. jdbc:pinot://controller:9000 or jdbc:pinotgrpc://controller:9000")
  private String _jdbcUrl;

  @CommandLine.Option(names = {"-n", "--user"}, description = "Username")
  private String _user;

  @CommandLine.Option(names = {"-p", "--password"}, description = "Password")
  private String _password;

  @CommandLine.Option(names = {"--header"}, description = "Extra header key=value (repeatable)")
  private final Map<String, String> _headers = new LinkedHashMap<>();

  @CommandLine.Option(names = {"-e", "--execute"}, description = "Execute SQL and exit")
  private String _execute;

  @CommandLine.Option(names = {"-f", "--file"}, description = "Execute SQL from file and exit")
  private String _file;

  @CommandLine.Option(names = {"-o", "--output"}, description = "Output format: table|csv|json (default: table)")
  private String _output = "table";

  @CommandLine.Option(names = {"--output-format"},
      description = "Batch output format: "
          + "CSV|CSV_HEADER|CSV_UNQUOTED|CSV_HEADER_UNQUOTED|"
          + "TSV|TSV_HEADER|JSON|ALIGNED|VERTICAL|AUTO|MARKDOWN|NULL")
  private String _outputFormat;

  @CommandLine.Option(names = {"--output-format-interactive"},
      description = "Interactive output format: "
          + "ALIGNED|VERTICAL|AUTO|MARKDOWN|CSV|CSV_HEADER|CSV_UNQUOTED|"
          + "CSV_HEADER_UNQUOTED|TSV|TSV_HEADER|JSON|NULL (default: ALIGNED)")
  private String _outputFormatInteractive = "ALIGNED";

  @CommandLine.Option(names = {"--pager"},
      description = "Pager program for interactive results (empty to disable). Example: less -SRFXK")
  private String _pager;

  @CommandLine.Option(names = {"--history-file"},
      description = "Path to history file for interactive mode")
  private File _historyFile;

  @CommandLine.Option(names = {"--config"},
      description = "Path to config properties file to set defaults")
  private File _configFile;

  @CommandLine.Option(names = {"--debug"}, description = "Enable debug output and stack traces")
  private boolean _debug = false;

  @CommandLine.Option(names = {"--set"}, description = "Query option key=value (repeatable)")
  private final Map<String, String> _options = new LinkedHashMap<>();

  // Client-side extras
  private String _path; // displayed in prompt
  private final Set<String> _roles = new HashSet<>();
  private OutputFormat _overrideFormat; // set when using \G

  @Override
  public Integer call()
      throws Exception {
    loadConfigDefaults();
    Properties props = new Properties();
    if (_user != null) {
      props.setProperty("user", _user);
    }
    if (_password != null) {
      props.setProperty("password", _password);
    }
    // headers.Authorization or headers.X-... supported by PinotDriver
    for (Map.Entry<String, String> e : _headers.entrySet()) {
      props.setProperty("headers." + e.getKey(), e.getValue());
    }
    // query options are passed as properties; PinotConnection will convert to SET statements
    for (Map.Entry<String, String> e : _options.entrySet()) {
      props.setProperty(e.getKey(), e.getValue());
    }

    try (Connection conn = DriverManager.getConnection(_jdbcUrl, props)) {
      if (_execute != null) {
        runSingle(conn, _execute);
        return 0;
      }
      if (_file != null) {
        runFile(conn, _file);
        return 0;
      }
      runInteractive(conn);
    }
    return 0;
  }

  private void runInteractive(Connection conn)
      throws IOException {
    Terminal terminal = TerminalBuilder.builder().system(true).build();
    LineReaderBuilder readerBuilder = LineReaderBuilder.builder().terminal(terminal);
    if (_historyFile == null) {
      File home = new File(System.getProperty("user.home"));
      _historyFile = new File(home, ".pinot_history");
    }
    readerBuilder.variable(LineReader.HISTORY_FILE, _historyFile.toPath());
    LineReader reader = readerBuilder.build();
    String basePrompt = "pinot";
    String contPrompt = "....> ";
    StringBuilder sqlBuffer = new StringBuilder();
    while (true) {
      try {
        String prompt = basePrompt;
        if (_path != null && !_path.isEmpty()) {
          prompt += ":" + _path;
        }
        prompt += "> ";
        String line = reader.readLine(sqlBuffer.length() == 0 ? prompt : contPrompt);
        if (line == null) {
          break;
        }
        String trimmed = line.trim();
        if (sqlBuffer.length() == 0) {
          if (trimmed.isEmpty()) {
            continue;
          }
          if ("exit".equalsIgnoreCase(trimmed) || "quit".equalsIgnoreCase(trimmed)) {
            break;
          }
          if ("help".equalsIgnoreCase(trimmed)) {
            printHelp();
            continue;
          }
          if ("clear".equalsIgnoreCase(trimmed)) {
            // ANSI clear screen
            terminal.writer().print("\u001b[H\u001b[2J");
            terminal.flush();
            continue;
          }
          if (trimmed.toUpperCase().startsWith("SET ")) {
            handleSet(trimmed.substring(4));
            continue;
          }
          if (trimmed.toUpperCase().startsWith("UNSET ")) {
            handleUnset(trimmed.substring(6));
            continue;
          }
          if (trimmed.equalsIgnoreCase("SHOW SESSION")) {
            showSession();
            continue;
          }
          if (trimmed.toUpperCase().startsWith("USE ")) {
            _path = trimmed.substring(4).trim();
            continue;
          }
          if (trimmed.toUpperCase().startsWith("SET PATH ")) {
            _path = trimmed.substring("SET PATH ".length()).trim();
            continue;
          }
          if (trimmed.toUpperCase().startsWith("SET ROLE ")) {
            String role = trimmed.substring("SET ROLE ".length()).trim();
            if (!role.isEmpty()) {
              _roles.add(role);
              System.out.println("Role set: " + role + " (client-side only)");
            }
            continue;
          }
          if (trimmed.equalsIgnoreCase("RESET ROLE")) {
            _roles.clear();
            System.out.println("Roles cleared (client-side only)");
            continue;
          }
        }

        // Accumulate SQL; submit when terminated with ';' or '\\G'
        sqlBuffer.append(line).append('\n');
        Terminator t = detectTerminator(sqlBuffer);
        if (t._completed) {
          String sql = stripTrailingTerminator(sqlBuffer.toString(), t);
          sqlBuffer.setLength(0);
          _overrideFormat = t._vertical ? OutputFormat.VERTICAL : null;
          if (!sql.trim().isEmpty()) {
            executeAndRender(conn, sql);
          }
        }
      } catch (UserInterruptException e) {
        // Ctrl-C: skip current line
        sqlBuffer.setLength(0);
      } catch (EndOfFileException e) {
        break;
      } catch (Exception e) {
        System.err.println("Error: " + e.getMessage());
        if (_debug) {
          e.printStackTrace(System.err);
        }
      }
    }
  }

  private void runFile(Connection conn, String file)
      throws IOException, SQLException {
    try (BufferedReader br = Files.newBufferedReader(Paths.get(file), StandardCharsets.UTF_8)) {
      String sql;
      StringBuilder buf = new StringBuilder();
      while ((sql = br.readLine()) != null) {
        buf.append(sql).append('\n');
      }
      executeAndRender(conn, buf.toString());
    }
  }

  private void runSingle(Connection conn, String sql)
      throws SQLException {
    executeAndRender(conn, sql);
  }

  private void executeAndRender(Connection conn, String sql)
      throws SQLException {
    String composed = prefixSessionOptions(sql);
    Instant start = Instant.now();
    Progress progress = new Progress();
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<?> spinner = scheduler.scheduleAtFixedRate(() -> progress.tick(), 0, 120, TimeUnit.MILLISECONDS);
    try (Statement stmt = conn.createStatement()) {
      boolean hasResult = stmt.execute(composed);
      if (!hasResult) {
        System.out.println("OK");
        printSqlWarnings(stmt.getWarnings());
        return;
      }
      try (ResultSet rs = stmt.getResultSet()) {
        OutputFormat format = _overrideFormat != null ? _overrideFormat : resolveOutputFormat();
        boolean usePager = shouldUsePager();
        Appendable out;
        StringBuilder buffer = null;
        if (usePager) {
          buffer = new StringBuilder();
          out = buffer;
        } else {
          out = System.out;
        }
        int rows = render(rs, format, out, getTerminalWidthIfInteractive());
        printSqlWarnings(rs.getWarnings());
        if (usePager && buffer != null) {
          page(buffer.toString());
        }
        if (_debug) {
          Instant end = Instant.now();
          Duration d = Duration.between(start, end);
          System.err.println("[debug] rows=" + rows + ", elapsed=" + d.toMillis() + " ms");
        }
      }
    } finally {
      spinner.cancel(true);
      scheduler.shutdownNow();
      progress.clear();
      _overrideFormat = null;
    }
  }

  private int render(ResultSet rs, OutputFormat format, Appendable out, int terminalWidth)
      throws SQLException {
    if (format == OutputFormat.NULL) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      return count;
    }
    if (format == OutputFormat.JSON) {
      return renderJsonLines(rs, out);
    }
    if (format == OutputFormat.VERTICAL) {
      return renderVertical(rs, out);
    }
    if (format == OutputFormat.MARKDOWN) {
      return renderMarkdown(rs, out);
    }
    if (format == OutputFormat.CSV
        || format == OutputFormat.CSV_HEADER
        || format == OutputFormat.CSV_UNQUOTED
        || format == OutputFormat.CSV_HEADER_UNQUOTED) {
      boolean includeHeader = (format == OutputFormat.CSV_HEADER || format == OutputFormat.CSV_HEADER_UNQUOTED);
      boolean quoted = (format == OutputFormat.CSV || format == OutputFormat.CSV_HEADER);
      return renderSeparated(rs, out, ',', includeHeader, quoted);
    }
    if (format == OutputFormat.TSV || format == OutputFormat.TSV_HEADER) {
      boolean includeHeader = (format == OutputFormat.TSV_HEADER);
      // TSV is unquoted
      return renderSeparated(rs, out, '\t', includeHeader, false);
    }
    // ALIGNED or AUTO
    if (format == OutputFormat.AUTO) {
      // Decide based on width
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      int[] widths = new int[cols];
      String[] headers = new String[cols];
      for (int i = 1; i <= cols; i++) {
        headers[i - 1] = md.getColumnLabel(i);
        widths[i - 1] = headers[i - 1].length();
      }
      List<String[]> rows = new ArrayList<>();
      while (rs.next()) {
        String[] row = new String[cols];
        for (int i = 1; i <= cols; i++) {
          String v = rs.getString(i);
          row[i - 1] = v == null ? "NULL" : v;
          widths[i - 1] = Math.max(widths[i - 1], row[i - 1].length());
        }
        rows.add(row);
      }
      int requiredWidth = 0;
      for (int i = 0; i < cols; i++) {
        if (i > 0) {
          requiredWidth += 3; // separator " | "
        }
        requiredWidth += widths[i];
      }
      if (terminalWidth > 0 && requiredWidth > terminalWidth) {
        return renderVertical(headers, rows, out);
      } else {
        return renderAligned(headers, widths, rows, out);
      }
    }
    // ALIGNED
    return renderAlignedFromResultSet(rs, out);
  }

  private String prefixSessionOptions(String sql) {
    if (_options.isEmpty()) {
      return sql;
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : _options.entrySet()) {
      sb.append("SET ").append(e.getKey()).append("=").append(e.getValue()).append(";\n");
    }
    sb.append(sql);
    return sb.toString();
  }

  private int renderAlignedFromResultSet(ResultSet rs, Appendable out)
      throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    int[] widths = new int[cols];
    String[] headers = new String[cols];
    for (int i = 1; i <= cols; i++) {
      headers[i - 1] = md.getColumnLabel(i);
      widths[i - 1] = headers[i - 1].length();
    }
    List<String[]> rows = new ArrayList<>();
    while (rs.next()) {
      String[] row = new String[cols];
      for (int i = 1; i <= cols; i++) {
        String v = rs.getString(i);
        row[i - 1] = v == null ? "NULL" : v;
        widths[i - 1] = Math.max(widths[i - 1], row[i - 1].length());
      }
      rows.add(row);
    }
    return renderAligned(headers, widths, rows, out);
  }

  private int renderAligned(String[] headers, int[] widths, List<String[]> rows, Appendable out)
      throws SQLException {
    int cols = headers.length;
    // header
    StringBuilder sep = new StringBuilder();
    StringBuilder head = new StringBuilder();
    for (int i = 0; i < cols; i++) {
      if (i > 0) {
        sep.append("-+-");
        head.append(" | ");
      }
      sep.append(repeat('-', widths[i]));
      head.append(pad(headers[i], widths[i]));
    }
    tryAppendLine(out, sep.toString());
    tryAppendLine(out, head.toString());
    tryAppendLine(out, sep.toString());
    for (String[] row : rows) {
      StringBuilder line = new StringBuilder();
      for (int i = 0; i < cols; i++) {
        if (i > 0) {
          line.append(" | ");
        }
        line.append(pad(row[i], widths[i]));
      }
      tryAppendLine(out, line.toString());
    }
    tryAppendLine(out, sep.toString());
    tryAppendLine(out, rows.size() + " row(s)");
    return rows.size();
  }

  private int renderSeparated(ResultSet rs,
      Appendable out,
      char delimiter,
      boolean includeHeader,
      boolean quoted)
      throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    if (includeHeader) {
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= cols; i++) {
        if (i > 1) {
          header.append(delimiter);
        }
        String h = md.getColumnLabel(i);
        header.append(quoted ? escapeCsv(h) : escapeSeparated(h, delimiter));
      }
      tryAppendLine(out, header.toString());
    }
    int count = 0;
    while (rs.next()) {
      StringBuilder line = new StringBuilder();
      for (int i = 1; i <= cols; i++) {
        if (i > 1) {
          line.append(delimiter);
        }
        String v = rs.getString(i);
        if (quoted) {
          line.append(escapeCsv(v));
        } else {
          line.append(escapeSeparated(v, delimiter));
        }
      }
      tryAppendLine(out, line.toString());
      count++;
    }
    return count;
  }

  private int renderJsonLines(ResultSet rs, Appendable out)
      throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    int count = 0;
    while (rs.next()) {
      StringBuilder obj = new StringBuilder();
      obj.append("{");
      for (int i = 1; i <= cols; i++) {
        if (i > 1) {
          obj.append(",");
        }
        String name = md.getColumnLabel(i);
        String v = rs.getString(i);
        obj.append("\"" + escapeJson(name) + "\":");
        if (v == null) {
          obj.append("null");
        } else {
          obj.append("\"" + escapeJson(v) + "\"");
        }
      }
      obj.append("}");
      tryAppendLine(out, obj.toString());
      count++;
    }
    return count;
  }

  private int renderVertical(ResultSet rs, Appendable out)
      throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    int count = 0;
    while (rs.next()) {
      tryAppendLine(out, "-[ RECORD " + (count + 1) + " ]--------");
      for (int i = 1; i <= cols; i++) {
        String name = md.getColumnLabel(i);
        String v = rs.getString(i);
        tryAppendLine(out, name + " | " + (v == null ? "NULL" : v));
      }
      count++;
    }
    return count;
  }

  private int renderMarkdown(ResultSet rs, Appendable out)
      throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    StringBuilder header = new StringBuilder();
    StringBuilder sep = new StringBuilder();
    for (int i = 1; i <= cols; i++) {
      if (i > 1) {
        header.append(" | ");
        sep.append("|");
      }
      String h = md.getColumnLabel(i);
      header.append(h);
      sep.append(" --- ");
    }
    tryAppendLine(out, header.toString());
    tryAppendLine(out, sep.toString());
    int count = 0;
    while (rs.next()) {
      StringBuilder line = new StringBuilder();
      for (int i = 1; i <= cols; i++) {
        if (i > 1) {
          line.append(" | ");
        }
        String v = rs.getString(i);
        line.append(v == null ? "" : v);
      }
      tryAppendLine(out, line.toString());
      count++;
    }
    return count;
  }

  private int renderVertical(String[] headers, List<String[]> rows, Appendable out)
      throws SQLException {
    int count = 0;
    for (String[] row : rows) {
      tryAppendLine(out, "-[ RECORD " + (count + 1) + " ]--------");
      for (int i = 0; i < headers.length; i++) {
        tryAppendLine(out, headers[i] + " | " + (row[i] == null ? "NULL" : row[i]));
      }
      count++;
    }
    return count;
  }

  private void tryAppendLine(Appendable out, String line)
      throws SQLException {
    try {
      out.append(line).append('\n');
    } catch (IOException ioe) {
      throw new SQLException("Failed to write output", ioe);
    }
  }

  private String escapeSeparated(String s, char delimiter) {
    if (s == null) {
      return "";
    }
    // Normalize CRLF/CR to LF for consistency
    String value = s.replace("\r\n", "\n").replace('\r', '\n');
    boolean needsQuoting = false;
    StringBuilder sb = null;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == delimiter || c == '"' || c == '\n') {
        needsQuoting = true;
        if (sb == null) {
          sb = new StringBuilder(value.length() + 2);
          sb.append(value, 0, i);
        }
        if (c == '"') {
          sb.append("\"\"");
        } else {
          sb.append(c);
        }
      } else if (sb != null) {
        sb.append(c);
      }
    }
    if (!needsQuoting) {
      return value;
    }
    if (sb == null) {
      sb = new StringBuilder(value);
    }
    return '"' + sb.toString() + '"';
  }

  private static String pad(String s, int width) {
    if (s == null) {
      s = "";
    }
    if (s.length() >= width) {
      return s;
    }
    StringBuilder b = new StringBuilder(s);
    while (b.length() < width) {
      b.append(' ');
    }
    return b.toString();
  }

  private static String repeat(char ch, int count) {
    return String.valueOf(ch).repeat(count);
  }

  private static String escapeCsv(String s) {
    if (s == null) {
      return "";
    }
    boolean needQuotes = s.contains(",") || s.contains("\n") || s.contains("\r") || s.contains("\"");
    String escaped = s.replace("\"", "\"\"");
    return needQuotes ? "\"" + escaped + "\"" : escaped;
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "null"; // Should not be called with nulls normally
    }
    StringBuilder b = new StringBuilder(s.length() + 8);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          b.append("\\\"");
          break;
        case '\\':
          b.append("\\\\");
          break;
        case '\n':
          b.append("\\n");
          break;
        case '\r':
          b.append("\\r");
          break;
        case '\t':
          b.append("\\t");
          break;
        case '\b':
          b.append("\\b");
          break;
        case '\f':
          b.append("\\f");
          break;
        default:
          if (c <= 0x1F) {
            String hex = Integer.toHexString(c);
            b.append("\\u");
            for (int k = hex.length(); k < 4; k++) {
              b.append('0');
            }
            b.append(hex);
          } else {
            b.append(c);
          }
          break;
      }
    }
    return b.toString();
  }

  // Minimal tokenizer that splits a command string into argv respecting simple quotes
  private static String[] tokenizeCommand(String command) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inSingle = false;
    boolean inDouble = false;
    for (int i = 0; i < command.length(); i++) {
      char c = command.charAt(i);
      if (c == '\'' && !inDouble) {
        inSingle = !inSingle;
        continue;
      }
      if (c == '"' && !inSingle) {
        inDouble = !inDouble;
        continue;
      }
      if (Character.isWhitespace(c) && !inSingle && !inDouble) {
        if (current.length() > 0) {
          parts.add(current.toString());
          current.setLength(0);
        }
      } else {
        current.append(c);
      }
    }
    if (current.length() > 0) {
      parts.add(current.toString());
    }
    return parts.toArray(new String[0]);
  }

  public static void main(String[] args) {
    int code = new CommandLine(new PinotCli()).execute(args);
    System.exit(code);
  }

  private OutputFormat resolveOutputFormat() {
    // Backward compatibility for -o/--output
    if (_outputFormat == null) {
      if (_execute != null || _file != null) {
        // batch default
        if ("json".equalsIgnoreCase(_output)) {
          return OutputFormat.JSON;
        } else if ("csv".equalsIgnoreCase(_output)) {
          return OutputFormat.CSV;
        } else {
          return OutputFormat.ALIGNED;
        }
      } else {
        // interactive default
        return parseFormat(_outputFormatInteractive);
      }
    }
    return parseFormat(_outputFormat);
  }

  private OutputFormat parseFormat(String fmt) {
    if (fmt == null) {
      return OutputFormat.ALIGNED;
    }
    String f = fmt.trim().toUpperCase();
    try {
      if ("TABLE".equals(f)) {
        return OutputFormat.ALIGNED;
      }
      return OutputFormat.valueOf(f);
    } catch (IllegalArgumentException iae) {
      return OutputFormat.ALIGNED;
    }
  }

  private boolean shouldUsePager() {
    if (_execute != null || _file != null) {
      return false; // batch mode: no pager
    }
    String pager = effectivePager();
    return pager != null && !pager.isEmpty();
  }

  private String effectivePager() {
    if (_pager != null) {
      return _pager;
    }
    return System.getenv("PINOT_PAGER");
  }

  private void page(String content)
      throws SQLException {
    String pager = effectivePager();
    if (pager == null || pager.isEmpty()) {
      System.out.print(content);
      return;
    }
    // Reject shell metacharacters and avoid invoking a shell to prevent command injection
    if (pager.matches(".*[|&;`$><\\\\].*")) {
      throw new SQLException("Invalid pager command: shell metacharacters are not allowed");
    }
    String[] pagerCmd = tokenizeCommand(pager);
    if (pagerCmd.length == 0) {
      System.out.print(content);
      return;
    }
    ProcessBuilder pb = new ProcessBuilder(pagerCmd);
    try {
      Process p = pb.start();
      try (OutputStream os = p.getOutputStream()) {
        os.write(content.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }
      p.waitFor();
    } catch (Exception e) {
      throw new SQLException("Failed to run pager: " + pager, e);
    }
  }

  private int getTerminalWidthIfInteractive() {
    if (_execute != null || _file != null) {
      return -1;
    }
    try {
      Terminal t = TerminalBuilder.builder().system(true).build();
      return t.getWidth();
    } catch (IOException e) {
      return -1;
    }
  }

  private void printHelp() {
    System.out.println("Supported commands:");
    System.out.println("HELP");
    System.out.println("CLEAR");
    System.out.println("EXIT | QUIT");
    System.out.println("SHOW SESSION");
    System.out.println("SET key=value");
    System.out.println("UNSET key");
    System.out.println("USE <schema> | SET PATH <schema>");
    System.out.println("SET ROLE <role> | RESET ROLE (client-side only)");
    System.out.println("Execute SQL statements directly.");
    System.out.println("Multi-line SQL is supported; terminate statements with ';' or \\G for vertical output.");
  }

  private Terminator detectTerminator(StringBuilder buf) {
    int i = buf.length() - 1;
    while (i >= 0 && Character.isWhitespace(buf.charAt(i))) {
      i--;
    }
    if (i < 0) {
      return new Terminator(false, false);
    }
    if (buf.charAt(i) == ';') {
      return new Terminator(true, false);
    }
    if (i >= 1 && buf.charAt(i) == 'G' && buf.charAt(i - 1) == '\\') {
      return new Terminator(true, true);
    }
    return new Terminator(false, false);
  }

  private String stripTrailingTerminator(String sql, Terminator t) {
    int end = sql.length();
    while (end > 0 && Character.isWhitespace(sql.charAt(end - 1))) {
      end--;
    }
    if (t._vertical) {
      if (end >= 2 && sql.charAt(end - 1) == 'G' && sql.charAt(end - 2) == '\\') {
        end -= 2;
      }
    } else {
      if (end > 0 && sql.charAt(end - 1) == ';') {
        end--;
      }
    }
    return sql.substring(0, end);
  }

  private void loadConfigDefaults()
      throws IOException {
    if (_configFile == null) {
      String env = System.getenv("PINOT_CONFIG");
      if (env != null && !env.isEmpty()) {
        _configFile = new File(env);
      }
    }
    if (_configFile == null) {
      return;
    }
    java.util.Properties p = new java.util.Properties();
    try (java.io.InputStream in = new java.io.FileInputStream(_configFile)) {
      p.load(in);
    }
    // Map supported options from properties to fields if not set on CLI
    if (_jdbcUrl == null && p.getProperty("server") != null) {
      _jdbcUrl = p.getProperty("server");
    }
    if (_user == null && p.getProperty("user") != null) {
      _user = p.getProperty("user");
    }
    if (_password == null && p.getProperty("password") != null) {
      _password = p.getProperty("password");
    }
    if (_outputFormat == null && p.getProperty("output-format") != null) {
      _outputFormat = p.getProperty("output-format");
    }
    if ("table".equalsIgnoreCase(_output) && p.getProperty("output") != null) {
      _output = p.getProperty("output");
    }
    if ("ALIGNED".equalsIgnoreCase(_outputFormatInteractive)
        && p.getProperty("output-format-interactive") != null) {
      _outputFormatInteractive = p.getProperty("output-format-interactive");
    }
    if (_pager == null && p.getProperty("pager") != null) {
      _pager = p.getProperty("pager");
    }
    if (_historyFile == null && p.getProperty("history-file") != null) {
      _historyFile = new File(p.getProperty("history-file"));
    }
    if (!_debug && p.getProperty("debug") != null) {
      _debug = Boolean.parseBoolean(p.getProperty("debug"));
    }
    // headers.* and arbitrary options
    for (String name : p.stringPropertyNames()) {
      if (name.startsWith("headers.")) {
        String key = name.substring("headers.".length());
        if (!_headers.containsKey(key)) {
          _headers.put(key, p.getProperty(name));
        }
      }
    }
    for (String name : p.stringPropertyNames()) {
      if (name.startsWith("headers.")) {
        continue;
      }
      // Exclude non-session configuration keys from being forwarded as query options
      if (name.equals("server")
          || name.equals("user")
          || name.equals("password")
          || name.equals("output")
          || name.equals("output-format")
          || name.equals("output-format-interactive")
          || name.equals("pager")
          || name.equals("history-file")
          || name.equals("debug")) {
        continue;
      }
      // don't override explicit --set
      if (!_options.containsKey(name)) {
        _options.put(name, p.getProperty(name));
      }
    }
  }

  private void handleSet(String expr) {
    int idx = expr.indexOf('=');
    if (idx <= 0) {
      System.err.println("SET requires key=value");
      return;
    }
    String key = expr.substring(0, idx).trim();
    String value = expr.substring(idx + 1).trim();
    if (key.isEmpty()) {
      System.err.println("Invalid key");
      return;
    }
    _options.put(key, value);
    System.out.println("Set " + key + "=" + value);
  }

  private void handleUnset(String key) {
    key = key.trim();
    if (key.isEmpty()) {
      System.err.println("UNSET requires key");
      return;
    }
    if (_options.remove(key) != null) {
      System.out.println("Unset " + key);
    } else {
      System.out.println("No such key: " + key);
    }
  }

  private void showSession() {
    System.out.println("URL: " + _jdbcUrl);
    System.out.println("User: " + (_user == null ? "" : _user));
    System.out.println("Path: " + (_path == null ? "" : _path));
    System.out.println("Roles: " + (_roles.isEmpty() ? "(none)" : String.join(",", _roles)) + " (client-side)");
    System.out.println("Output (batch): " + (_outputFormat == null ? _output : _outputFormat));
    System.out.println("Output (interactive): " + _outputFormatInteractive);
    if (_pager != null) {
      System.out.println("Pager: " + _pager);
    }
    if (!_options.isEmpty()) {
      System.out.println("Session properties:");
      for (Map.Entry<String, String> e : _options.entrySet()) {
        System.out.println("  " + e.getKey() + "=" + e.getValue());
      }
    }
  }

  private void printSqlWarnings(java.sql.SQLWarning warn) {
    if (warn == null) {
      return;
    }
    System.err.println("Warnings:");
    for (java.sql.SQLWarning w = warn; w != null; w = w.getNextWarning()) {
      System.err.println(
          "  " + w.getMessage() + (w.getSQLState() != null ? (" [SQLState=" + w.getSQLState() + "]") : ""));
    }
  }

  private static final class Terminator {
    final boolean _completed;
    final boolean _vertical;

    Terminator(boolean completed, boolean vertical) {
      _completed = completed;
      _vertical = vertical;
    }
  }

  private static final class Progress {
    private final char[] _spin = new char[]{'|', '/', '-', '\\'};
    private int _idx = 0;
    private final long _start = System.currentTimeMillis();

    synchronized void tick() {
      long elapsed = System.currentTimeMillis() - _start;
      System.err.print("\r[" + _spin[_idx] + "] Executing... " + elapsed + " ms");
      System.err.flush();
      _idx = (_idx + 1) % _spin.length;
    }

    synchronized void clear() {
      System.err.print("\r\033[K");
      System.err.flush();
    }
  }

  private enum OutputFormat {
    CSV,
    CSV_HEADER,
    CSV_UNQUOTED,
    CSV_HEADER_UNQUOTED,
    TSV,
    TSV_HEADER,
    JSON,
    ALIGNED,
    VERTICAL,
    AUTO,
    MARKDOWN,
    NULL
  }
}
