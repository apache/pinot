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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PostQueryCommandTest {
  private static final String JSON_RESPONSE_WITH_RESULT_TABLE =
      "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"col1\",\"col2\"],"
          + "\"columnDataTypes\":[\"STRING\",\"LONG\"]},\"rows\":[[\"a\",1],[\"b,c\",2]]},"
          + "\"exceptions\":[],\"numRowsResultSet\":2}";

  private static final String JSON_RESPONSE_WITH_NULL_AND_LITERAL_NULL_STRING =
      "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"col1\",\"col2\"],"
          + "\"columnDataTypes\":[\"STRING\",\"STRING\"]},\"rows\":[[null,\"null\"]]},"
          + "\"exceptions\":[],\"numRowsResultSet\":1}";

  private static final String JSON_RESPONSE_WITHOUT_RESULT_TABLE =
      "{\"exceptions\":[{\"errorCode\":150,\"message\":\"QueryExecutionError\"}]}";

  private static final String JSON_RESPONSE_WITH_RESULT_TABLE_AND_EXCEPTIONS =
      "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"col1\"],\"columnDataTypes\":[\"STRING\"]},"
          + "\"rows\":[[\"a\"]]},\"exceptions\":[{\"errorCode\":200,\"message\":\"partial failure\"}]}";

  private static final String NOT_JSON_RESPONSE = "<html><body>502 Bad Gateway</body></html>";

  // A dedicated per-test temp directory, not the shared system temp dir. Pinot CI runs unit-test
  // classes in three parallel forks, and other pinot-tools tests also create files under the
  // shared system temp dir; snapshotting that shared directory would make file-count assertions
  // flaky. This directory is created fresh in setUp() and is only ever touched by this test.
  private File _tempDir;
  private File _tempOutputFile;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("post-query-command-test-dir-").toFile();
    _tempOutputFile = File.createTempFile("post-query-command-test-", ".out", _tempDir);
  }

  @AfterMethod
  public void tearDown() {
    if (_tempDir != null) {
      File[] files = _tempDir.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
      _tempDir.delete();
    }
  }

  @Test
  public void testFormatResponseDefaultIsJson() {
    PostQueryCommand command = new PostQueryCommand();
    String formatted = command.formatResponse(JSON_RESPONSE_WITH_RESULT_TABLE);
    assertEquals(formatted, JSON_RESPONSE_WITH_RESULT_TABLE);
  }

  @Test
  public void testFormatResponseCsvRendersResultTable() {
    PostQueryCommand command = new PostQueryCommand();
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    String csv = command.formatResponse(JSON_RESPONSE_WITH_RESULT_TABLE);
    String[] lines = csv.split("\r\n");
    assertEquals(lines.length, 3);
    assertEquals(lines[0], "col1,col2");
    assertEquals(lines[1], "a,1");
    // A comma-containing cell value must be quoted by CSV rendering, not silently break columns.
    assertEquals(lines[2], "\"b,c\",2");
  }

  @Test
  public void testFormatResponseCsvRendersSqlNullAsEmptyFieldDistinctFromLiteralNullString() {
    // A JSON null cell (SQL NULL) must render as an empty CSV field, matching Pinot CLI's
    // existing convention -- distinct from a STRING cell that literally contains "null".
    PostQueryCommand command = new PostQueryCommand();
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    String csv = command.formatResponse(JSON_RESPONSE_WITH_NULL_AND_LITERAL_NULL_STRING);
    String[] lines = csv.split("\r\n");
    assertEquals(lines.length, 2);
    assertEquals(lines[0], "col1,col2");
    assertEquals(lines[1], ",null");
  }

  @Test
  public void testFormatResponseCsvFallsBackToJsonWithoutResultTable() {
    PostQueryCommand command = new PostQueryCommand();
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    String formatted = command.formatResponse(JSON_RESPONSE_WITHOUT_RESULT_TABLE);
    assertEquals(formatted, JSON_RESPONSE_WITHOUT_RESULT_TABLE);
  }

  @Test
  public void testFormatResponseCsvFallsBackToRawResponseWhenNotJson() {
    // e.g. a proxy/broker error page that isn't valid JSON at all -- must not throw.
    PostQueryCommand command = new PostQueryCommand();
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    String formatted = command.formatResponse(NOT_JSON_RESPONSE);
    assertEquals(formatted, NOT_JSON_RESPONSE);
  }

  @Test
  public void testFormatResponseCsvWithResultTableAndExceptionsStillRendersCsv() {
    // A resultTable can coexist with a non-empty 'exceptions' list (partial failure). CSV mode
    // should still render the table (rather than silently discarding it) while separately
    // warning that the CSV doesn't carry the exception detail.
    PostQueryCommand command = new PostQueryCommand();
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    String csv = command.formatResponse(JSON_RESPONSE_WITH_RESULT_TABLE_AND_EXCEPTIONS);
    assertEquals(csv, "col1\r\na\r\n");
  }

  @Test
  public void testCliParsingDefaultOutputFormatIsJson() {
    PostQueryCommand command = new PostQueryCommand();
    new CommandLine(command).parseArgs("-query", "select 1");
    assertEquals(command.formatResponse(JSON_RESPONSE_WITH_RESULT_TABLE), JSON_RESPONSE_WITH_RESULT_TABLE);
  }

  @Test
  public void testCliParsingOutputFormatCsv() {
    PostQueryCommand command = new PostQueryCommand();
    new CommandLine(command).parseArgs("-query", "select 1", "-outputFormat", "CSV");
    String csv = command.formatResponse(JSON_RESPONSE_WITH_RESULT_TABLE);
    assertTrue(csv.startsWith("col1,col2"));
  }

  @Test
  public void testExecuteWritesFormattedResponseToOutputFile()
      throws Exception {
    PostQueryCommand command = new PostQueryCommand() {
      @Override
      public String run()
          throws Exception {
        // Skip the real broker HTTP call; exercise execute()'s output-file writing with a
        // canned CSV-formatted response instead.
        return "col1,col2\r\na,1\r\n";
      }
    };
    command.setOutputFile(_tempOutputFile.getAbsolutePath());
    command.execute();
    String written = new String(Files.readAllBytes(_tempOutputFile.toPath()), StandardCharsets.UTF_8);
    assertEquals(written, "col1,col2\r\na,1\r\n");
  }

  @Test
  public void testExecuteDoesNotWriteOutputFileWhenCsvRequestedButResponseNotRenderableAsCsv()
      throws Exception {
    // execute() must not let a file consumer mistake a raw JSON/error fallback body for a
    // successful CSV export: when -outputFormat CSV was requested but formatResponse() fell back
    // (e.g. broker error response with no resultTable), execute() must report failure and must
    // not write -outputFile at all.
    assertTrue(_tempOutputFile.delete());
    PostQueryCommand command = new PostQueryCommand() {
      @Override
      public String run() {
        // formatResponse() is invoked for real here (not overridden), so the CSV-fallback path
        // and _lastResponseRenderedAsRequested bookkeeping are exercised end-to-end.
        return formatResponse(JSON_RESPONSE_WITHOUT_RESULT_TABLE);
      }
    };
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    command.setOutputFile(_tempOutputFile.getAbsolutePath());
    boolean result = command.execute();
    assertFalse(result);
    assertFalse(_tempOutputFile.exists());
  }

  @Test
  public void testExecuteWritesCsvFormattedResponseToOutputFileOnSuccessfulCsvRendering()
      throws Exception {
    // End-to-end coverage for the success path that the CSV-fallback-failure test above does not
    // exercise: -outputFormat CSV + -outputFile, with a response that CAN be rendered as CSV,
    // must both report success and write the actual CSV-rendered body (not the raw JSON) to disk.
    assertTrue(_tempOutputFile.delete());
    PostQueryCommand command = new PostQueryCommand() {
      @Override
      public String run() {
        // formatResponse() is invoked for real here, so the CSV success path and
        // _lastResponseRenderedAsRequested bookkeeping are exercised end-to-end, matching the
        // fallback-failure test's style above.
        return formatResponse(JSON_RESPONSE_WITH_RESULT_TABLE);
      }
    };
    command.setOutputFormat(PostQueryCommand.OutputFormat.CSV);
    command.setOutputFile(_tempOutputFile.getAbsolutePath());
    boolean result = command.execute();
    assertTrue(result);
    String written = new String(Files.readAllBytes(_tempOutputFile.toPath()), StandardCharsets.UTF_8);
    assertEquals(written, "col1,col2\r\na,1\r\n\"b,c\",2\r\n");
  }

  @Test
  public void testExecuteWithoutOutputFileDoesNotWriteAnyFile()
      throws Exception {
    // Regression guard: execute() must not write to any file when -outputFile was never set.
    // Uses a dedicated per-test temp directory (see setUp()) rather than the shared system temp
    // dir, so a file created/removed by an unrelated parallel test can't make this flaky.
    String[] filesBefore = _tempDir.list();
    PostQueryCommand command = new PostQueryCommand() {
      @Override
      public String run()
          throws Exception {
        return "col1,col2\r\na,1\r\n";
      }
    };
    command.execute();
    String[] filesAfter = _tempDir.list();
    assertEquals(filesAfter, filesBefore);
  }
}
