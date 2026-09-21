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
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PostQueryCommandTest {
  private static final String JSON_RESPONSE_WITH_RESULT_TABLE =
      "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"col1\",\"col2\"],"
          + "\"columnDataTypes\":[\"STRING\",\"LONG\"]},\"rows\":[[\"a\",1],[\"b,c\",2]]},"
          + "\"exceptions\":[],\"numRowsResultSet\":2}";

  private static final String JSON_RESPONSE_WITHOUT_RESULT_TABLE =
      "{\"exceptions\":[{\"errorCode\":150,\"message\":\"QueryExecutionError\"}]}";

  private static final String JSON_RESPONSE_WITH_RESULT_TABLE_AND_EXCEPTIONS =
      "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"col1\"],\"columnDataTypes\":[\"STRING\"]},"
          + "\"rows\":[[\"a\"]]},\"exceptions\":[{\"errorCode\":200,\"message\":\"partial failure\"}]}";

  private static final String NOT_JSON_RESPONSE = "<html><body>502 Bad Gateway</body></html>";

  private File _tempOutputFile;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempOutputFile = File.createTempFile("post-query-command-test-", ".out");
  }

  @AfterMethod
  public void tearDown() {
    if (_tempOutputFile != null) {
      _tempOutputFile.delete();
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
  public void testExecuteWithoutOutputFileDoesNotWriteAnyFile()
      throws Exception {
    // Regression guard: execute() must not write to any file when -outputFile was never set.
    // Snapshot the temp dir's file list before/after rather than checking one hardcoded path,
    // so the assertion catches a write to any unexpected path, not just one guess.
    File tempDir = _tempOutputFile.getParentFile();
    Set<String> filesBefore = Arrays.stream(tempDir.list()).collect(Collectors.toSet());
    PostQueryCommand command = new PostQueryCommand() {
      @Override
      public String run()
          throws Exception {
        return "col1,col2\r\na,1\r\n";
      }
    };
    command.execute();
    Set<String> filesAfter = Arrays.stream(tempDir.list()).collect(Collectors.toSet());
    assertEquals(filesAfter, filesBefore);
  }
}
