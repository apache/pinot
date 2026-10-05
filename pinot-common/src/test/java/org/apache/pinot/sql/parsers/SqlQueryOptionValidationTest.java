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
package org.apache.pinot.sql.parsers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlQueryOptionValidationMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Covers the opt-in SQL query option validation modes. The pre-existing default behavior is pinned
/// by [CalciteSqlCompilerTest] and `QueryOptionsUtilsTest`, which are deliberately untouched.
public class SqlQueryOptionValidationTest {

  @AfterMethod
  public void resetValidationMode() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.NONE);
  }

  @Test
  public void defaultModeIsNoneAndPreservesUnknownKeysVerbatim() {
    assertEquals(QueryOptionsUtils.getSqlQueryOptionValidationMode(), SqlQueryOptionValidationMode.NONE);

    Map<String, String> setOptions = optionsOf("SET deliCious='yes'; select * from vegetables");
    assertEquals(setOptions.get("deliCious"), "yes");

    Map<String, String> legacyOptions = optionsOf("select * from vegetables OPTION(deliCious=yes)");
    assertEquals(legacyOptions.get("deliCious"), "yes");
  }

  @Test
  public void defaultModeKeepsLastWinsForDuplicateKeysInDifferentCases() {
    Map<String, String> options = optionsOf("SET timeoutMs='1'; SET TIMEOUTMS='2'; select * from vegetables");
    // Both spellings resolve to the same canonical key; which value wins is unspecified (as on master),
    // but exactly one entry must survive.
    assertEquals(options.size(), 1);
    assertTrue(List.of("1", "2").contains(options.get("timeoutMs")), options.toString());
  }

  @Test
  public void rejectModeFailsUnknownSetOptionWithSuggestion() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    SqlCompilationException e = expectThrows(SqlCompilationException.class,
        () -> optionsOf("SET timoutMs='100'; select * from vegetables"));
    assertTrue(e.getMessage().contains("Unsupported query option 'timoutMs'"), e.getMessage());
    assertTrue(e.getMessage().contains("Did you mean 'timeoutMs'"), e.getMessage());
  }

  @Test
  public void rejectModeFailsUnknownLegacyOption() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    SqlCompilationException e = expectThrows(SqlCompilationException.class,
        () -> optionsOf("select * from vegetables OPTION(delicious=yes)"));
    assertTrue(e.getMessage().contains("Unsupported query option 'delicious'"), e.getMessage());
  }

  @Test
  public void rejectModeOmitsSuggestionForKeysNowhereNearAKnownOption() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    SqlCompilationException e = expectThrows(SqlCompilationException.class,
        () -> optionsOf("SET zzz='1'; select * from vegetables"));
    assertTrue(e.getMessage().contains("Unsupported query option 'zzz'"), e.getMessage());
    assertFalse(e.getMessage().contains("Did you mean"), e.getMessage());
  }

  @Test
  public void rejectModeAcceptsKnownKeysCaseInsensitivelyAndTraceAndDatabase() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    Map<String, String> options =
        optionsOf("SET timeoutMS='100'; SET TRACE='true'; SET Database='db1'; select * from vegetables");
    assertEquals(options.get("timeoutMs"), "100");
    assertEquals(options.get("TRACE"), "true");
    assertEquals(options.get("Database"), "db1");
  }

  @Test
  public void rejectModeLeavesDmlOptionsFreeForm() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    // DML carries free-form task and filesystem properties, via both SET and legacy OPTION.
    Map<String, String> legacyOptions =
        optionsOf("INSERT INTO db.tbl FROM FILE 'file:///tmp/file1' OPTION(taskName=myTask-1)");
    assertEquals(legacyOptions.get("taskName"), "myTask-1");

    Map<String, String> setOptions =
        optionsOf("SET taskName='myTask-1'; INSERT INTO db.tbl FROM FILE 'file:///tmp/file1'");
    assertEquals(setOptions.get("taskName"), "myTask-1");
  }

  @Test
  public void rejectModeAcceptsRegisteredPluginKeys() {
    QueryOptionsUtils.registerSqlQueryOptionKey("myPluginOption");
    // Plugin init can run more than once; registration must be idempotent.
    QueryOptionsUtils.registerSqlQueryOptionKey("myPluginOption");
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    // Registered case-insensitively, and the key case the user typed is preserved as before.
    assertEquals(optionsOf("SET MYPLUGINOPTION='x'; select * from vegetables").get("MYPLUGINOPTION"), "x");
  }

  @Test
  public void rejectModeNeverAcceptsUserSuppliedRlsFilters() {
    // The broker injects rlsFilters* after parsing; registering the key must not open a back door,
    // and the prefix match is case-insensitive so case tricks cannot bypass it either.
    QueryOptionsUtils.registerSqlQueryOptionKey("rlsFiltersMyTable");
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    for (String key : List.of("rlsFiltersMyTable", "RLSFILTERSMyTable")) {
      SqlCompilationException e = expectThrows(SqlCompilationException.class,
          () -> optionsOf("SET " + key + "='col=1'; select * from vegetables"));
      assertTrue(e.getMessage().contains("Unsupported query option '" + key + "'"), e.getMessage());
    }
  }

  @Test
  public void warnModePreservesUnknownKeysAndLogsOncePerDistinctKey() {
    CapturingAppender appender = CapturingAppender.attachTo(QueryOptionsUtils.class.getName());
    try {
      QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.WARN);

      // A misspelled option from a high-QPS client must be logged once, not once per query.
      for (int i = 0; i < 100; i++) {
        assertEquals(optionsOf("SET warnOnceOption='v'; select * from vegetables").get("warnOnceOption"), "v");
      }
      assertEquals(appender.messagesContaining("warnOnceOption").size(), 1);

      // A different unknown key still gets its own line.
      optionsOf("SET anotherWarnOnceOption='v'; select * from vegetables");
      assertEquals(appender.messagesContaining("anotherWarnOnceOption").size(), 1);

      // Known keys are never logged.
      int loggedSoFar = appender.messagesContaining("Unsupported query option").size();
      optionsOf("SET timeoutMs='100'; select * from vegetables");
      assertEquals(appender.messagesContaining("Unsupported query option").size(), loggedSoFar);
    } finally {
      appender.detach();
    }
  }

  @Test
  public void restStyleOptionsStayFreeFormInEveryMode() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.REJECT);

    // REST/JSON queryOptions and broker-injected options never go through the SQL parser; they are
    // merged through resolveCaseInsensitiveOptions, which validates nothing in any mode.
    Map<String, String> resolved = QueryOptionsUtils.resolveCaseInsensitiveOptions(
        Map.of("customFreeForm", "x", "timeoutMS", "5", "rlsFilters-tbl", "col=1"));
    assertEquals(resolved.get("customFreeForm"), "x");
    assertEquals(resolved.get("timeoutMs"), "5");
    assertEquals(resolved.get("rlsFilters-tbl"), "col=1");
  }

  private static Map<String, String> optionsOf(String sql) {
    return CalciteSqlParser.compileToSqlNodeAndOptions(sql).getOptions();
  }

  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> _messages = new CopyOnWriteArrayList<>();
    private final Logger _logger;

    private CapturingAppender(Logger logger) {
      super("SqlQueryOptionValidationCapture", null, null, true, Property.EMPTY_ARRAY);
      _logger = logger;
    }

    static CapturingAppender attachTo(String loggerName) {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      CapturingAppender appender = new CapturingAppender(context.getLogger(loggerName));
      appender.start();
      appender._logger.addAppender(appender);
      return appender;
    }

    void detach() {
      _logger.removeAppender(this);
      stop();
      // The captured logger is never declared in log4j2.xml, so attaching the appender implicitly registered a
      // LoggerConfig for it. Remove it to leave the shared configuration as found — test classes share the JVM in
      // this module, and other tests assert on the set of configured loggers.
      LoggerContext context = _logger.getContext();
      context.getConfiguration().removeLogger(_logger.getName());
      context.updateLoggers();
    }

    List<String> messagesContaining(String substring) {
      return _messages.stream().filter(message -> message.contains(substring)).collect(Collectors.toList());
    }

    @Override
    public void append(LogEvent event) {
      _messages.add(event.getMessage().getFormattedMessage());
    }
  }
}
