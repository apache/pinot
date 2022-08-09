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
package org.apache.pinot.common.utils;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LoggerUtilsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggerUtilsTest.class);

  @Test
  public void testGetAllLoggers() {
    List<String> allLoggers = LoggerUtils.getAllLoggers();
    assertEquals(allLoggers.size(), 1);
    assertEquals(allLoggers.get(0), "root");
  }

  @Test
  public void testGetLoggerInfo() {
    Map<String, String> rootLoggerInfo = LoggerUtils.getLoggerInfo("root");
    assertEquals(rootLoggerInfo.get("name"), "root");
    assertEquals(rootLoggerInfo.get("level"), "WARN");
    assertNull(rootLoggerInfo.get("filter"));

    assertNull(LoggerUtils.getLoggerInfo("notExistLogger"));
  }

  @Test
  public void testChangeLoggerLevel() {
    assertEquals(LoggerUtils.getLoggerInfo("root").get("level"), "WARN");
    for (String level : ImmutableList.of("WARN", "INFO", "DEBUG", "ERROR", "WARN")) {
      LoggerUtils.setLoggerLevel("root", level);
      checkLogLevel(level);
      assertEquals(LoggerUtils.getLoggerInfo("root").get("level"), level);
    }
  }

  @Test
  public void testChangeLoggerLevelWithExceptions() {
    try {
      LoggerUtils.setLoggerLevel("notExistLogger", "INFO");
      fail("Shouldn't reach here");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(), "Logger - notExistLogger not found");
    }
    try {
      LoggerUtils.setLoggerLevel("root", "NotALevel");
      fail("Shouldn't reach here");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(), "Unrecognized logger level - NotALevel");
    }
  }

  private void checkLogLevel(String level) {
    switch (level) {
      case "ERROR":
        assertTrue(LOGGER.isErrorEnabled());
        assertFalse(LOGGER.isWarnEnabled());
        assertFalse(LOGGER.isInfoEnabled());
        assertFalse(LOGGER.isDebugEnabled());
        assertFalse(LOGGER.isTraceEnabled());
        break;
      case "WARN":
        assertTrue(LOGGER.isErrorEnabled());
        assertTrue(LOGGER.isWarnEnabled());
        assertFalse(LOGGER.isInfoEnabled());
        assertFalse(LOGGER.isDebugEnabled());
        assertFalse(LOGGER.isTraceEnabled());
        break;
      case "INFO":
        assertTrue(LOGGER.isErrorEnabled());
        assertTrue(LOGGER.isWarnEnabled());
        assertTrue(LOGGER.isInfoEnabled());
        assertFalse(LOGGER.isDebugEnabled());
        assertFalse(LOGGER.isTraceEnabled());
        break;
      case "DEBUG":
        assertTrue(LOGGER.isErrorEnabled());
        assertTrue(LOGGER.isWarnEnabled());
        assertTrue(LOGGER.isInfoEnabled());
        assertTrue(LOGGER.isDebugEnabled());
        assertFalse(LOGGER.isTraceEnabled());
        break;
      case "TRACE":
        assertTrue(LOGGER.isErrorEnabled());
        assertTrue(LOGGER.isWarnEnabled());
        assertTrue(LOGGER.isInfoEnabled());
        assertTrue(LOGGER.isDebugEnabled());
        assertTrue(LOGGER.isTraceEnabled());
        break;
      default:
        break;
    }
  }
}
