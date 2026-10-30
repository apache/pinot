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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TestCommandHelp {
  /// A help-only invocation has never started services, but its registered shutdown hook still
  /// calls cleanup. Exercise that lifecycle before JVM shutdown and ensure help remains usable.
  @Test
  public void testHelpOnlyCommandsCanBeCleanedUpAndReused() {
    List<AbstractBaseAdminCommand> commands = List.of(
        new StartServiceManagerCommand(), new MoveReplicaGroup(), new OfflineSegmentIntervalCheckerCommand());
    for (AbstractBaseAdminCommand command : commands) {
      StringWriter output = new StringWriter();
      CommandLine commandLine = new CommandLine(command).setOut(new PrintWriter(output, true));
      try {
        assertEquals(commandLine.execute("--help"), 0);
        String originalHelp = output.toString();
        assertTrue(originalHelp.contains("Usage: " + commandLine.getCommandName()));
        command.cleanup();
        command.cleanup();
        output.getBuffer().setLength(0);
        assertEquals(commandLine.execute("--help"), 0);
        assertEquals(output.toString(), originalHelp,
            "Cleanup of an unstarted command must leave its help/configuration intact");
      } finally {
        command.cleanup();
      }
    }
  }

  @DataProvider(name = "subCommands")
  Object[][] subCommands() {
    PinotAdministrator administrator = new PinotAdministrator();
    List<Object[]> inputs = new ArrayList<>();
    for (Map.Entry<String, Command> subCommand : administrator.getSubCommands().entrySet()) {
      inputs.add(new Object[]{subCommand.getKey()});
    }

    return inputs.toArray(new Object[0][]);
  }

  @Test(dataProvider = "subCommands")
  void testHelp(String subCommand) {
    PinotAdministrator administrator = new PinotAdministrator();
    String[] args = {subCommand, "--help"};
    administrator.execute(args);
    assertEquals(administrator.getStatus(), 0);
  }
}
