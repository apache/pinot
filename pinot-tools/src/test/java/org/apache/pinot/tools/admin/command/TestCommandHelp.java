package org.apache.pinot.tools.admin.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TestCommandHelp {
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
