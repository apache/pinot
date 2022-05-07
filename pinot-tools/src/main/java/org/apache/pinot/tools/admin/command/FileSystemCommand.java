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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "FileSystem")
public class FileSystemCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemCommand.class.getName());

  private static final String[] SUPPORTED_FS_COMMANDS = new String[]{
      "EXISTS", "IS_DIR", "LAST_MODIFIED", "LENGTH",
      "LIST_FILE", "LIST_FILE_RECURSIVE",
      "CP", "COPY_DIR",
      "COPY_FROM_LOCAL_FILE", "COPY_FROM_LOCAL_DIR", "COPY_TO_LOCAL_FILE",
      "MV", "DELETE"
  };

  @CommandLine.Option(names = {"-scheme"}, required = true, description = "PinotFS scheme name, e.g. "
      + "file/s3/adls/gcs/hdfs.")
  private String _scheme;

  @CommandLine.Option(names = {"-config", "-configFile"}, description =
      "PinotFS Config file.")
  private String _configFile;

  @CommandLine.Option(names = {"-command"}, required = true, arity = "1..*", description = "File system command, e.g."
      + " EXISTS, IS_DIR, LAST_MODIFIED, LENGTH, LIST_FILE, LIST_FILE_RECURSIVE, CP, COPY_DIR, COPY_FROM_LOCAL_FILE, "
      + "COPY_FROM_LOCAL_DIR, COPY_TO_LOCAL_FILE, MV, DELETE")
  private String[] _command;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, usageHelp = true, description = "Print this message.")
  private boolean _help = false;

  public FileSystemCommand setScheme(String scheme) {
    _scheme = scheme;
    return this;
  }

  public FileSystemCommand setConfigFile(String configFile) {
    _configFile = configFile;
    return this;
  }

  public FileSystemCommand setCommand(String[] command) {
    _command = command;
    return this;
  }

  public FileSystemCommand setHelp(boolean help) {
    _help = help;
    return this;
  }

  public String getScheme() {
    return _scheme;
  }

  public String getConfigFile() {
    return _configFile;
  }

  public String[] getCommand() {
    return _command;
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "FileSystem";
  }

  @Override
  public String toString() {
    return ("FileSystem --scheme " + _scheme + " -configFileName " + _configFile + " -command " + Arrays.toString(
        _command));
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Pinot File system operation.";
  }

  public String run()
      throws Exception {
    LOGGER.info("Executing command: " + this);
    Map<String, Object> configs =
        getConfigFile() == null ? new HashMap<>() : PinotConfigUtils.readConfigFromFile(getConfigFile());
    PinotFSFactory.init(new PinotConfiguration(configs));
    registerDefaultPinotFS();

    PinotFS pinotFS = PinotFSFactory.create(getScheme());
    String output = "DONE";
    String fsOp = _command[0].replace("_", "").toUpperCase();
    String[] arguments = Arrays.copyOfRange(_command, 1, _command.length);
    try {
      switch (fsOp) {
        case "LSFILE":
        case "LISTFILE":
          output = StringUtil.join("\n", pinotFS.listFiles(URI.create(arguments[0]), false));
          break;
        case "LISTFILERECURSIVE":
          output = StringUtil.join("\n", pinotFS.listFiles(URI.create(arguments[0]), true));
          break;
        case "CP":
        case "COPY":
          output = String.valueOf(pinotFS.copy(URI.create(arguments[0]), URI.create(arguments[1])));
          break;
        case "COPYDIR":
          pinotFS.copyDir(URI.create(arguments[0]), URI.create(arguments[1]));
          break;
        case "COPYFROMLOCALDIR":
          pinotFS.copyFromLocalDir(new File(arguments[0]), URI.create(arguments[1]));
          break;
        case "COPYFROMLOCALFILE":
          pinotFS.copyFromLocalFile(new File(arguments[0]), URI.create(arguments[1]));
          break;
        case "COPYTOLOCALFILE":
          pinotFS.copyToLocalFile(URI.create(arguments[0]), new File(arguments[1]));
          break;
        case "EXISTS":
          output = String.valueOf(pinotFS.exists(URI.create(arguments[0])));
          break;
        case "DELETE":
          pinotFS.delete(URI.create(arguments[0]), true);
          break;
        case "ISDIR":
        case "ISDIRECTORY":
          output = String.valueOf(pinotFS.isDirectory(URI.create(arguments[0])));
          break;
        case "LASTMODIFIED":
          output = String.valueOf(pinotFS.lastModified(URI.create(arguments[0])));
          break;
        case "LENGTH":
          output = String.valueOf(pinotFS.length(URI.create(arguments[0])));
          break;
        case "MV":
        case "MOVE":
          output = String.valueOf(pinotFS.move(URI.create(arguments[0]), URI.create(arguments[1]), true));
          break;
        case "MKDIR":
          output = String.valueOf(pinotFS.mkdir(URI.create(arguments[0])));
          break;
        default:
          output = "Unsupported file system op: " + fsOp + ". Current supported operations are: "
              + StringUtil.join(", ", SUPPORTED_FS_COMMANDS);
          break;
      }
    } catch (Exception e) {
      e.printStackTrace(new PrintStream(output));
    }
    return output;
  }

  private void registerDefaultPinotFS() {
    registerPinotFS("s3", "org.apache.pinot.plugin.filesystem.S3PinotFS",
        ImmutableMap.of("region", System.getProperty("AWS_REGION", "us-west-2")));
  }

  public static void registerPinotFS(String scheme, String fsClassName, Map<String, Object> configs) {
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      LOGGER.info("PinotFS for scheme: {} is already registered.", scheme);
      return;
    }
    try {
      PinotFSFactory.register(scheme, fsClassName, new PinotConfiguration(configs));
      LOGGER.info("Registered PinotFS for scheme: {}", scheme);
    } catch (Exception e) {
      LOGGER.error("Unable to init PinotFS for scheme: {}, class name: {}, configs: {}", scheme, fsClassName, configs,
          e);
    }
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info(result);
    return true;
  }

  public static void main(String[] args) {
    PluginManager.get().init();
    new CommandLine(new FileSystemCommand()).execute(args);
  }
}
