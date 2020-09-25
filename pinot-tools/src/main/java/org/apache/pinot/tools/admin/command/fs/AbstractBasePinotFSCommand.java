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
package org.apache.pinot.tools.admin.command.fs;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.kohsuke.args4j.Option;


abstract class AbstractBasePinotFSCommand extends AbstractBaseAdminCommand implements Command {

  @Option(name = "-help", aliases = {"-h", "--h", "--help"}, usage = "Prints help")
  protected boolean help = false;

  @Option(name = "-config", aliases = {"-c", "--config"}, usage = "A configuration file for Pinot file systems.")
  private String _fsConfigFile;

  private PinotConfiguration _configuration;

  @Override
  public boolean getHelp() {
    return help;
  }

  protected PinotFS getPinotFS(URI uri) throws Exception {
    String scheme = uri.getScheme();
    if (scheme == null) {
      scheme = "file";
    }

    PinotFSFactory.init(getConfiguration());
    // TODO: handle config doesn't define scheme that is being used
    return PinotFSFactory.create(scheme);
  }

  private PinotConfiguration getConfiguration()
      throws ConfigurationException {
    if (_configuration != null) {
      return _configuration;
    }

    // TODO: handle when this is a directory and look for other bad cases
    // TODO: handle when this is null (local pinot fs)
    return new PinotConfiguration(new PropertiesConfiguration(new File(_fsConfigFile)));
  }

  abstract String getSubCommandUsage();

  public void printUsage() {
    System.out.println("Usage: pinot-admin.sh PinotFS " + getSubCommandUsage());

    Class klass =  this.getClass();
    while (klass != null) {
      for (Field f : klass.getDeclaredFields()) {
        if (f.isAnnotationPresent(Option.class)) {
          Option option = f.getAnnotation(Option.class);

          System.out.println(String
              .format("\t%-25s: %s (required=%s)", option.name(), option.usage(),
                  option.required()));
        }
      }
      klass = klass.getSuperclass();
    }
  }

  protected boolean checkMatchingSchemes(URI a, URI b) {
    String schemeA = a.getScheme();
    if (schemeA == null) {
      schemeA = "file";
    }

    String schemeB = b.getScheme();
    if (schemeB == null) {
      schemeB = "file";
    }

    return schemeA.equals(schemeB);
  }

  public void setConfiguration(Map<String, Object> props) {
    _configuration = new PinotConfiguration(props);
  }
}
