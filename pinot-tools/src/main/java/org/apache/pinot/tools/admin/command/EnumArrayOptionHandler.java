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

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;


public class EnumArrayOptionHandler<T extends Enum<T>> extends OptionHandler<T> {

  public EnumArrayOptionHandler(CmdLineParser parser, OptionDef option, Setter<T> setter) {
    super(parser, option, setter);
  }

  /**
   * Returns {@code "T[]"}.
   *
   * @return return "T[]";
   */
  @Override
  public String getDefaultMetaVariable() {
    return setter.getType().getName() + "[]";
  }

  /**
   * Tries to parse {@code String[]} argument from {@link Parameters}.
   */
  @Override
  public int parseArguments(Parameters params)
      throws CmdLineException {
    int counter = 0;
    for (; counter < params.size(); counter++) {
      String param = params.getParameter(counter);

      if (param.startsWith("-")) {
        break;
      }

      for (String p : param.split(" ")) {
        Class<T> t = (Class<T>) setter.getType();
        setter.addValue(Enum.valueOf(t, p));
      }
    }

    return counter;
  }
}