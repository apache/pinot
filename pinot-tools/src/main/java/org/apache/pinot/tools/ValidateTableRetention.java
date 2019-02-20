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
package org.apache.pinot.tools;

import org.apache.pinot.controller.util.TableRetentionValidator;
import org.kohsuke.args4j.Option;


@SuppressWarnings("FieldCanBeLocal")
public class ValidateTableRetention extends AbstractBaseCommand implements Command {
  @Option(name = "-zkAddress", required = true, metaVar = "<string>", usage = "Address of the Zookeeper (host:port)")
  private String _zkAddress;

  @Option(name = "-clusterName", required = true, metaVar = "<string>", usage = "Pinot cluster name")
  private String _clusterName;

  @Option(name = "-tableNamePattern", required = false, metaVar = "<string>", usage = "Optional table name pattern to trigger adding inverted index, default: null (match any table name)")
  private String _tableNamePattern = null;

  @Option(name = "-durationInDaysThreshold", required = false, metaVar = "<long>", usage =
      "Optional duration in days threshold to log a warning for table with too large retention time, default: "
          + TableRetentionValidator.DEFAULT_DURATION_IN_DAYS_THRESHOLD)
  private long _durationInDaysThreshold = TableRetentionValidator.DEFAULT_DURATION_IN_DAYS_THRESHOLD;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Check the tables retention setting and segments metadata for the given cluster";
  }

  @Override
  public boolean execute()
      throws Exception {
    TableRetentionValidator tableRetentionValidator = new TableRetentionValidator(_zkAddress, _clusterName);
    tableRetentionValidator.overrideDefaultSettings(_tableNamePattern, _durationInDaysThreshold);
    tableRetentionValidator.run();
    return true;
  }
}
