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

import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.LogicalTable;


/**
 * Utility class which contains {@link LogicalTable} related operations.
 */
public class LogicalTableUtils {

  private LogicalTableUtils() {
  }

  /**
   * Fetch {@link LogicalTable} from a {@link ZNRecord}.
   */
  public static LogicalTable fromZNRecord(@Nonnull ZNRecord record)
      throws IOException {
    String tableJSON = record.getSimpleField("tableJSON");
    return LogicalTable.fromString(tableJSON);
  }

  /**
   * Wrap {@link LogicalTable} into a {@link ZNRecord}.
   */
  public static ZNRecord toZNRecord(@Nonnull LogicalTable table) {
    ZNRecord record = new ZNRecord(table.getTableName());
    record.setSimpleField("tableJSON", table.toSingleLineJsonString());
    return record;
  }
}
