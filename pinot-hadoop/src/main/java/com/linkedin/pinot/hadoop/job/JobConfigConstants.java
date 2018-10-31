/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.hadoop.job;


public class JobConfigConstants {
  public static final String PATH_TO_INPUT = "path.to.input";
  public static final String PATH_TO_OUTPUT = "path.to.output";
  public static final String PATH_TO_READER_CONFIG = "path.to.reader.config";
  // Leave this for backward compatibility. We prefer to use the schema fetched from the controller.
  public static final String PATH_TO_SCHEMA = "path.to.schema";

  public static final String TARGZ = ".tar.gz";

  public static final String TIME_COLUMN_NAME = "table.time.column.name";
  public static final String TIME_COLUMN_TYPE = "table.time.column.type";
  public static final String TABLE_PUSH_TYPE = "table.push.type";
  public static final String SCHEMA = "data.schema";
  public static final String SEGMENT_TABLE_NAME = "segment.table.name";

  public static final String PUSH_TO_HOSTS = "push.to.hosts";
  public static final String PUSH_TO_PORT = "push.to.port";

  public static final String TABLE_CONFIG = "table.config";

  public static final String DEFAULT_PERMISSIONS_MASK = "fs.permissions.umask-mode";
}
