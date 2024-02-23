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
package org.apache.pinot.client.utils;

public class Constants {
  private Constants() {
  }

  public static final String DRIVER_NAME = "APACHE_PINOT_DRIVER";
  public static final String DRIVER_VERSION = "1.0";
  public static final String PRODUCT_NAME = "APACHE_PINOT";
  public static final String PINOT_VERSION = "0.10"; //This needs to be changed as per the project maven version

  public static final String[] CATALOG_COLUMNS = {"TABLE_CAT"};
  public static final String[] CATALOG_COLUMNS_DTYPES = {"STRING"};

  public static final String[] SCHEMA_COLUMNS = {"TABLE_SCHEM", "TABLE_CATALOG"};
  public static final String[] SCHEMA_COLUMNS_DTYPES = {"STRING", "STRING"};

  public static final String[] TABLE_COLUMNS = {
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
      "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
  };
  public static final String[] TABLE_COLUMNS_DTYPES =
      {"STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING"};

  public static final String[] TABLE_SCHEMA_COLUMNS = {
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
      "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
      "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
      "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
  };
  public static final String[] TABLE_SCHEMA_COLUMNS_DTYPES = {
      "STRING", "STRING", "STRING", "STRING", "INT", "STRING", "INT", "INT", "INT", "INT", "INT", "STRING", "STRING",
      "INT", "INT", "INT", "INT", "STRING", "STRING", "STRING", "STRING", "INT", "STRING", "STRING"
  };

  public static final String[] TABLE_TYPES_COLUMNS = {"TABLE_TYPE"};
  public static final String[] TABLE_TYPES_COLUMNS_DTYPES = {"STRING"};

  public static final String TABLE_TYPE = "TABLE";
  public static final String GLOBAL_CATALOG = "global";

  public static final String SYS_FUNCTIONS = "maxTimeuuid,minTimeuuid,token,uuid";
  public static final String NUM_FUNCTIONS = "avg,count,max,min,sum,exp,ln,ceil,floor,sqrt";
  public static final String TIME_FUNCTIONS =
      "toEpochSeconds,toEpochMinutes,toEpochHours,t oEpochDays,toEpochSecondsRounded,toEpochMinutesRounded,"
          + "toEpochHoursRounded,toEpochDaysRounded,toEpochSecondsBucket,toEpochMinutesBucket,toEpochHoursBucket,"
          + "toEpochDaysBucket,fromEpochSeconds,fromEpochMinutes,fromEpochHours,fromEpochDays,"
          + "fromEpochSecondsBucket,fromEpochMinutesBucket,fromEpochHoursBucket,fromEpochDaysBucket,"
          + "toDateTime,fromDateTime,round,now";
  public static final String STRING_FUNCTIONS =
      "lower,upper,trim,ltrim,rtrim,subst,regexp_extract,reverse,replace,lpad,rpad,length,strpos,startsWith,concat";
}
