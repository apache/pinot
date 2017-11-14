/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools;

import java.util.Arrays;
import java.util.List;

public final class SchemaInfo {
    /* Zookeeper Configurations */
    public static String DEFAULT_ZOOKEEPER_ADDRESS = "localhost:2181";

    /* Pinot Broker Configurations */
    public static int DEFAULT_BROKER_PORT = 8099;

    /* Pinot Controller Configurations */
    public static String DEFAULT_DATA_DIR = "/tmp/pinotController";
    public static String DEFAULT_CONTROLLER_PORT = "9000";

    /* Pinot Server Configurations */
    public static int DEFAULT_SERVER_PORT = 8098;
    public static String DATA_DIR = "/tmp/data";
    public static String SEGMENT_DIR = "/tmp/segment";

    /* Schema Files for tables */
    public static final List<String> SCHEMAS = Arrays.asList("new_data/jobSchema.json", "new_data/adsSchema.json",
            "new_data/articlesSchema.json", "new_data/viewsSchema.json");

    /* Schema Annotation files for each schema file */
    public static final List<String> SCHEMAS_ANNOTATIONS = Arrays.asList("new_data/jobSchemaAnnotation.json",
            "new_data/adsSchemaAnnotation.json", "new_data/articlesSchemaAnnotation.json", "new_data/viewsSchemaAnnotation.json");

    /* Number of records to generate for each table */
    public static final List<Integer> NUM_RECORDS = Arrays.asList(1000000, 1000, 1000, 1000);

    /* Number of segments to generate using NUM_RECORDS for each table */
    public static final List<Integer> NUM_SEGMENTS = Arrays.asList(4, 4, 4, 4);

    /* Names of directory to store the AVRO data for each table */
    public static final List<String> DATA_DIRS = Arrays.asList("jobs", "ads", "articles", "views");

    /* Names of segments for each table - will be appended by segment number for each table */
    public static final String SEGMENT_NAME = "segment";

    /* Table names */
    public static final List<String> TABLE_NAMES = Arrays.asList("Job", "Ads", "Articles", "Views");

    /* Table configuration files */
    public static final List<String> TABLE_DEFINITIONS = Arrays.asList("new_data/jobTable.json",
            "new_data/adsTable.json", "new_data/articlesTable.json", "new_data/viewsTable.json");

    /* Upload intervals for each table (in minutes) */
    public static final List<Integer> UPLOAD_DURATION = Arrays.asList(1, 2, 3, 4);

}
