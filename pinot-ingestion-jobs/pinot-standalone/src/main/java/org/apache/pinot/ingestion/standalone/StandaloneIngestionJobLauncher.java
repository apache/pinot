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
package org.apache.pinot.ingestion.standalone;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringWriter;
import org.apache.pinot.ingestion.common.SegmentGenerationJobSpec;
import org.yaml.snakeyaml.Yaml;


public class StandaloneIngestionJobLauncher {


  public static void main(String[] args) throws Exception{
    String jobSpecFilePath = args[0];

    try(Reader reader = new BufferedReader(new FileReader(jobSpecFilePath))) {

      Yaml yaml = new Yaml();
      SegmentGenerationJobSpec spec = yaml.loadAs(reader, SegmentGenerationJobSpec.class);
      StringWriter sw = new StringWriter();
      yaml.dump(spec, sw);
      System.out.println("dump = " + sw.toString());

    }
  }
}
