/*
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
import java.util.zip.ZipFile

def expected = [
    'simple-assembly/pinot-plugin.properties',
    'simple-assembly/classes/org/apache/pinot/it/Simple.class',
    "simple-assembly/commons-lang3-${commonslang3_version}.jar"
] as Set

def entries = new File(basedir,'target/simple-assembly-0.0.1-SNAPSHOT-plugin.zip').with {
  f ->
    def archive = new ZipFile(f)
    def result = archive.entries().findAll{ !it.directory }.collect { it.name } as Set
    archive.close()
    return result
}

assert entries == expected
