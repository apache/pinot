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

def zip = new File(basedir, 'target/simple-assembly-0.0.1-SNAPSHOT-plugin.zip')
if (!zip.isFile()) {
  // The invoker runs this script even when the IT build itself failed, and the exception thrown here then replaces
  // the invoker's own "build exited with code N" message in the console. Include the end of the IT build log so that
  // the console shows the real error instead of just a missing-file exception.
  def buildLog = new File(basedir, 'build.log')
  def logTail = buildLog.isFile() ? buildLog.readLines().takeRight(100).join('\n') : '(no build.log found)'
  throw new AssertionError("${zip} was not produced; end of ${buildLog}:\n${logTail}" as String)
}

def entries = zip.with {
  f ->
    def archive = new ZipFile(f)
    def result = archive.entries().findAll{ !it.directory }.collect { it.name } as Set
    archive.close()
    return result
}

assert entries == expected
