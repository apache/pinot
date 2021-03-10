<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Compatibility Regression Testing Scripts for Apache Pinot

## Usage

### Step 1: checkout source code and build targets for older commit and newer commit
```shell
./compatibility-verifier/checkoutAndBuild.sh [olderCommit] [newerCommit] [workingDir]
```
***NOTE***: `[workingDir]` is optional, if user does not specify `[workingDir]`, the script will create a temporary working 
dir and output the path, which can be used in step 2.

### Step 2: run compatibility regression test against the two targets build in step1
```shell
./compatibility-verifier/compCheck.sh [workingDir]
```
***NOTE***: the script can only be run under the root folder of the project currently. Before run the script, make sure to 
change to the right directory first.
