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

# Pinot Controller UI
This package contains code for Pinot Controller UI.

## How to setup Pinot UI for development 

1. Make sure pinot backend is running on port 9000. Follow [this guide](https://github.com/apache/pinot?tab=readme-ov-file#building-pinot) for the same.
2. Navigate to ui source code folder 
```shell
cd pinot-controller/src/main/resources
```
3. Switch to node `v20.11.0`.
Use nvm to switch to the required Node.js version used by the `frontend-maven-plugin` in `pinot-controller/pom.xml`. If you don’t have nvm, install it from [here](https://github.com/nvm-sh/nvm).
```shell
nvm use 20.11.0
```
4. Install required packages. Make sure you are using node `v20.11.0`.
```shell
npm ci
```
5. Start the Development Server
```shell
npm run dev
```

6. App should be running on [http://localhost:8080](http://localhost:8080)
