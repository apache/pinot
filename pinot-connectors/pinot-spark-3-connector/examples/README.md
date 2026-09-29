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
# Pinot Spark 3 Connector Example: Reading from Pinot via Proxy with Auth Token

This example demonstrates how to use the Pinot Spark 3 Connector to read data from a Pinot cluster via a proxy with authentication token support.

## Prerequisites

- Apache Spark 3.x installed and `spark-shell` available in your PATH.

- Setup PINOT_HOME env variable:
  ```
  export PINOT_HOME=/path/to/pinot
  ```

- The Pinot Spark 3 Connector shaded JAR built and available at:
  ```
  $PINOT_HOME/pinot-connectors/pinot-spark-3-connector/target/pinot-spark-3-connector-*-shaded.jar
  ```

- Example Scala script located at:
  ```
  $PINOT_HOME/pinot-connectors/pinot-spark-3-connector/examples/read_pinot_from_proxy_with_auth_token.scala
  ```

## How to Run

Launch the example in `spark-shell` with the following command:

```bash
spark-shell --master 'local[*]' --name read-pinot --jars "$PINOT_HOME/pinot-connectors/pinot-spark-3-connector/target/pinot-spark-3-connector-*-shaded.jar" < "$PINOT_HOME/pinot-connectors/pinot-spark-3-connector/examples/read_pinot_from_proxy_with_auth_token.scala"
```





