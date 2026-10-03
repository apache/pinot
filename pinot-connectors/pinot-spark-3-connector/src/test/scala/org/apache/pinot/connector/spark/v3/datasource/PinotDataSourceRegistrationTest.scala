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
package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.sources.DataSourceRegister

import java.util.ServiceLoader
// Use the cross-2.12/2.13 compatible converter package, matching the rest of pinot-spark-3-connector
// (which still supports Scala 2.12 via the root pom's -Pscala-2.12 profile). The Spark 4 sibling
// is Scala 2.13 only and can use scala.jdk.CollectionConverters there.
import scala.collection.JavaConverters._

/**
 * Verifies that PinotDataSource is correctly registered via the Java ServiceLoader under the
 * Spark 3 DataSourceRegister SPI. If this test breaks we have a wiring problem with the
 * META-INF/services file or the DataSourceRegister interface — both critical for `spark.read
 * .format("pinot")` to resolve.
 */
class PinotDataSourceRegistrationTest extends BaseTest {

  test("PinotDataSource is discoverable as 'pinot' via DataSourceRegister SPI") {
    val registered = ServiceLoader.load(classOf[DataSourceRegister]).asScala.toSeq
    val pinot = registered.find(_.shortName() == "pinot")

    pinot shouldBe defined
    pinot.get.getClass.getName shouldBe classOf[PinotDataSource].getName
  }

  test("PinotDataSource implements the Spark 3 TableProvider + DataSourceRegister contract") {
    val ds = new PinotDataSource()
    ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
    ds shouldBe a[DataSourceRegister]
    ds.shortName() shouldBe "pinot"
    ds.supportsExternalMetadata() shouldBe true
  }

  test("isSpark4ConnectorOnClasspath returns false when the v4 connector class is absent") {
    // Spark-3 only is on the classpath in this test JVM; the bidirectional conflict guard
    // must therefore report no conflict. The probe runs against `getClass.getClassLoader`,
    // so a missing v4 class manifests as ClassNotFoundException → false.
    PinotDataSource.isSpark4ConnectorOnClasspath(getClass.getClassLoader) shouldBe false
  }

  test("guardAgainstSpark4ConnectorOnClasspath does not throw when only v3 is on classpath") {
    // The constructor invokes guardAgainstSpark4ConnectorOnClasspath() which throws if the
    // v4 class is on the same classpath. In this test JVM only v3 is, so no throw.
    noException should be thrownBy new PinotDataSource()
  }

  test("spark4ConflictMessage surfaces a clear instructional message") {
    // Pin the message contents so future refactors of the guard don't regress its UX.
    PinotDataSource.spark4ConflictMessage should include("pinot-spark-3-connector")
    PinotDataSource.spark4ConflictMessage should include("pinot-spark-4-connector")
    PinotDataSource.spark4ConflictMessage should include("Remove")
  }
}
