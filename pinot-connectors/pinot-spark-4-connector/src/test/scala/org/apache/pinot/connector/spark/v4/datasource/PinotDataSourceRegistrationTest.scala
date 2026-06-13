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
package org.apache.pinot.connector.spark.v4.datasource

import org.apache.spark.sql.sources.DataSourceRegister

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/**
 * Verifies that PinotDataSource is correctly registered via the Java ServiceLoader under the
 * Spark 4 DataSourceRegister SPI. If this test breaks we have a wiring problem with the
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

  test("PinotDataSource implements the Spark 4 TableProvider + DataSourceRegister contract") {
    val ds = new PinotDataSource()
    ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
    ds shouldBe a[DataSourceRegister]
    ds.shortName() shouldBe "pinot"
    ds.supportsExternalMetadata() shouldBe true
  }

  test("isSpark3ConnectorOnClasspath returns false when the v3 connector class is absent") {
    // The v3 PinotDataSource class is NOT a dependency of pinot-spark-4-connector, so the
    // standard test ClassLoader should not see it. This pins the negative case so a future
    // accidental dependency on pinot-spark-3-connector (which would defeat the conflict
    // guard) fails this test.
    PinotDataSource.isSpark3ConnectorOnClasspath(getClass.getClassLoader) shouldBe false
  }

  test("guardAgainstSpark3ConnectorOnClasspath does not throw when only v4 is on classpath") {
    // The constructor of PinotDataSource invokes the guard; if the negative case starts
    // throwing here it means the v3 class is unintentionally on this module's classpath.
    noException should be thrownBy new PinotDataSource()
  }

  test("spark3ConflictMessage surfaces a clear instructional message") {
    // The exact message text is part of the user-facing contract; a regression test pins it
    // so an accidental change can't make the diagnostic less actionable.
    PinotDataSource.spark3ConflictMessage should include("pinot-spark-4-connector")
    PinotDataSource.spark3ConflictMessage should include("pinot-spark-3-connector")
    PinotDataSource.spark3ConflictMessage should include("Remove")
  }
}
