// Copyright 2015-2016 Eric Evans : Scala migration
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.elevans.migration.cassandra

import java.net.InetSocketAddress
import scala.collection.JavaConversions._
import org.scalatest._
import com.datastax.driver.core.{Row, Session, Cluster}
import org.elevans.migration.parse.KeyValueTransitionMetadataParser
import org.elevans.migration.resource.{JarFileResourceResolver, FileResourceResolver, ResourceTransitionFinder, ResourceTransition}

class CqlMigratorTest extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  // Cassandra
  val CASSANDRA_HOST = Option(System.getenv("CASSANDRA_HOST")) getOrElse "localhost"
  val CASSANDRA_PORT = Option(System.getenv("CASSANDRA_PORT")).map(_.toInt) getOrElse 9042
  var cluster: Cluster  = null
  implicit var session: Session = null

  // Migrator
  val TEST_ORIGIN_STATE = "None"
  val TEST_KEYSPACE = "test_cql_migration"
  val ALTERNATE_CURRENT_STATE_KEYSPACE = "test_cql_migration_current_state_keyspace"
  val transitionParser = new KeyValueTransitionMetadataParser("transition-before", "transition-after", "transition-destructive", "=")
  val resourceResolvers = Seq( FileResourceResolver, JarFileResourceResolver )
  var migrator: CqlMigrator = null

  override protected def beforeAll() {
    cluster = Cluster.builder.addContactPointsWithPorts(List(new InetSocketAddress(CASSANDRA_HOST, CASSANDRA_PORT))).build()
  }
  override protected def afterAll() {
    cluster.close()
  }
  override protected def beforeEach() {
    session = cluster.connect()
  }
  override protected def afterEach() {
    if (keySpaceExists(TEST_KEYSPACE)) session.execute(s"DROP KEYSPACE $TEST_KEYSPACE")
    if (keySpaceExists(ALTERNATE_CURRENT_STATE_KEYSPACE)) session.execute(s"DROP KEYSPACE $ALTERNATE_CURRENT_STATE_KEYSPACE")
    session.close()
  }
  def migratorHasTypicalSetup {
    if (!keySpaceExists(TEST_KEYSPACE)) session.execute(s"CREATE KEYSPACE $TEST_KEYSPACE WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"USE $TEST_KEYSPACE")
    val resourceTransitionFinder = new ResourceTransitionFinder(this.getClass.getClassLoader, "schema-scripts", transitionParser, resourceResolvers)
    migrator = new CqlMigrator(session, "test", TEST_ORIGIN_STATE, resourceTransitionFinder)
  }
  def migratorHasAlternateCurrentStateKeySpace {
    val resourceTransitionFinder = new ResourceTransitionFinder(this.getClass.getClassLoader, "schema-scripts-with-use-statements", transitionParser, resourceResolvers)
    migrator = new CqlMigrator(session, "test", TEST_ORIGIN_STATE, resourceTransitionFinder, Some(ALTERNATE_CURRENT_STATE_KEYSPACE))
  }

  def keySpaceExists(keySpace: String)(implicit session: Session) =
    session.execute(s"SELECT * FROM system.schema_keyspaces where keyspace_name='$keySpace'").all.size == 1

  def tableExists(table: String, keySpace: String)(implicit session: Session) =
    session.execute(s"SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='$keySpace' and columnfamily_name='$table'").all.size == 1

  def rowsInTable[R](table: String, keySpace: String) (mapping: Row => R) (implicit session: Session) =
    session.execute(s"SELECT * FROM $keySpace.$table").all.map { mapping }.toSet


  describe("CassandraCqlMigrator") {
    
    val A_VAL = " /* a */ "
    val B_VAL = "-- b "
    val C_VAL = "// '//c' "

    it("finds the CQL scripts in the specified resource directory") {
      migratorHasTypicalSetup
      migrator.transitions shouldBe Set(
        ResourceTransition("schema-scripts/fromNone_toA_destructiveFalse.cql", "None", "A", false),
        ResourceTransition("schema-scripts/fromA_toNone_destructiveTrue.cql", "A", "None", true),
        ResourceTransition("schema-scripts/fromA_toB_destructiveFalse.cql", "A", "B", false),
        ResourceTransition("schema-scripts/fromB_toA_destructiveTrue.cql", "B", "A", true))
    }
    it("getPath returns the path between requested states") {
      migratorHasTypicalSetup
      migrator.getPath("None", "B") shouldBe Some(Seq(
        ResourceTransition("schema-scripts/fromNone_toA_destructiveFalse.cql","None","A",false),
        ResourceTransition("schema-scripts/fromA_toB_destructiveFalse.cql","A","B",false)))

      migrator.getPath("B", "None") shouldBe Some(Seq(
        ResourceTransition("schema-scripts/fromB_toA_destructiveTrue.cql","B","A",true),
        ResourceTransition("schema-scripts/fromA_toNone_destructiveTrue.cql","A","None",true)))
    }
    it("getPathFromCurrentState returns the path between requested states") {
      migratorHasTypicalSetup
      migrator.getPathFromCurrentState("B") shouldBe Some(Seq(
        ResourceTransition("schema-scripts/fromNone_toA_destructiveFalse.cql","None","A",false),
        ResourceTransition("schema-scripts/fromA_toB_destructiveFalse.cql","A","B",false)))
    }
    it("getNonDestructivePath returns the path between requested states") {
      migratorHasTypicalSetup
      migrator.getNonDestructivePath("None", "B") shouldBe Some(Seq(
        ResourceTransition("schema-scripts/fromNone_toA_destructiveFalse.cql","None","A",false),
        ResourceTransition("schema-scripts/fromA_toB_destructiveFalse.cql","A","B",false)))

      migrator.getNonDestructivePath("B", "None") shouldBe None
    }
    it("getNonDestructivePathFromCurrentState returns the path between requested states") {
      migratorHasTypicalSetup
      migrator.getNonDestructivePathFromCurrentState("B") shouldBe Some(Seq(
        ResourceTransition("schema-scripts/fromNone_toA_destructiveFalse.cql","None","A",false),
        ResourceTransition("schema-scripts/fromA_toB_destructiveFalse.cql","A","B",false)))
    }
    it("executes each individual transition script in a path properly") {
      migratorHasTypicalSetup

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"

      val path1 = migrator.getPath("None", "A")
      path1 shouldNot be( None )
      path1 shouldNot be( Some(Seq.empty) )
      path1.get.exists(_.isDestructive) shouldBe false
      migrator.execute(path1.get)

      keySpaceExists(TEST_KEYSPACE) shouldBe true
      tableExists("a_table", TEST_KEYSPACE) shouldBe true
      rowsInTable("a_table", TEST_KEYSPACE)(r => (r.getInt("id"), r.getString("field1"))) shouldBe Set((1, A_VAL), (2, B_VAL), (3, C_VAL))
      migrator.getCurrentState shouldBe "A"

      val path2 = migrator.getPath("A", "B")
      path2 shouldNot be( None )
      path2 shouldNot be( Some(Seq.empty) )
      path2.get.exists(_.isDestructive) shouldBe false
      migrator.execute(path2.get)

      keySpaceExists(TEST_KEYSPACE) shouldBe true
      tableExists("a_table", TEST_KEYSPACE) shouldBe true
      rowsInTable("a_table", TEST_KEYSPACE)(r => (r.getInt("id"), r.getString("field1"), r.getBool("field2"))) shouldBe Set((1, A_VAL, false), (2, B_VAL, true), (3, C_VAL, false))
      migrator.getCurrentState shouldBe "B"

      val path3 = migrator.getPath("B", "A")
      path3 shouldNot be( None )
      path3 shouldNot be( Some(Seq.empty) )
      path3.get.exists(_.isDestructive) shouldBe true
      migrator.execute(path3.get)

      keySpaceExists(TEST_KEYSPACE) shouldBe true
      tableExists("a_table", TEST_KEYSPACE) shouldBe true
      rowsInTable("a_table", TEST_KEYSPACE)(r => (r.getInt("id"), r.getString("field1"))) shouldBe Set((1, A_VAL), (2, B_VAL), (3, C_VAL))

      migrator.getCurrentState shouldBe "A"

      val path4 = migrator.getPath("A", "None")
      path4 shouldNot be( None )
      path4 shouldNot be( Some(Seq.empty) )
      path4.get.exists(_.isDestructive) shouldBe true
      migrator.execute(path4.get)

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"
    }
    it("executes the transition scripts in a path, sequentially, and in order (verify entire path)") {
      migratorHasTypicalSetup

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"

      val path = migrator.getPathFromCurrentState("B")
      path shouldNot be( None )
      path shouldNot be( Some(Seq.empty) )
      path.get.exists(_.isDestructive) shouldBe false
      migrator.execute(path.get)

      keySpaceExists(TEST_KEYSPACE) shouldBe true
      tableExists("a_table", TEST_KEYSPACE) shouldBe true
      rowsInTable("a_table", TEST_KEYSPACE)(r => (r.getInt("id"), r.getString("field1"), r.getBool("field2"))) shouldBe Set((1, A_VAL, false), (2, B_VAL, true), (3, C_VAL, false))
      migrator.getCurrentState shouldBe "B"

      val pathBack = migrator.getPathFromCurrentState("None")
      pathBack shouldNot be( None )
      pathBack shouldNot be( Some(Seq.empty) )
      pathBack.get.exists(_.isDestructive) shouldBe true
      migrator.execute(pathBack.get)

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"
    }
    it("executes the transition scripts in a path, sequentially, and in order (verify entire path), using an alternate current state keyspace") {
      migratorHasAlternateCurrentStateKeySpace

      keySpaceExists(TEST_KEYSPACE) shouldBe false
      keySpaceExists(ALTERNATE_CURRENT_STATE_KEYSPACE) shouldBe false

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"

      val path = migrator.getPathFromCurrentState("B")
      path shouldNot be( None )
      path shouldNot be( Some(Seq.empty) )
      path.get.exists(_.isDestructive) shouldBe false
      migrator.execute(path.get)

      keySpaceExists(TEST_KEYSPACE) shouldBe true
      tableExists("a_table", TEST_KEYSPACE) shouldBe true
      rowsInTable("a_table", TEST_KEYSPACE)(r => (r.getInt("id"), r.getString("field1"), r.getBool("field2"))) shouldBe Set((1, A_VAL, false), (2, B_VAL, true), (3, C_VAL, false))
      migrator.getCurrentState shouldBe "B"

      val pathBack = migrator.getPathFromCurrentState("None")
      pathBack shouldNot be( None )
      pathBack shouldNot be( Some(Seq.empty) )
      pathBack.get.exists(_.isDestructive) shouldBe true
      migrator.execute(pathBack.get)

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      keySpaceExists(TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"
    }
    it("handles empty migration path") {
      migratorHasTypicalSetup

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"

      val path = migrator.getPathFromCurrentState("None")
      path shouldNot be( None )
      path shouldBe( Some(Seq.empty) )
      migrator.execute(path.get)

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"
    }
    it("migrates from the current state to the requested end state") {
      migratorHasTypicalSetup

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"

      migrator.migrateTo("B")

      keySpaceExists(TEST_KEYSPACE) shouldBe true
      tableExists("a_table", TEST_KEYSPACE) shouldBe true
      rowsInTable("a_table", TEST_KEYSPACE)(r => (r.getInt("id"), r.getString("field1"), r.getBool("field2"))) shouldBe Set((1, A_VAL, false), (2, B_VAL, true), (3, C_VAL, false))
      migrator.getCurrentState shouldBe "B"

      migrator.migrateTo("None")

      tableExists("a_table", TEST_KEYSPACE) shouldBe false
      migrator.getCurrentState shouldBe "None"
    }
    it("throws an Exception if it cannot migrate from the current state to the requested end state because no such path exists") {
      migratorHasTypicalSetup

      migrator.getCurrentState shouldBe "None"

      migrator.migrateTo("B")
      migrator.getCurrentState shouldBe "B"

      an [IllegalArgumentException] should be thrownBy
        migrator.migrateNonDestructiveTo("None")
    }
  }
}
