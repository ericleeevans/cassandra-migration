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

import org.elevans.migration.parse.TransitionMetadataParser
import org.elevans.migration.resource.{ResourceResolver, ResourceTransitionFinder}
import com.datastax.driver.core.Session

/**
 * Represents a group of related Cassandra objects that are migrated together.
 *
 * @author Eric Evans
 */
trait CassandraSchema {

  /** Unique name for the schema (e.g., "com.foo.bar.widgets") */
  val name: String

  /** Schema state required by the current version of the code that uses it. */
  val requiredState: String

  /** The "empty" state of the schema, before any of its CQL scripts is executed (i.e., when it does not yet exist). */
  val originState: String

  /** Path to the resource "directory" in the classpath that contains the CQL schema migration scripts for this schema. */
  val scriptsLocation: String

  /** TransitionMetadataParser for extracting state-transition metadata from the CQL scripts. */
  val transitionParser: TransitionMetadataParser

  def getMigrator(session: Session, resourceResolvers: Seq[ResourceResolver]) = {
    val cqlScriptFinder = new ResourceTransitionFinder(this.getClass.getClassLoader, scriptsLocation, transitionParser, resourceResolvers)
    new CqlMigrator(session, name, originState, cqlScriptFinder)
  }

  def migrateToRequiredState(session: Session, resourceResolvers: Seq[ResourceResolver]) =
    migrateTo(requiredState, session, resourceResolvers)

  def migrateTo(targetState: String, session: Session, resourceResolvers: Seq[ResourceResolver]) = {
    val migrator = getMigrator(session, resourceResolvers)
    migrator.migrateTo(targetState)
  }
}



import org.elevans.migration.parse._

object CoreCassandraSchema extends CassandraSchema {

  val name             = "com.fooble.bar.data.users"
  val requiredState    = "0.1.0"
  val originState      = "0.0.0"
  val scriptsLocation  = "com/fooble/bar/data/users/cql_scripts"
  val transitionParser = new KeyValueTransitionMetadataParser("state-before", "state-after", "is-destructive", ":")
}
