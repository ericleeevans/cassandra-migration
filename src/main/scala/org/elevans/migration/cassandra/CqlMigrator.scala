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

import java.io.FileNotFoundException
import com.datastax.driver.core._
import com.typesafe.scalalogging.{LazyLogging => Logging}
import org.elevans.migration.MigrationGraph
import org.elevans.migration.resource.{ResourceTransition, ResourceTransitionFinder}
import org.apache.commons.io.IOUtils

object CqlMigrator {
  val CURRENT_STATE_TABLE_NAME = "cql_migrator_current_states"
}

/**
 * This utility can be used to:
 *  - access the CQL scripts that create and modify Cassandra objects (e.g., column families, etc.) for some "scope".
 *  - parse developer-supplied metadata from comments in these CQL scripts (each of which implements a single Transition
 *    from one "state" to another) to determine the sequence of scripts (or "path") that must be executed to migrate the
 *    scope to a given target state from its current state.
 *  - execute the CQL scripts in a given path, sequentially.
 *  - maintain a persistent copy of the scope's current state for use in subsequent future migrations.
 *
 *  NOTE: The optional 'alternateCurrentStateKeySpace' may be given to specify where the scope's current state tracking
 *  data should be stored. It defaults to None, in which case this data is stored in the Session's keyspace. A common case
 *  would be accept the default and to ensure that none of the CQL migration scripts attempt to change the Session's
 *  keyspace (e.g., via a 'USE' statement). In that case the scope's current state data will be stored in the same keyspace
 *  all of its other data. However, you may optionally provide an explicit 'alternateCurrentStateKeySpace' if for some
 *  reason you do not want the current state stored in the Session's keyspace (e.g., if some of the CQL scripts do change
 *  the Session's keyspace via 'USE' statements).
 *
 * @param session                       Cassandra session to use for executing CQL scripts and maintaining current state.
 * @param scope                         scope of Cassandra objects manipulated by the CQL scripts.
 * @param originState                   state that this scope is in before the first CQL script is executed (i.e., before any of them are created).
 * @param transitionFinder              ResourceTransitionFinder instance that can find resources that implement Transitions,
 *                                      and open each one as an InputStream.
 * @param alternateCurrentStateKeySpace if defined, store the current state tracking data in this keyspace, creating it
 *                                      if it does not exist; defaults to None, in which case the current state tracking
 *                                      data is stored in the Session's keyspace.
 *
 * @author Eric Evans
 */
class CqlMigrator(val session: Session,
                  val scope: String,
                  val originState: String,
                  val transitionFinder: ResourceTransitionFinder,
                  val alternateCurrentStateKeySpace: Option[String] = None)

  extends MigrationGraph[ResourceTransition] with Logging
{
  import org.elevans.migration.cassandra.CqlMigrator._

  override lazy val transitions = transitionFinder.getTransitions

  /** Get the current state of this scope. */
  def getCurrentState: String = {
    if (alternateCurrentStateKeySpace.isDefined && !alternateCurrentStateKeySpaceExists) createAlternateCurrentStateKeySpace()
    if (!currentStateTableExists) createCurrentStatesTable()
    loadCurrentState.getOrElse(originState)
  }

  /** Set the current state of this scope. */
  def setCurrentState(state: String) = session.execute(s"UPDATE $currentStateTable SET state = '$state' WHERE scope = '$scope'")

  /** Return a sequence of transitions (if any exists) that would migrate the scope from its current state to the given end state. */
  def getPathFromCurrentState(endState: String): Option[Seq[ResourceTransition]] = getPath(getCurrentState, endState)

  /** Return a non-destructive sequence of transitions (if any exists) that would migrate the scope from its current state to the given end state. */
  def getNonDestructivePathFromCurrentState(endState: String): Option[Seq[ResourceTransition]] = getNonDestructivePath(getCurrentState, endState)

  /**
   * If a sequence of transitions exists from the current state to the given end state, execute them in order.
   * @throws IllegalArgumentException if no sequence of transitions exists from the current state to the requested end state
   */
   def migrateTo(endState: String) = getPathFromCurrentState(endState).map(execute)
    .getOrElse(throw new IllegalArgumentException(s"No sequence of transitions exists from the current state '$getCurrentState' to the requested end state '$endState'"))

  /**
   * If a sequence of non-destructive transitions exists from the current state to the given end state, execute them in order.
   * @throws IllegalArgumentException if no sequence of non-destructive transitions exists from the current state to the requested end state
   */
  def migrateNonDestructiveTo(endState: String) = getNonDestructivePathFromCurrentState(endState).map(execute)
    .getOrElse(throw new IllegalArgumentException(s"No non-destructive sequence of transitions exists from the current state '$getCurrentState' to the requested end state '$endState'"))

  /**
   * Execute the given sequence of transitions in order, if the first transition starts with the current state of the scope.
   * @throws IllegalArgumentException if the first transition does not start with the current state of the scope
   */
  def execute(path: Seq[ResourceTransition]) = if (path.nonEmpty) {
    val currentState = getCurrentState
    if (path.head.beforeState != currentState) throw new IllegalArgumentException(s"Cannot execute migration from state '${path.head.beforeState}' because current state is actually '$currentState'")

    logger.info(s"Migrating scope '$scope' from current state '$currentState' to the target state '${path.last.afterState}'")
    path foreach { transition =>
      try {
        logger.info(s"Executing transition from state '${transition.beforeState}' to state '${transition.afterState}' using CQL script '${transition.resourcePath}'")
        executeCqlScript(transition.resourcePath)
        setCurrentState(transition.afterState)
      }
      catch {
        case e:Throwable => throw new Exception(s"Failed to complete CQL transition script '${transition.resourcePath}'; current state is '$getCurrentState', however some parts of the transition might have been committed to the Cassandra store.", e)
      }
    }
    logger.info(s"Migration completed for scope '$scope'; current state is now '$getCurrentState'")
  }

  require(session.getLoggedKeyspace != null || alternateCurrentStateKeySpace.isDefined, "The given Session is not logged into any keyspace, and no alternateCurrentStateKeySpace was given.")
  
  val currentStateKeySpace = alternateCurrentStateKeySpace.getOrElse(session.getLoggedKeyspace)
  private val currentStateTable = s"$currentStateKeySpace.$CURRENT_STATE_TABLE_NAME"

  private def alternateCurrentStateKeySpaceExists = session.execute(s"SELECT * FROM system.schema_keyspaces where keyspace_name='${alternateCurrentStateKeySpace.get}'").all.size == 1
  private def createAlternateCurrentStateKeySpace() = session.execute(s"CREATE KEYSPACE ${alternateCurrentStateKeySpace.get} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

  private def currentStateTableExists = session.execute(s"SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='$currentStateKeySpace' and columnfamily_name='$CURRENT_STATE_TABLE_NAME'").all.size == 1
  private def createCurrentStatesTable() = session.execute(s"CREATE TABLE $currentStateTable (scope text PRIMARY KEY, state text)")

  private def loadCurrentState: Option[String] = Option(session.execute(s"SELECT state FROM $currentStateTable where scope='$scope'").one()).map(_.getString("state"))

  private def executeCqlScript(resourcePath: String) = transitionFinder.getInputStream(resourcePath) match {
    case None     => throw new FileNotFoundException(resourcePath)
    case Some(in) =>
      try {
        val cqlScript = IOUtils.toString(in, "UTF-8")
        val statements = CqlUtils.stripComments(cqlScript).split(";").map(_.trim).filterNot(_.isEmpty)
        statements foreach session.execute
      }
      finally { in.close() }
  }
}

