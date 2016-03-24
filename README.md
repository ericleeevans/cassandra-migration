# Cassandra Migration Utilities for Scala

The `CassandraSchema` trait and the `CqlMigrator` utility can be used to automatically determine and execute the proper sequence of CQL scripts needed to migrate a Cassandra database from one state to another.

For example, if some releases of your software package include a CQL script to migrate an existing Cassandra database's schema and data from the _previous_ release of your software package, then you can use this utility to automatically migrate such a database across _multiple_ releases, without needing to provide special migration scripts for every combination of current and target release.

## Usage

This package depends on the [Asset Migration Utilities for Scala](https://github.com/ericleeevans/migration). To use it, you will need to add _both dependencies to your project. E.g.:

~~~
"org.elevans" % "migration" % "0.1.0"

"org.elevans" % "cassandra-migration" % "0.1.0"
~~~

## The CqlMigrator Class

An instance of `CqlMigrator` can be used to:

* Define and migrate logically distinct sets of schema and data separately, by giving each a unique name (i.e., its "_scope_").

* Find the CQL scripts that create and modify Cassandra objects (e.g., column families, etc.) for your software package (these are typically provided as _resources_).

* Parse simple metadata from comments in these CQL scripts to determine the best sequence of scripts (or "path") that must be executed to migrate the
scope to a given target state from its current state.

* Execute the CQL scripts in the path, sequentially.

* Maintain a persistent copy of each scope's current state for use in future migrations.

You can instantiate an instance of `CqlMigrator` with the following:
 
* A Cassandra session (i.e., an instance of `com.datastax.driver.core.Session`) to use for executing the CQL scripts and maintaining the current state data.
 
* The scope (e.g., "com.fooble.bar.data.users") of Cassandra objects manipulated by these CQL scripts.
 
* The name of the "origin state" state that this scope is in before any CQL script is ever executed (i.e., before any of its Cassandra schema and data is created). E.g., if you use release numbers for the various states, you might specify "0.0.0" for the origin state; but you could use any String that would notbe a typical release name, e.g., "Null".
 
* A `ResourceTransitionFinder` instance that can find and open your CQL scripts. See the "Finding Transition Metadata" section of the [Asset Migration Utilities for Scala](https://github.com/ericleeevans/migration), which provides convenient tools to find your CQL scripts in the filesystem, as JAR resource files, or as OSGi bundle resource files.
                                    

By default, the specified scope's "current state tracking data" will be stored in Cassandra, in the Session's keyspace - i.e., in the same keyspace as all of its other data. 
The common case is to accept this default, and to make sure that none of your CQL scripts attempt to switch the Session's keyspace via the 'USE' statement.

An _optional_ `alternateCurrentStateKeySpace` name may be given to specify a _different_ keyspace where the scope's current state tracking data should be stored, if for some reason you do not want this data stored in the Session's keyspace (for example, if some of your CQL scripts _do_ switch the Session's keyspace via 'USE' statements).


## The CassandraSchema Trait

The `CassandraSchema` trait provides a level of convenience above the `CqlMigrator` class. 
An implementation instance of `CassandraSchema` (typically a Scala `object`) represents a group of related Cassandra objects that must be migrated together, to get to a specified target state (i.e., the state required by the current release).
You just need to provide concrete values for the abstract members noted below, and then your implementation of `CassandraSchema` will provide methods to execute the migration automatically.

* The scope name (same as that for `CqlMigrator`; e.g., "com.fooble.bar.data.users").

* The current "required state" - i.e., the state of the scope's Cassandra schema and data required by the current release of your software package.

* The "origin state" (same as that for `CqlMigrator`; e.g., "0.0.0", "Null", "Void", or etc.).

* The file path to the _resource_ "directory" in the classpath that contains the CQL schema migration scripts for this schema.

* An instance of TransitionMetadataParser for extracting state-transition metadata from those CQL scripts.

### Example

The 1.6.0 release of the software package "com.fooble.bar" might include the following `CassandraSchema` implementation object:

~~~
import org.elevans.migration.parse._

object UsersCassandraSchema extends CassandraSchema {
  val name             = "com.fooble.bar.data.users"
  val requiredState    = "1.6.0"
  val originState      = "0.0.0"
  val scriptsLocation  = "com/fooble/bar/data/users/cql_scripts"
  val transitionParser = new KeyValueTransitionMetadataParser("state-before", "state-after", "is-destructive", ":")
}
~~~

This package's JAR file also contains all of the CQL scripts ever written to migrate the Cassandra schema and data for a given release of "com.fooble.bar" to the next release.
These CQL scripts are in the "resources" directory "com/fooble/bar/data/users/cql_scripts".
Each CQL script contains CQL comments that specify the before and after states for that script, and whether the changes made by that script are "destructive" (i.e., irreversible). 
These comments use the terms "state-before", "state-after", and "is-destructive" for those pieces of information, and use a colon (':') as the name/value separator. 

For example, the CQL script from the first release 0.1.0 might contain the following comments:

~~~
-- state-before : 0.0.0
-- state-after : 0.1.0
-- is-destructive : false

CREATE TABLE users (
  id UUID PRIMARY KEY,
  [... and so on...]
~~~

If the releases between 0.1.0 and 1.6.0 required no CQL scripts, but release 1.6.0 _does_ require a CQL script, then release 1.6.0 would have a CQL script containing comments like these:

~~~
-- state-before : 0.1.0
-- state-after : 1.6.0
-- is-destructive : false

ALTER TABLE users 
  [... and so on...]
~~~

By providing these CQL scripts as well as the `UsersCassandraSchema` object, the 1.6.0 release of "com.fooble.bar" could provide code (i.e., in a "migrate" script, or at initialization, or etc.) that automatically executes the required migration. 

For example:

~~~
val session: com.datastax.driver.core.Session = . . .

. . .

UsersCassandraSchema.migrateToRequiredState(session, Seq(new JarFileResourceResolver()))

. . .
~~~



---
 