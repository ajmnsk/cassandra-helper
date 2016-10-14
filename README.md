Simple cassandra-helper using Scala
===================================

This library provides a few simple API(s) to deal with Cassandra database Session(s), connectivity, and basic DML routines.
Below code snippets describe library usage giving it in a context of Play application written in Scala.

## Connect using Singleton

A Singleton type ```CassandraConnect``` must extend Connect abstract class to handle database Session(s), and connectivity.
Values for ```hosts```, ```port```, and ```keySpaces``` must be available at the time of Singleton construction.

```
import javax.inject.{ Inject, Singleton }
import play.api.inject.ApplicationLifecycle
import play.api.Logger
import scala.concurrent.Future

import org.aj.cassandra.Connect
import myapp.Config._

@Singleton
class CassandraConnect @Inject() (lifecycle: ApplicationLifecycle) extends Connect(hosts, port, keySpaces) {

  connect() match {
    case Right(b) =>
      Logger.info(s"Connected to database, host(s): ${hosts}, port: ${port}, keySpace(s): ${keySpaces}")
    case Left(error) =>
      Logger.error(s"Failed to connect to database error: ${error}")
  }

  lifecycle.addStopHook { () =>
    Logger.info(s"Closing database Session(s), connection to host(s): ${hosts}, port: ${port}, keySpace(s): ${keySpaces}")
    Future.successful(close())
  }
}
```

Method ```connect()``` is attempting to build connection to Cassandra cluster, and create Session(s) for the list of keys space(s) provided.
Method will fail if there is invalid (non existing) ```key space``` value.
Once constructed, ```CassandraConnect``` will become available for application wide usage.
It has a public method ```session(keySpace: String)``` to get a Session object by a key space value.

## Create Statement Singleton on per table basis

Singleton ```MyTableStatements``` must extend ```Statement``` trait with one or few of the following traits depending on your needs: ```Read```, ```Insert```, ```Delete```.
It uses previously created ```CassandraConnect``` to get access to a valid cassandra Session.
Once constructed, it can be used to get a Session object for the key space table belongs to, and ```PreparedStatement```S for ```read()```, ```insert()```, and ```delete()``` operations on the table.

```
import javax.inject.{ Inject, Named, Singleton }

import com.datastax.driver.core.Session
import org.aj.cassandra._
import play.api.Logger
import myapp.Config._

@Singleton
class MyTableStatements @Inject() (cassandraConnect: CassandraConnect, @Named("Simple") sql: Sql) extends Statement with Read with Insert with Delete {

  val keySpace = myapp.Config.myKeySpace
  val fields = Set(
    "uuid_1",
    "key_2",
    "text_1",
    "text_2",
    "date_1",
    "date_2",
    "array_1",
    "int_1")
  val keys = Set("uuid_1=?", "key_2=?")
  val table = "mytable"
  val readSql = sql.select(table, fields, Some(keys))
  val insertSql = sql.insert(table, fields)
  val deleteSql = sql.delete(table, keys)
  val sqls: Set[String] = Set(readSql, insertSql, deleteSql)

  implicit val connectRetry: Int = models.utils.DbConfig.connectRetry

  def session(): Option[Session] = cassandraConnect.session(keySpace)

  getPreparedStatements() match {
    case Right(number) => Logger.info(s"Created prepared statements: ${sqls}")
    case Left(error) => Logger.error(s"Failed to create prepared statements: ${error}")
  }

}
```

## Connection re-try

You can enable connection re-try feature by assigning a greater than ```0``` value to a ```connectRetry``` field.
It helps in cases if database connection is lost due to network, or being temporarely off line.

## Read, Update, Delete record(s) sample

Here is a sample application class used to perform basic DML tasks on a table.

```
import java.util.Date
import javax.inject.Inject

import com.datastax.driver.core.{ BatchStatement, BoundStatement, Row }
import org.aj.cassandra._
import myapp.MyRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.{ ListBuffer, Map }

/*
case class MyRecord(uuid_1 UUID, key_2: String,
                    text_1: Option[String], text_2: Option[String],
                    date_1: Option[Date], date_2: Option[Date],
                    array_1: Option[Array[String]],
                    int_1: Option[Int])
*/

class MyTableOps @Inject() (myTableStatements: MyTableStatements) extends Field {

  //method to construct your application domain record from a database Row object
  private def get(row: Row): MyRecord = {
    MyRecord(
      uuid_1 = row.getUUID("uuid_1"),
      key_2 = row.getString("key_2"),
      text_1 = if (row.isNull("text_1")) None else Some(row.getString("text_1")),
      text_2 = if (row.isNull("text_1")) None else Some(row.getString("text_1")),
      date_1 = if (row.isNull("date_1")) None else Some(row.getTimestamp("date_1")),
      date_2 = if (row.isNull("date_2")) None else Some(row.getTimestamp("date_2")),
      array_1 = if (row.isNull("array_1")) None else Some(row.getSet("array_1", classOf[String]).asScala.toArray),
      int_1 = if (row.isNull("int_1")) None else Some(row.getInt("int_1"))
    )
  }
```

Read information from database using table key values. Method throws an exception if database is not available at the moment

```
  def read(uuid_1: UUID, key_2: String): Option[MyRecord] = {

    //get PreparedStatement for read
    myTableStatements.read match {
      case Some(r) => {

        //to collect data
        val records = new ListBuffer[MyRecord]()

        //set key
        val bs = new BoundStatement(r)
        bs.setUUID("uuid_1", uuid_1)
        bs.setString("key_2", key_2)

        //get records from database
        val future = myTableStatements.session match {
          case Some(session) => session.execute(bs)
          case None => throw new IllegalArgumentException("Cassandra Session is not available")
        }

        for (row <- future.getUninterruptibly().iterator().asScala)
          records += get(row)

        // a single record is expected
        if (records.length < 1) None else Some(records(0))
      }
      case None => throw new IllegalArgumentException("myTableStatements.read PreparedStatement is None")
    }
  }
```

Update / insert database record. Method throws an exception if database is not available at the moment

```
  def submit(myRecord: MyRecord): MyRecord = {

    myTableStatements.insert match {

      case Some(i) => {

        val uuidsFields = Map("uuid_1" -> Some(myRecord.uuid_1))

        val stringFields = Map[String, Option[String]]()

        //key
        collectData[String]("key_2", stringFields, Some(myRecord.key_2))
        //data
        collectData[String]("text_1", stringFields, myRecord.text_1)
        collectData[String]("text_2", stringFields, myRecord.text_2)

        val dateFields = Map("date_1" -> Some(new Date()), "date_2" -> myRecord.date_2)

        val setFields = myRecord.array_1 match {
          case Some(array) => Map("array_1" -> Some(array.toSet))
          case _ => Map("array_1" -> Some(Set.empty[String]))
        }

        val intFields = Map("int_1" -> myRecord.int_1)

        val fields = List(
          new BindStrings(stringFields.toMap),
          new BindUUIDs(uuidsFields.toMap),
          new BindDates(dateFields.toMap),
          new BindSets(setFields.toMap)
          new BindInts(intFields.toMap)
        )

        //create BoundStatement, and bind collected data to it
        val bs = new BoundStatement(i)
        fields.foreach((obj) => obj.to(bs))

        val batch = new BatchStatement()
        batch.add(bs)

        myTableStatements.session match {
          case Some(session) => session.execute(batch)
          case None => throw new IllegalArgumentException("Cassandra session is not available")
        }

        //return just submitted record
        myRecord
      }
      case None => throw new IllegalArgumentException(s"myTableStatements.insert PreparedStatement is None")
    }

  }
```

Delete information from database using table key values. Method throws an exception if database is not available at the moment

```
  def delete(uuid_1: UUID, key_2: String): Unit = {

    myTableStatements.delete match {
      case Some(d) => {

        val bs = new BoundStatement(d)
        bs.setUUID("uuid_1", uuid_1)
        bs.setString("key_2", key_2)

        val batch = new BatchStatement()
        batch.add(bs)

        myTableStatements.session match {
          case Some(session) => session.execute(batch)
          case None => throw new IllegalArgumentException("Cassandra Session is not available")
        }

        //Unit
        ()

      }
      case None => throw new IllegalArgumentException("myTableStatements.delete PreparedStatement is None")
    }
  }

}
```

## Conclusion

Presented set of API(s) helps to implement following

 * Singleton responsible for Cassandra database connectivity, Session(s) maintance
 * Singleton on per table basis, which provides access to your table key space Session, and PreparedStatement(s) for simple DML operations
 * A set of Traits, helper Classes to collect / bind fields values while reading, or submitting the data

## Author & license

If you have any questions regarding this project contact:
Andrei <ajmnsk@gmail.com>
For licensing info see LICENSE file in project's root directory.

