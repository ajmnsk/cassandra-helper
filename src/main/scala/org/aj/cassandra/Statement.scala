package org.aj.cassandra

import com.datastax.driver.core.{PreparedStatement, Session}

import scala.annotation.tailrec
import scala.collection.mutable

trait Statement {

  protected val keySpace: String
  protected val fields: Set[String]
  protected val keys: Set[String]
  protected val table: String
  protected val sqls: Set[String]
  protected implicit val connectRetry: Int

  private val statements: mutable.Map[String, Option[PreparedStatement]] = mutable.Map.empty[String, Option[PreparedStatement]]

  /**
   * Get prepared statement
   *
   * @param session database session
   * @param sql sql statement
   * @return prepared statement
   */
  private def prepareStatement(session: Option[Session], sql: String): Option[PreparedStatement] = {
    session match {
      case Some(s) => Some(s.prepare(sql));
      case None => None
    }
  }

  def session(): Option[Session]

  protected val getPreparedStatements: () => Either[String, Int] = () => prepareStatements(session())

  protected def prepareStatements(session: Option[Session]): Either[String, Int] = {

    try {
      session match {
        case None => throw new IllegalArgumentException("Session is not available")
        case session => {
          //clear
          statements --= sqls.toList
          //populate
          sqls foreach (sql => statements += (sql -> prepareStatement(session, sql)))
          //If success return the number of statements created
          Right(statements.size)
        }
      }

    } catch {
      case e: Throwable => {
        //clear
        statements --= sqls.toList
        //If fails return error message
        Left(e.getMessage)
      }
    }

  }

  protected def getStatement(sql: String): Option[PreparedStatement] = {

    @tailrec
    def statement(sql: String, tries: Int): Option[PreparedStatement] = {

      if (tries > 0) {
        getPreparedStatements()
      }
      statements.get(sql) match {
        case None => {
          if (tries >= connectRetry) None
          else statement(sql, tries + 1)
        }
        case Some(preparedStatement) => preparedStatement
      }
    }

    statement(sql, 0)
  }
}

trait Read {
  self: Statement =>
  protected val readSql: String
  def read(): Option[PreparedStatement] = {
    getStatement(readSql)
  }
}

trait Insert {
  self: Statement =>
  protected val insertSql: String
  def insert(): Option[PreparedStatement] = {
    getStatement(insertSql)
  }
}

trait Delete {
  self: Statement =>
  protected val deleteSql: String
  def delete(): Option[PreparedStatement] = {
    getStatement(deleteSql)
  }
}
