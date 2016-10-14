package org.aj.cassandra

/**
 * Type to built Sql(s)
 */
trait Sql {

  /**
   * Builds sql for insert operation.
   *
   * @param table name of the table to insert record into.
   * @param fields fields to populate.
   * @return built sql.
   */
  def insert(table: String, fields: Set[String]): String

  /**
   * Builds sql for select operation.
   *
   * @param table name of the table to insert record into.
   * @param fields fields to select.
   * @param keys a Set of key fields to use for selection, None if no keys provided.
   * @return built sql.
   */
  def select(table: String, fields: Set[String], keys: Option[Set[String]] = None): String

  /**
   * Builds sql for select distinct operation.
   *
   * @param table name of the table to insert record into.
   * @param fields fields to select.
   * @param keys a Set of key fields to use for selection, None if no keys provided.
   * @return built sql.
   */
  def selectDistinct(table: String, fields: Set[String], keys: Option[Set[String]] = None): String

  /**
   * Builds sql to delete fields / record from a table by key(s)
   *
   * @param table table name to delete data from.
   * @param fields a list of fields to delete from a table for a particular key, if None, delete the whole record.
   * @param keys Set of key fields to use.
   * @return built sql.
   */
  def delete(table: String, keys: Set[String], fields: Option[Set[String]] = None): String

}
