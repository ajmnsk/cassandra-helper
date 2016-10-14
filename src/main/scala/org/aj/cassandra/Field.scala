package org.aj.cassandra

import scala.collection.mutable.Map

/**
 * Created by ajlnx on 6/9/16.
 *
 * Trait to provide Generic logic to collect Field data.
 *
 */
trait Field {

  /**
   * Method collects field data to be submitted to database
   *
   * @param fieldName database field name
   * @param fields Map to collect database fields data
   * @param value data for the field to collect
   */
  def collectData[T](fieldName: String, fields: Map[String, Option[T]], value: Option[T]): Unit = {
    value match {
      case Some(value) => fields += (fieldName -> Some(value))
      case _ => None
    }
  }

}
