package org.aj.cassandra

import java.util.{Date, UUID}

import com.datastax.driver.core.BoundStatement

import scala.collection.JavaConverters._

/**
 * Trait to help set values to the fields
 *
 * @tparam T data type of the value for the field
 */
trait Bind[T] {
  val fieldsData: Map[String, T]
  def fields: Set[String] = fieldsData.keySet
  def to(bs: BoundStatement): BoundStatement
}

class BindDates(fd: Map[String, Option[Date]]) extends Bind[Option[Date]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setTimestamp(field._1, v)
        case None => bs.setToNull(field._1)
      })
    bs
  }
}

class BindDoubles(fd: Map[String, Option[Double]]) extends Bind[Option[Double]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setDouble(field._1, v)
        case None => bs.setToNull(field._1)
      })
    bs
  }
}

class BindInts(fd: Map[String, Option[Int]]) extends Bind[Option[Int]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setInt(field._1, v)
        case None => bs.setToNull(field._1)
      })
    bs
  }
}

class BindLongs(fd: Map[String, Option[Long]]) extends Bind[Option[Long]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setLong(field._1, v)
        case None => bs.setToNull(field._1)
      })
    bs
  }
}

class BindMap[K, V](fd: Map[String, Option[Map[K, V]]]) extends Bind[Option[Map[K, V]]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setMap(field._1, v.asJava)
        case None => bs.setMap(field._1, Map.empty[K, V].asJava)
      })
    bs
  }
}

class BindSets[T](fd: Map[String, Option[Set[T]]]) extends Bind[Option[Set[T]]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setSet(field._1, v.asJava)
        case None => bs.setSet(field._1, Set.empty[T].asJava)
      })
    bs
  }
}

class BindStrings(fd: Map[String, Option[String]]) extends Bind[Option[String]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setString(field._1, v)
        case None => bs.setToNull(field._1)
      })
    bs
  }
}

class BindUUIDs(fd: Map[String, Option[UUID]]) extends Bind[Option[UUID]] {
  override val fieldsData = fd
  override def to(bs: BoundStatement): BoundStatement = {
    fieldsData foreach ((field) =>
      field._2 match {
        case Some(v) => bs.setUUID(field._1, v)
        case None => bs.setToNull(field._1)
      })
    bs
  }
}
