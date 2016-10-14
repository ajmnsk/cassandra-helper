package org.aj.cassandra

import com.datastax.driver.core.{Cluster, Session}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Right

/**
 * This abstract class provides functionality to
 *
 *  1. connect to Cassandra cluster
 *  2. close Cassandra cluster connection
 *  3. create / close Session
 *  4. keep a Map of open Sessions to be used
 *
 * It has functionality to try reconnect to the cluster if connection found lost
 * Class is intended to be extended by Singleton type of object
 *
 */
abstract class Connect(
  val hosts: Set[String],
  val port: Int,
  val keySpaces: Set[String]
) {

  require(hosts.size > 0)
  require(keySpaces.size > 0)

  private var cluster: Option[Cluster] = None
  private val sessions: mutable.Map[String, Session] = mutable.Map.empty[String, Session]

  /**
   * Method tries to get a reference to a Session object by a key space name
   * It is assumed that Cluster connection, and Sessions are already created
   * If Session is not available, it may try to re-connect by specifying connectRetry parameter
   * connectRetry is defaulted to 1, to try re-connect at least once
   *
   * @param keySpace
   * @param connectRetry
   * @return
   */
  def session(keySpace: String)(implicit connectRetry: Int): Option[Session] = {

    @tailrec
    def getSession(keySpace: String, tries: Int): Option[Session] = {

      if (tries > 0) connect()

      sessions.get(keySpace) match {
        case None => {
          if (tries >= connectRetry) None
          else getSession(keySpace, tries + 1)
        }
        case session => session
      }

    }

    getSession(keySpace, 0)

  }

  /**
   * Connect to Cassandra Cluster, and create Sessions to a specified list of key spaces
   *
   */
  protected def connect(): Either[String, Boolean] = {

    try {

      close()

      cluster = Some(Cluster.builder().addContactPoints(hosts.toList: _*).withPort(port).build)

      cluster match {
        case Some(c) => {
          keySpaces foreach (ks => sessions += (ks -> c.connect(ks)))
          //created and collected sessions with no issues
          Right(true)
        }
        case _ => throw new IllegalArgumentException(s"Failed to build cluster connection to: ${hosts}, ${port}")
      }

    } catch {
      case e: Throwable => Left(e.getMessage)
    }

  }

  /**
   * Close database sessions if open, and remove their references from the Map
   *
   * @return
   */
  private def closeSessions() = {
    val kss = sessions map { r =>
      if (!r._2.isClosed) r._2.close()
      r._1
    }
    sessions --= kss.toList
  }

  /**
   * Close database resources if open
   */
  protected def close() = {
    //close sessions first
    closeSessions()
    //close cluster connection
    cluster match {
      case Some(c) => if (!c.isClosed()) c.close()
      case _ => None
    }
  }

}
