package com.asto.dop.core.helper

import java.util.concurrent.atomic.AtomicLong

import com.ecfront.common.{JsonHelper, Resp}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, SQLConnection, UpdateResult}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/**
 * DB 异步操作处理辅助类
 *
 * 包含了DB 的几类基础操作
 */
object DBHelper extends LazyLogging {

  var dbClient: JDBCClient = _

  def update(sql: String, parameters: List[Any] = null): Future[Resp[Void]] = {
    val p = Promise[Resp[Void]]()
    db.onComplete {
      case Success(conn) =>
        try {
          if (parameters == null) {
            conn.update(sql,
              new Handler[AsyncResult[UpdateResult]] {
                override def handle(event: AsyncResult[UpdateResult]): Unit = {
                  if (event.succeeded()) {
                    conn.close()
                    p.success(Resp.success(null))
                  } else {
                    conn.close()
                    logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                    p.success(Resp.serverError(event.cause().getMessage))
                  }
                }
              }
            )
          } else {
            conn.updateWithParams(sql,
              new JsonArray(parameters.toList),
              new Handler[AsyncResult[UpdateResult]] {
                override def handle(event: AsyncResult[UpdateResult]): Unit = {
                  if (event.succeeded()) {
                    conn.close()
                    p.success(Resp.success(null))
                  } else {
                    conn.close()
                    logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                    p.success(Resp.serverError(event.cause().getMessage))
                  }
                }
              }
            )
          }
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql [$parameters]", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) =>
        p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  def batch(sql: String, parameterList: List[List[Any]] = null): Future[Resp[Void]] = {
    val p = Promise[Resp[Void]]()
    db.onComplete {
      case Success(conn) =>
        try {
          val counter = new AtomicLong(parameterList.length)
          parameterList.foreach {
            parameters =>
              if (parameters == null) {
                conn.update(sql,
                  new Handler[AsyncResult[UpdateResult]] {
                    override def handle(event: AsyncResult[UpdateResult]): Unit = {
                      if (!event.succeeded()) {
                        logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                      }
                      if (counter.decrementAndGet() == 0) {
                        conn.close()
                        p.success(Resp.success(null))
                      }
                    }
                  }
                )
              } else {
                conn.updateWithParams(sql,
                  new JsonArray(parameters),
                  new Handler[AsyncResult[UpdateResult]] {
                    override def handle(event: AsyncResult[UpdateResult]): Unit = {
                      if (!event.succeeded()) {
                        logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                      }
                      if (counter.decrementAndGet() == 0) {
                        conn.close()
                        p.success(Resp.success(null))
                      }
                    }
                  }
                )
              }
          }
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) =>
        p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  def get[E](sql: String, parameters: List[Any], resultClass: Class[E] = classOf[JsonObject]): Future[Resp[E]] = {
    val p = Promise[Resp[E]]()
    db.onComplete {
      case Success(conn) =>
        try {
          conn.queryWithParams(sql,
            new JsonArray(parameters.toList),
            new Handler[AsyncResult[ResultSet]] {
              override def handle(event: AsyncResult[ResultSet]): Unit = {
                if (event.succeeded()) {
                  val row = if (event.result().getNumRows == 1) {
                    event.result().getRows.get(0)
                  } else {
                    null
                  }
                  if (row != null) {
                    if (resultClass != classOf[JsonObject]) {
                      val result = Resp.success(JsonHelper.toObject(row.encode(), resultClass))
                      conn.close()
                      p.success(result)
                    } else {
                      val result = Resp.success(row.asInstanceOf[E])
                      conn.close()
                      p.success(result)
                    }
                  } else {
                    conn.close()
                    p.success(Resp.success(null.asInstanceOf[E]))
                  }
                } else {
                  conn.close()
                  logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                  p.success(Resp.serverError(event.cause().getMessage))
                }
              }
            }
          )
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql [$parameters]", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) => p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  def find[E](sql: String, parameters: List[Any], resultClass: Class[E] = classOf[JsonObject]): Future[Resp[List[E]]] = {
    val p = Promise[Resp[List[E]]]()
    db.onComplete {
      case Success(conn) =>
        try {
          conn.queryWithParams(sql,
            new JsonArray(parameters.toList),
            new Handler[AsyncResult[ResultSet]] {
              override def handle(event: AsyncResult[ResultSet]): Unit = {
                if (event.succeeded()) {
                  val rows = event.result().getRows.toList
                  if (resultClass != classOf[JsonObject]) {
                    val result = Resp.success(rows.map {
                      row =>
                        JsonHelper.toObject(row.encode(), resultClass)
                    })
                    conn.close()
                    p.success(result)
                  } else {
                    val result = Resp.success(rows.asInstanceOf[List[E]])
                    conn.close()
                    p.success(result)
                  }
                } else {
                  conn.close()
                  logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                  p.success(Resp.serverError(event.cause().getMessage))
                }
              }
            }
          )
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql [$parameters]", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) => p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  def page[E](sql: String, parameters: List[Any], pageNumber: Long = 1, pageSize: Int = 10, resultClass: Class[E] = classOf[JsonObject]): Future[Resp[Page[E]]] = {
    val p = Promise[Resp[Page[E]]]()
    db.onComplete {
      case Success(conn) =>
        try {
          countInner(sql, parameters).onSuccess {
            case countResp =>
              if (countResp) {
                val page = new Page[E]
                page.pageNumber = pageNumber
                page.pageSize = pageSize
                page.recordTotal = countResp.body
                page.pageTotal = (page.recordTotal + pageSize - 1) / pageSize
                val limitSql = s"$sql limit ${(pageNumber - 1) * pageSize} ,$pageSize"
                conn.queryWithParams(limitSql,
                  new JsonArray(parameters.toList),
                  new Handler[AsyncResult[ResultSet]] {
                    override def handle(event: AsyncResult[ResultSet]): Unit = {
                      if (event.succeeded()) {
                        val rows = event.result().getRows.toList
                        if (resultClass != classOf[JsonObject]) {
                          page.objects = rows.map {
                            row =>
                              JsonHelper.toObject(row.encode(), resultClass)
                          }
                        } else {
                          page.objects = rows.asInstanceOf[List[E]]
                        }
                        conn.close()
                        p.success(Resp.success(page))
                      } else {
                        conn.close()
                        logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                        p.success(Resp.serverError(event.cause().getMessage))
                      }
                    }
                  }
                )
              } else {
                conn.close()
                p.success(countResp)
              }
          }
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql [$parameters]", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) => p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  def count(sql: String, parameters: List[Any]): Future[Resp[Long]] = {
    val p = Promise[Resp[Long]]()
    db.onComplete {
      case Success(conn) =>
        try {
          conn.queryWithParams(sql,
            new JsonArray(parameters.toList),
            new Handler[AsyncResult[ResultSet]] {
              override def handle(event: AsyncResult[ResultSet]): Unit = {
                if (event.succeeded()) {
                  val result = Resp.success[Long](event.result().getResults.get(0).getLong(0))
                  conn.close()
                  p.success(result)
                } else {
                  conn.close()
                  logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                  p.success(Resp.serverError(event.cause().getMessage))
                }
              }
            }
          )
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql [$parameters]", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) => p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  //此方法仅为分页请求提供
  private def countInner(sql: String, parameters: List[Any]): Future[Resp[Long]] = {
    val p = Promise[Resp[Long]]()
    db.onComplete {
      case Success(conn) =>
        val countSql = s"SELECT COUNT(1) FROM ( $sql ) _${System.currentTimeMillis()}"
        conn.queryWithParams(countSql,
          new JsonArray(parameters.toList),
          new Handler[AsyncResult[ResultSet]] {
            override def handle(event: AsyncResult[ResultSet]): Unit = {
              if (event.succeeded()) {
                val result = Resp.success[Long](event.result().getResults.get(0).getLong(0))
                conn.close()
                p.success(result)
              } else {
                logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                conn.close()
                p.success(Resp.serverError(event.cause().getMessage))
              }
            }
          }
        )
      case Failure(ex) => p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  def exist(sql: String, parameters: List[Any]): Future[Resp[Boolean]] = {
    val p = Promise[Resp[Boolean]]()
    db.onComplete {
      case Success(conn) =>
        try {
          conn.queryWithParams(sql,
            new JsonArray(parameters.toList),
            new Handler[AsyncResult[ResultSet]] {
              override def handle(event: AsyncResult[ResultSet]): Unit = {
                if (event.succeeded()) {
                  if (event.result().getNumRows > 0) {
                    conn.close()
                    p.success(Resp.success(true))
                  } else {
                    conn.close()
                    p.success(Resp.success(false))
                  }
                } else {
                  conn.close()
                  logger.debug(s"DB execute error : $sql [$parameters]", event.cause())
                  p.success(Resp.serverError(event.cause().getMessage))
                }
              }
            }
          )
        } catch {
          case ex: Throwable =>
            conn.close()
            logger.error(s"DB execute error : $sql [$parameters]", ex)
            p.success(Resp.serverError(ex.getMessage))
        }
      case Failure(ex) => p.success(Resp.serverUnavailable(ex.getMessage))
    }
    p.future
  }

  private def db: Future[SQLConnection] = {
    val p = Promise[SQLConnection]()
    dbClient.getConnection(new Handler[AsyncResult[SQLConnection]] {
      override def handle(conn: AsyncResult[SQLConnection]): Unit = {
        if (conn.succeeded()) {
          p.success(conn.result())
        } else {
          logger.error("DB connecting fail .", conn.cause())
          p.failure(conn.cause())
        }
      }
    })
    p.future
  }
}

class Page[E] {
  //start with 1
  var pageNumber: Long = _
  var pageSize: Int = _
  var pageTotal: Long = _
  var recordTotal: Long = _
  var objects: List[E] = _

}
