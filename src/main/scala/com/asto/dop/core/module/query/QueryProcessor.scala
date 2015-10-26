package com.asto.dop.core.module.query

import java.text.SimpleDateFormat

import com.ecfront.common.Resp
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.concurrent.{Future, Promise}

/**
 * 查询处理器父类
 */
trait QueryProcessor extends LazyLogging {

  protected val df = new SimpleDateFormat("yyyyMMddHHmmss")

  protected def process(req: Map[String, String], p: Promise[Resp[Any]])

  def process(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]()
    process(req, p)
    p.future
  }

}
