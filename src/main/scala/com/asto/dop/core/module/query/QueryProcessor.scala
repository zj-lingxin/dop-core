package com.asto.dop.core.module.query

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ecfront.common.Resp
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Promise}

/**
 * 查询处理器父类
 */
trait QueryProcessor extends LazyLogging {

  protected val df = new SimpleDateFormat("yyyyMMddHHmmss")
  protected val dfd = new SimpleDateFormat("yyyyMMdd")
  protected def process(req: Map[String, String], p: Promise[Resp[Any]])

  def process(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]()
    process(req, p)
    p.future
  }

  protected def dateOffset(offsetDays: Int, currentDate: Date = new Date()): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTime(currentDate)
    calendar.add(Calendar.DATE, offsetDays)
    dfd.format(calendar.getTime).toLong
  }

  protected def getDateRange(start: Long, end: Long): List[Long] = {
    val endDate = dfd.parse(end + "")
    val calendar = Calendar.getInstance()
    calendar.setTime(dfd.parse(start + ""))
    val result = ArrayBuffer[Long]()
    while (calendar.getTime.getTime <= endDate.getTime) {
      result += dfd.format(calendar.getTime).toLong
      calendar.add(Calendar.DATE,1)
    }
    result.toList
  }

}
