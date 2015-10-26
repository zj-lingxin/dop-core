package com.asto.dop.core

import com.asto.dop.core.module.collect.APIProcessor
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener
import com.github.shyiko.mysql.binlog.event._
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * Binlog监控路由分发器
 */
class BinLogRouter(monitorTables: Seq[String]) extends EventListener with LazyLogging {

  override def onEvent(event: Event): Unit = {
    event.getData[EventData] match {
      case e: TableMapEventData =>
        logger.trace("Captures table map." + e.toString)
        val tableInfo = e.asInstanceOf[TableMapEventData]
        if (monitorTables.contains(tableInfo.getTable)) {
          BinLogRouter.tableMap += tableInfo.getTableId -> tableInfo.getTable
        }
      case e: WriteRowsEventData if BinLogRouter.tableMap.contains(e.getTableId) =>
        logger.trace("Captures the specified change : " + e.toString)
        APIProcessor.process(BinLogRouter.tableMap(e.getTableId))
      case e: UpdateRowsEventData if BinLogRouter.tableMap.contains(e.getTableId) =>
        logger.trace("Captures the specified change : " + e.toString)
        APIProcessor.process(BinLogRouter.tableMap(e.getTableId))
      case e: DeleteRowsEventData if BinLogRouter.tableMap.contains(e.getTableId) =>
        logger.trace("Captures the specified change : " + e.toString)
        APIProcessor.process(BinLogRouter.tableMap(e.getTableId))
      case _ =>
    }
  }
}

object BinLogRouter {

  val tableMap = collection.mutable.Map[Long, String]()

}

