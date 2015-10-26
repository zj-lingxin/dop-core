package com.asto.dop.core.function

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener
import com.github.shyiko.mysql.binlog.event._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.FunSuite

/**
  * MySQL BinLog Monitor （Test）
  */
class BinLogSpec extends FunSuite with LazyLogging {

  /** *
    * Mapping tableId to tableName，key = tableId , value = tableName
    */
  private val tableMap = collection.mutable.Map[Long, String]()

  private val (host, port, userName, userPwd, filterTable) = ("192.168.99.100", 3306, "root", "123456", "test")

  test("Data Change Filter by Table") {
    val client = new BinaryLogClient(host, port, userName, userPwd)
    client.registerEventListener(new EventListener() {
      override def onEvent(event: Event): Unit = {
        // logger.trace("Receive a event : " + event.toString)
        event.getData[EventData] match {
          case e: TableMapEventData =>
            //TODO called before each change data , Why  ?
            logger.trace("Captures table map." + e.toString)
            val tableInfo = e.asInstanceOf[TableMapEventData]
            if (tableInfo.getTable == filterTable) {
              tableMap += tableInfo.getTableId -> tableInfo.getTable
            }
          case e if
          e.isInstanceOf[WriteRowsEventData] && tableMap.contains(e.asInstanceOf[WriteRowsEventData].getTableId) ||
            e.isInstanceOf[UpdateRowsEventData] && tableMap.contains(e.asInstanceOf[UpdateRowsEventData].getTableId) ||
            e.isInstanceOf[DeleteRowsEventData] && tableMap.contains(e.asInstanceOf[DeleteRowsEventData].getTableId)
          =>
            logger.debug("Captures the specified change." + e.toString)
          //Do something.
          case _ =>
        }
      }
    })
    client.connect()
  }

}
