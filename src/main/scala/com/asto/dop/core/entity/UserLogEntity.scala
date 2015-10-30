package com.asto.dop.core.entity

import com.asto.dop.core.helper.{DBHelper, Page}
import com.ecfront.common.Resp
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

/**
 * 用户操作日志实体，用于记录UserOptEntity记录最后一次更新时间
 */
case class UserLogEntity(
                      // | Y | 记录主键，是操作行为，用于记录此访问的业务含义，枚举：`bind/apply/self_examine_pass/bank_examine_pass`
                      var action: String,
                      // | Y | 最后一次更新时间，格式`yyyyMMddHHmmss`,如 20151012100000
                      var last_update_time: String
                        )

object UserLogEntity extends LazyLogging {

  object db {

    private val TABLE_NAME = "user_log"

    def init(): Unit = {
      DBHelper.update {
        s"""CREATE TABLE IF NOT EXISTS $TABLE_NAME
            |(
            |    action varchar(200) NOT NULL COMMENT '记录主键，是操作行为，用于记录此访问的业务含义，枚举：`register/apply/self_examine_pass/bank_examine_pass`' ,
            |    last_update_time varchar(14) NOT NULL COMMENT '最后一次更新时间，格式`yyyyMMddHHmmss`,如 20151012100000' ,
            |    PRIMARY KEY(action) ,
            |    INDEX i_last_update_time(last_update_time)
            |)ENGINE=innodb DEFAULT CHARSET=utf8
            | """.stripMargin
      }.onSuccess {
        case initResp =>
          if (initResp) {
            save(UserLogEntity(UserOptEntity.FLAG_APPLY, "20151001000000"))
            save(UserLogEntity(UserOptEntity.FLAG_BIND, "20151001000000"))
            save(UserLogEntity(UserOptEntity.FLAG_SELF_EXAMINE_PASS, "20151001000000"))
            save(UserLogEntity(UserOptEntity.FLAG_BANK_EXAMINE_PASS, "20151001000000"))
          }
      }
    }

    def save(obj: UserLogEntity): Future[Resp[Void]] = {
      DBHelper.update(
        s"""
           |INSERT INTO $TABLE_NAME ( action , last_update_time)
           |SELECT ? , ? FROM DUAL WHERE NOT EXISTS( SELECT 1 FROM $TABLE_NAME WHERE action = ?	)
        """.stripMargin,
        List(
          obj.action, obj.last_update_time,obj.action
        )
      )
    }

    def update(newValues: String, condition: String, parameters: List[Any]): Future[Resp[Void]] = {
      DBHelper.update(
        s"UPDATE $TABLE_NAME Set $newValues WHERE $condition",
        parameters
      )
    }

    def delete(action: String): Future[Resp[Void]] = {
      DBHelper.update(
        s"DELETE FROM $TABLE_NAME WHERE action = ? ",
        List(action)
      )
    }

    def delete(condition: String, parameters: List[Any]): Future[Resp[Void]] = {
      DBHelper.update(
        s"DELETE FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def get(action: String): Future[Resp[UserLogEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE action = ? ",
        List(action),
        classOf[UserLogEntity]
      )
    }

    def get(condition: String, parameters: List[Any]): Future[Resp[UserLogEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[UserLogEntity]
      )
    }

    def exist(condition: String, parameters: List[Any]): Future[Resp[Boolean]] = {
      DBHelper.exist(
        s"SELECT 1 FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def find(condition: String = " 1=1 ", parameters: List[Any] = List()): Future[Resp[List[UserLogEntity]]] = {
      DBHelper.find(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[UserLogEntity]
      )
    }

    def page(condition: String = " 1=1 ", parameters: List[Any] = List(), pageNumber: Long = 1, pageSize: Int = 10): Future[Resp[Page[UserLogEntity]]] = {
      DBHelper.page(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        pageNumber, pageSize,
        classOf[UserLogEntity]
      )
    }

    def count(condition: String = " 1=1 ", parameters: List[Any] = List()): Future[Resp[Long]] = {
      DBHelper.count(
        s"SELECT count(1) FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    init()

  }

}





