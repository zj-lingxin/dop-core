package com.asto.dop.core.entity

import com.asto.dop.core.helper.{DBHelper, Page}
import com.ecfront.common.Resp

import scala.concurrent.Future

/**
 * 用户操作实体，API请求会持久化到此对象
 */
case class UserOptEntity() {
  // | Y | 记录主键，格式`action+传入的主键id`
  var id: String = _
  // | Y | 发生时间，格式`yyyyMMddHHmmss`,如 20151012100000
  var occur_time: Long = _
  // | Y | 发生日期，格式`yyyyMMdd`,如 20151012
  var occur_date: Long = _
  // | Y | 发生年月，格式`yyyyMM`,如 201510
  var occur_month: Long = _
  // | Y | 发生年份，格式`yyyyMMdd`,如 2015
  var occur_year: Long = _
  // | Y | 用户id，要求是真实用户表的主键或可以与之映射，未登录用户此字段为空
  var user_id: String = _
  //| Y | 客户端使用的平台，枚举：`pc/mobile` ，除pc以外的设备都归属mobile |
  var platform: String = _
  // | Y | 访问转入来源
  var source: String = _
  // | Y | 操作行为，用于记录此访问的业务含义，枚举：`register/bind/apply/self_examine_pass/bank_examine_pass`
  var action: String = _
  // | Y | 金额，register时为0，单位厘
  var amount: Long = _
}


object UserOptEntity {

  val FLAG_REGISTER = "register"
  val FLAG_APPLY = "apply"
  val FLAG_BIND = "bind"
  val FLAG_SELF_EXAMINE_PASS = "self_examine_pass"
  val FLAG_BANK_EXAMINE_PASS = "bank_examine_pass"

  object db {

    val TABLE_NAME = "user_opt"

    def init(): Future[Resp[Void]] = {
      DBHelper.update {
        s"""CREATE TABLE IF NOT EXISTS $TABLE_NAME
            |(
            |    id varchar(150) NOT NULL COMMENT '记录主键，格式`action+传入的主键id`' ,
            |    occur_time BIGINT NOT NULL COMMENT '发生时间，格式`yyyyMMddHHmmss`,如 20151012100000' ,
            |    occur_date INT NOT NULL COMMENT '发生日期，格式`yyyyMMdd`,如 20151012' ,
            |    occur_month INT NOT NULL COMMENT '发生年月，格式`yyyyMM`,如 201510' ,
            |    occur_year INT NOT NULL COMMENT '发生年份，格式`yyyyMMdd`,如 2015' ,
            |    user_id varchar(255) NOT NULL COMMENT '用户id，要求是真实用户表的主键或可以与之映射，未登录用户此字段为空' ,
            |    platform varchar(10) NOT NULL COMMENT '客户端使用的平台，枚举：`pc/mobile` ，除pc以外的设备都归属mobile' ,
            |    source varchar(255) NOT NULL COMMENT '访问转入来源' ,
            |    action varchar(200) NOT NULL COMMENT '访问行为，用于记录此访问的业务含义，如：绑店、申请xx等' ,
            |    amount BIGINT NOT NULL COMMENT '金额，register时为0，单位厘' ,
            |    PRIMARY KEY(id) ,
            |    INDEX i_occur_time(occur_time) ,
            |    INDEX i_occur_date(occur_date) ,
            |    INDEX i_occur_month(occur_month) ,
            |    INDEX i_occur_year(occur_year) ,
            |    INDEX i_user_id(user_id) ,
            |    INDEX i_platform(platform) ,
            |    INDEX i_source(source) ,
            |    INDEX i_action(action) ,
            |    INDEX i_amount(amount)
            |)ENGINE=innodb DEFAULT CHARSET=utf8
            | """.stripMargin
      }
    }

    def save(obj: UserOptEntity): Future[Resp[Void]] = {
      DBHelper.update(
        s"""
           |INSERT INTO $TABLE_NAME ( id , occur_time ,occur_date , occur_month,occur_year,user_id ,platform,source,action ,amount )
           | SELECT ? , ? ,  ? , ? , ? , ? , ? , ? , ? , ? FROM DUAL WHERE NOT EXISTS( SELECT 1 FROM $TABLE_NAME WHERE id = ?	)
        """.stripMargin,
        List(
          obj.id, obj.occur_time, obj.occur_date, obj.occur_month, obj.occur_year, obj.user_id, obj.platform, obj.source, obj.action, obj.amount, obj.id
        )
      )
    }

    def update(newValues: String, condition: String, parameters: List[Any]): Future[Resp[Void]] = {
      DBHelper.update(
        s"UPDATE $TABLE_NAME Set $newValues WHERE $condition",
        parameters
      )
    }

    def delete(id: String): Future[Resp[Void]] = {
      DBHelper.update(
        s"DELETE FROM $TABLE_NAME WHERE id = ? ",
        List(id)
      )
    }

    def delete(condition: String, parameters: List[Any]): Future[Resp[Void]] = {
      DBHelper.update(
        s"DELETE FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def get(id: String): Future[Resp[UserOptEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE id = ? ",
        List(id),
        classOf[UserOptEntity]
      )
    }

    def get(condition: String, parameters: List[Any]): Future[Resp[UserOptEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[UserOptEntity]
      )
    }

    def exist(condition: String, parameters: List[Any]): Future[Resp[Boolean]] = {
      DBHelper.exist(
        s"SELECT 1 FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def find(condition: String = " 1=1 ", parameters: List[Any] = List()): Future[Resp[List[UserOptEntity]]] = {
      DBHelper.find(
        s"SELECT * FROM $TABLE_NAME WHERE  $condition ",
        parameters,
        classOf[UserOptEntity]
      )
    }

    def page(condition: String = " 1=1 ", parameters: List[Any] = List(), pageNumber: Long = 1, pageSize: Int = 10): Future[Resp[Page[UserOptEntity]]] = {
      DBHelper.page(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        pageNumber, pageSize,
        classOf[UserOptEntity]
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


