package com.asto.dop.core.entity

import com.asto.dop.core.helper.{DBHelper, Page}
import com.ecfront.common.{BeanHelper, Resp}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.beans.BeanProperty
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * 源标识解析实体，保存了源标识与其业务含义的映射关系
 */
case class SourceFlagEntity() {
  // | Y | 记录主键，自增
  @BeanProperty var id: Long = _
  // | Y | 标识
  @BeanProperty var flag: String = _
  // | Y | 渠道分类（公司）
  @BeanProperty var source_type: String = _
  // | Y | 渠道名称
  @BeanProperty var source: String = _
  // | Y | 链接目的页（落地页/活动）
  @BeanProperty var link_page: String = _
  // | Y | 链接类型 页面/活动
  @BeanProperty var link_type: String = _
  // | Y | 专属链接
  @BeanProperty var link_url: String = _
}

object SourceFlagEntity extends LazyLogging {

  object db {

    private val TABLE_NAME = "source_flag"

    def init(): Unit = {
      DBHelper.update {
        s"""CREATE TABLE IF NOT EXISTS $TABLE_NAME
            |(
            |    id INT NOT NULL AUTO_INCREMENT COMMENT '记录主键' ,
            |    flag varchar(100) NOT NULL COMMENT '标识' ,
            |    source_type varchar(100) NOT NULL COMMENT '渠道分类' ,
            |    source varchar(100) NOT NULL COMMENT '渠道名称' ,
            |    link_page varchar(100) NOT NULL COMMENT '链接目的页' ,
            |    link_type varchar(100) NOT NULL COMMENT '链接类型' ,
            |    link_url varchar(100) NOT NULL COMMENT '专属链接' ,
            |    PRIMARY KEY(id) ,
            |    INDEX i_flag(flag),
            |    INDEX i_source_type(source_type),
            |    INDEX i_source(source),
            |    INDEX i_link_page(link_page),
            |    INDEX i_link_type(link_type)
            |)ENGINE=innodb DEFAULT CHARSET=utf8
            | """.stripMargin
      }
    }

    def save(obj: SourceFlagEntity): Future[Resp[Void]] = {
      DBHelper.update(
        s"""
           |INSERT INTO $TABLE_NAME ( id , flag ,source_type, source ,link_page ,link_type ,link_url)
           |SELECT ? , ? , ? , ? , ? , ? , ? FROM DUAL WHERE NOT EXISTS( SELECT 1 FROM $TABLE_NAME WHERE id = ?	)
        """.stripMargin,
        List(
          obj.id, obj.flag, obj.source_type, obj.source, obj.link_page, obj.link_type, obj.link_url, obj.id
        )
      )
    }

    def update(id: String, obj: SourceFlagEntity): Future[Resp[Void]] = {
      val p = Promise[Resp[Void]]()
      get(id).onSuccess {
        case oldObjResp =>
          BeanHelper.copyProperties(oldObjResp.body, obj)
          DBHelper.update(
            s"UPDATE $TABLE_NAME Set flag =? ,source_type = ? ,source =? ,link_page =? ,link_type =? ,link_url =?  WHERE id = ? ",
            List(oldObjResp.body.flag, oldObjResp.body.source_type, oldObjResp.body.source, oldObjResp.body.link_page, oldObjResp.body.link_type, oldObjResp.body.link_url, id)
          ).onSuccess {
            case updateResp =>
              if (updateResp) {
                p.success(Resp.success(null))
              } else {
                p.success(updateResp)
              }
          }
      }
      p.future
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

    def get(id: String): Future[Resp[SourceFlagEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE id = ? ",
        List(id),
        classOf[SourceFlagEntity]
      )
    }

    def get(condition: String, parameters: List[Any]): Future[Resp[SourceFlagEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[SourceFlagEntity]
      )
    }

    def exist(condition: String, parameters: List[Any]): Future[Resp[Boolean]] = {
      DBHelper.exist(
        s"SELECT 1 FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def find(condition: String = " 1=1 ", parameters: List[Any] = List()): Future[Resp[List[SourceFlagEntity]]] = {
      DBHelper.find(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[SourceFlagEntity]
      )
    }

    def page(condition: String = " 1=1 ", parameters: List[Any] = List(), pageNumber: Long = 1, pageSize: Int = 10): Future[Resp[Page[SourceFlagEntity]]] = {
      DBHelper.page(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        pageNumber, pageSize,
        classOf[SourceFlagEntity]
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







