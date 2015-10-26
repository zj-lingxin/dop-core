package com.asto.dop.core.module.manage

import com.asto.dop.core.entity.SourceFlagEntity
import com.asto.dop.core.helper.Page
import com.ecfront.common.Resp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * 来源管理
 */
object SourceProcessor {

  def save(source: SourceFlagEntity): Future[Resp[Void]] = {
    SourceFlagEntity.db.save(source)
  }

  def update(req: Map[String, String], source: SourceFlagEntity): Future[Resp[Void]] = {
    if (!req.contains("id")) {
      Future(Resp.badRequest("【id】不能为空"))
    } else {
      val id = req("id")
      SourceFlagEntity.db.update(id, source)
    }
  }

  def get(req: Map[String, String]): Future[Resp[SourceFlagEntity]] = {
    if (!req.contains("id")) {
      Future(Resp.badRequest("【id】不能为空"))
    } else {
      val id = req("id")
      SourceFlagEntity.db.get(id)
    }
  }

  def find(req: Map[String, String]): Future[Resp[List[SourceFlagEntity]]] = {
    SourceFlagEntity.db.find()
  }

  def page(req: Map[String, String]): Future[Resp[Page[SourceFlagEntity]]] = {
    if (!req.contains("pageNumber")) {
      Future(Resp.badRequest("【pageNumber】不能为空"))
    } else {
      val pageNumber = req("pageNumber").toLong
      val pageSize = req.getOrElse("pageSize", "15").toInt
      SourceFlagEntity.db.page("1=1", List(), pageNumber, pageSize)
    }
  }

  def delete(req: Map[String, String]): Future[Resp[Void]] = {
    if (!req.contains("id")) {
      Future(Resp.badRequest("【id】不能为空"))
    } else {
      val id = req("id")
      SourceFlagEntity.db.delete(id)
    }
  }

}
