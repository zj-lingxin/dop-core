package com.asto.dop.core.module.query

import com.asto.dop.core.entity.VisitEntity
import com.asto.dop.core.helper.{WSHelper, WSReq}

import scala.collection.mutable

/**
 * 访问实体保存事件订阅器
 */
object VisitEntitySaveSubscriber extends mutable.Subscriber[VisitEntity, mutable.Publisher[VisitEntity]] {
  override def notify(pub: mutable.Publisher[VisitEntity], event: VisitEntity): Unit = {
    //这只是个例子
    WSHelper.ws(WSReq("visitEntitySave", event))
  }
}
