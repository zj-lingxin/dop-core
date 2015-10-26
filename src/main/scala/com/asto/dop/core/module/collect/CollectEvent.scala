package com.asto.dop.core.module.collect

import com.asto.dop.core.entity.VisitEntity

import scala.collection.mutable


/**
 * 访问实体保存事件
 */
class VisitEntitySaveEvent extends mutable.Publisher[VisitEntity]{
  def pub(entity:VisitEntity) = publish(entity)
}
