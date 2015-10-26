package com.asto.dop.core.module

import com.asto.dop.core.module.collect.VisitEntitySaveEvent
import com.asto.dop.core.module.query.VisitEntitySaveSubscriber

/**
 * 事件总线
 *
 * 用于不同业务大类互访时的解耦
 *
 * 这是一个发布订阅模型的实现，
 * 在业务A中定义一个Publisher用于发布一个特定的事件，
 * 在业务B中实现一个Subscriber用于订阅指定的事件，
 * 业务A发起一个事件，业务B收到对应的事件
 *
 */
object EventBus {

  def init(): Unit = {}

  /**
   * 访问实体保存事件
   */
  val visitEntitySaveEvent = new VisitEntitySaveEvent

  EventBus.visitEntitySaveEvent.subscribe(VisitEntitySaveSubscriber)

}
