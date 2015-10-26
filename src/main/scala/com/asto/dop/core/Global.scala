package com.asto.dop.core

import com.asto.dop.core.entity.UserOptEntity
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

/**
 * 全局数据存储类
 */
object Global extends LazyLogging {

  var vertx:Vertx=_
  //config.json数据
  var config: JsonObject = _

  lazy val visitRecomputeAction = config.getString("visitRecomputeAction")
  lazy val visitRegisterAction = config.getString("visitRegisterAction")

  private lazy val businessApi = config.getJsonObject("businessApi")
  lazy val businessApi_apply = (businessApi.getJsonObject(UserOptEntity.FLAG_APPLY).getString("table"), businessApi.getJsonObject(UserOptEntity.FLAG_APPLY).getString("api"))
  lazy val businessApi_bind = (businessApi.getJsonObject(UserOptEntity.FLAG_BIND).getString("table"), businessApi.getJsonObject(UserOptEntity.FLAG_BIND).getString("api"))
  lazy val businessApi_selfExaminePass = (businessApi.getJsonObject(UserOptEntity.FLAG_SELF_EXAMINE_PASS).getString("table"), businessApi.getJsonObject(UserOptEntity.FLAG_SELF_EXAMINE_PASS).getString("api"))
  lazy val businessApi_bankExaminePass = (businessApi.getJsonObject(UserOptEntity.FLAG_BANK_EXAMINE_PASS).getString("table"), businessApi.getJsonObject(UserOptEntity.FLAG_BANK_EXAMINE_PASS).getString("api"))

}
