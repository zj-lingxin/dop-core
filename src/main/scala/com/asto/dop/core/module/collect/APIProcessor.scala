package com.asto.dop.core.module.collect

import java.text.SimpleDateFormat
import java.util.Date

import com.asto.dop.core.Global
import com.asto.dop.core.entity.{UserLogEntity, UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.HttpHelper
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.{AsyncResult, Future, Handler}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

/**
 * API访问处理器
 *
 * 当收到Binlog事件时发起此流程，
 * 流程会访问事件对应的第三方业务系统接口取数并保存
 */
object APIProcessor extends LazyLogging {

  private val df = new SimpleDateFormat("yyyyMMddHHmmss")

  def process(tableName: String): Unit = {
    if (Global.businessApi_apply._1 == tableName) {
      processApply(UserOptEntity.FLAG_APPLY, Global.businessApi_apply._2)
    }
    if (Global.businessApi_bind._1 == tableName) {
      processBind(UserOptEntity.FLAG_BIND, Global.businessApi_bind._2)
    }
    if (Global.businessApi_selfExaminePass._1 == tableName) {
      processSelfExaminePass(UserOptEntity.FLAG_SELF_EXAMINE_PASS, Global.businessApi_selfExaminePass._2)
    }
    if (Global.businessApi_bankExaminePass._1 == tableName) {
      processBankExaminePass(UserOptEntity.FLAG_BANK_EXAMINE_PASS, Global.businessApi_bankExaminePass._2)
    }
  }

  def processApply(action: String, api: String): Unit = {
    doProcess(action, api)
  }

  def processBind(action: String, api: String): Unit = {
    doProcess(action, api)
  }

  def processSelfExaminePass(action: String, api: String): Unit = {
    doProcess(action, api)
  }

  def processBankExaminePass(action: String, api: String): Unit = {
    doProcess(action, api)
  }

  private def doProcess(action: String, api: String): Unit = {
    UserLogEntity.db.get(action).onSuccess {
      case lastUpdateTimeResp =>
        val lastUpdateTime = lastUpdateTimeResp.body.last_update_time + ""
        val currentTime = df.format(new Date()) + ""
        HttpHelper.post(s"$api", Map("token" -> "ybpadmin", "start" -> lastUpdateTime, "end" -> currentTime), classOf[JsonNode], "application/x-www-form-urlencoded").onSuccess {
          case newRecordsResp =>
            if (newRecordsResp) {
              if (newRecordsResp.body.get("respCode").asText() == "100200") {
                val userOptEntities = newRecordsResp.body.get("data").map {
                  record =>
                    val userOpt = new UserOptEntity
                    userOpt.id = action + "_" + record.get("loanUuid").asText()
                    val time = record.get("occurTime").asText()
                    userOpt.occur_time = time.toLong
                    userOpt.occur_date = time.substring(0, 8).toLong
                    userOpt.occur_month = time.substring(0, 6).toLong
                    userOpt.occur_year = time.substring(0, 4).toLong
                    userOpt.user_id = record.get("userId").asText()
                    userOpt.action = action
                    userOpt.platform = record.get("sysSource").asText().toLowerCase
                    if (userOpt.platform != VisitEntity.FLAG_PLATFORM_PC && userOpt.platform != "xdgc") {
                      userOpt.platform = VisitEntity.FLAG_PLATFORM_MOBILE
                    }
                    userOpt.amount =
                      if (record.has("amount") && record.get("amount") != null && record.get("amount").asText() != "null")
                        (record.get("amount").asText().replaceAll(",", "").toDouble * 1000).toLong
                      else
                        0L
                    userOpt
                }
                Global.vertx.executeBlocking(new Handler[Future[Void]] {
                  override def handle(event: Future[Void]): Unit = {
                    userOptEntities.foreach {
                      userOptEntity =>
                        val findResult = Await.result(UserOptEntity.db.find("user_id = ? AND action = ? LIMIT 1", List(userOptEntity.user_id, UserOptEntity.FLAG_REGISTER)), 30 seconds).body
                        if (findResult.length == 1) {
                          //找到对应的用户，使用注册时的来源
                          userOptEntity.source = findResult.head.source
                        } else {
                          //没有对应用户，使用空的来源
                          userOptEntity.source = ""
                        }
                        Await.result(UserOptEntity.db.save(userOptEntity), 30 seconds)
                    }
                    //TODO process update
                    UserLogEntity.db.update("last_update_time = ? ", "action =? ", List(currentTime, action))
                  }
                }, false, new Handler[AsyncResult[Void]] {
                  override def handle(event: AsyncResult[Void]): Unit = {}
                })
              } else {
                logger.warn(s"API Sync error [${newRecordsResp.body.get("respCode").asText()}] ${newRecordsResp.body.get("respMsg").asText()}")
              }
            } else {
              logger.warn(s"API Sync error [${newRecordsResp.code}] ${newRecordsResp.message}")
            }
        }
    }
  }

}
