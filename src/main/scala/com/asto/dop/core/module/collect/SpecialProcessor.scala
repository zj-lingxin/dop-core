package com.asto.dop.core.module.collect

import com.asto.dop.core.Global
import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.HttpHelper
import com.ecfront.common.Resp
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.{AsyncResult, Future, Handler}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * 特殊业务处理器
 */
object SpecialProcessor extends LazyLogging {

  //历史数据迁移，正常情况下用户注册信息由visit记录中v_action=register_success时写入user_opt表
  def processRegisterMigration(req: Map[String, String]): concurrent.Future[Resp[Void]] = {
    val p = Promise[Resp[Void]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      HttpHelper.post(s"${Global.config.getJsonObject("businessApi").getJsonObject("register").getString("api")}", Map("token" -> "ybpadmin", "start" -> start, "end" -> end), classOf[JsonNode], "application/x-www-form-urlencoded").onSuccess {
        case newRecordsResp =>
          if (newRecordsResp) {
            if (newRecordsResp.body.get("respCode").asText() == "100200") {
              val userOptEntities = newRecordsResp.body.get("data").map {
                record =>
                  val userOpt = new UserOptEntity
                  userOpt.id = UserOptEntity.FLAG_REGISTER + "_" + record.get("userId").asText()
                  val time = record.get("occurTime").asText()
                  userOpt.occur_time = time.toLong
                  userOpt.occur_datehour = time.substring(0, 10).toLong
                  userOpt.occur_date = time.substring(0, 8).toLong
                  userOpt.occur_month = time.substring(0, 6).toLong
                  userOpt.occur_year = time.substring(0, 4).toLong
                  userOpt.user_id = record.get("userId").asText()
                  userOpt.action = UserOptEntity.FLAG_REGISTER
                  userOpt.platform = record.get("sysSource").asText().toLowerCase
                  if (userOpt.platform != VisitEntity.FLAG_PLATFORM_PC && userOpt.platform != "xdgc") {
                    userOpt.platform = VisitEntity.FLAG_PLATFORM_MOBILE
                  }
                  userOpt.amount = 0L
                  userOpt.source = ""
                  userOpt.ipv4 = ""
                  userOpt.ip_addr = ""
                  userOpt.ip_country = ""
                  userOpt.ip_province = ""
                  userOpt.ip_city = ""
                  userOpt.ip_county = ""
                  userOpt.ip_isp = ""
                  userOpt
              }
              Global.vertx.executeBlocking(new Handler[Future[Void]] {
                override def handle(event: Future[Void]): Unit = {
                  userOptEntities.foreach {
                    userOpt =>
                      Await.result(UserOptEntity.db.save(userOpt), 30 seconds)
                  }
                  event.complete()
                }
              }, false, new Handler[AsyncResult[Void]] {
                override def handle(event: AsyncResult[Void]): Unit = {
                  if(event.succeeded()) {
                    p.success(Resp.success(null))
                  }else{
                    logger.error(s"Special user register migration error [${newRecordsResp.body.get("respCode").asText()}] ${newRecordsResp.body.get("respMsg").asText()}",event.cause())
                    p.success(Resp.serverError(event.cause().getMessage))
                  }
                }
              })
            } else {
              logger.warn(s"Special user register migration error [${newRecordsResp.body.get("respCode").asText()}] ${newRecordsResp.body.get("respMsg").asText()}")
              p.success(Resp.serverError(s"Special user register migration error [${newRecordsResp.body.get("respCode").asText()}] ${newRecordsResp.body.get("respMsg").asText()}"))
            }
          } else {
            logger.warn(s"Special user register migration error [${newRecordsResp.code}] ${newRecordsResp.message}")
            p.success(Resp.serverError(s"Special user register migration error [${newRecordsResp.body.get("respCode").asText()}] ${newRecordsResp.body.get("respMsg").asText()}"))
          }
      }
    }
    p.future
  }

}
