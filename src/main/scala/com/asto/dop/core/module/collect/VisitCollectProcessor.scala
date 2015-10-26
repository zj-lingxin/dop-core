package com.asto.dop.core.module.collect

import java.text.SimpleDateFormat
import java.util.Date

import com.asto.dop.core.Global
import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{EncryptHelper, Resp}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 访问信息采集处理器
 */
trait VisitCollectProcessor extends LazyLogging {

  protected val df = new SimpleDateFormat("yyyyMMddHHmmss")

  /**
   * 设定访客Id及判断是否是新访客
   * 定义：
   * 新访客：数据库不存在传入的cookieId或设备Id
   * 老访客：! 新访客
   */
  protected def getVisitorId(u_cookie_id: String, c_device_id: String): Future[Resp[(String, Boolean)]] = {
    //TODO Optimize use redis.
    val cond = if (c_device_id.nonEmpty) {
      (s"c_device_id = ? ", c_device_id)
    } else {
      (s"u_cookie_id = ? ", u_cookie_id)
    }
    val p = Promise[Resp[(String,Boolean)]]()
    DBHelper.find(
      s"""
         |SELECT visitor_id FROM ${VisitEntity.db.TABLE_NAME} WHERE ${cond._1} LIMIT 1
      """.stripMargin,
      List(cond._2), classOf[JsonObject]
    ).onSuccess {
      case findResp =>
        if (findResp) {
          if (findResp.body.length == 1) {
            //此访客已存在，返回之前的id
            p.success(Resp.success((findResp.body.head.getString("visitor_id"), false)))
          } else {
            p.success(Resp.success((df.format(new Date()) + "" + System.nanoTime(), true)))
          }
        } else {
          p.success(findResp)
        }
    }
    p.future
  }

  /**
   * 重算访客信息
   * 为什么重算：
   * 假设用户A是已经注册用户，但他用了一台新设备或清空了Cookie，
   * 那么他第一次访问时设备Id或CookieId都是新的，会被记成新访客（一条记录）
   * 当A用户登录后发现用户Id已经存在，所以要更新之前信息
   * 即将此设备Id或CookieId的记录都更新为老访客并且访客id要与之前的一致
   */
  protected def tryRecomputeVisitorInfo(v_action: String, u_user_id: String, u_cookie_id: String, c_device_id: String): Future[Resp[Void]] = {
    val p = Promise[Resp[Void]]
    if (v_action == Global.visitRecomputeAction && u_user_id.nonEmpty) {
      DBHelper.find(
        s"""
           |SELECT visitor_id FROM ${VisitEntity.db.TABLE_NAME} WHERE u_user_id = ? LIMIT 1
      """.stripMargin,
        List(u_user_id), classOf[JsonObject]
      ).onSuccess {
        case findResp =>
          if (findResp && findResp.body.length == 1) {
            //如果此用户已存在则表示之前的记录是老访客行为
            val visitorId = findResp.body.head.getString("visitor_id")
            for {
              _ <- VisitEntity.db.update("visitor_id = ? ", " u_cookie_id = ? AND c_device_id = ? ", List(visitorId, u_cookie_id, c_device_id))
              //排除第一条
              _ <- DBHelper.update(
                s"""
                   |UPDATE ${VisitEntity.db.TABLE_NAME} SET v_new_visitor = ?  WHERE id in
                   |  ( SELECT * FROM
                   |    ( SELECT id FROM ${VisitEntity.db.TABLE_NAME} WHERE visitor_id = ? LIMIT 18446744073709551610 OFFSET 1 ) as tmp
                   |  )
               """.stripMargin, List(false, visitorId))
            } yield {
              p.success(Resp.success(null))
            }
          } else {
            p.success(Resp.success(null))
          }
      }
    } else {
      p.success(Resp.success(null))
    }
    p.future
  }

  /**
   * 获取页面HASH(MD5算法)，用于页面停留时间统计
   */
  protected def getPVHash(v_url: String, request_id: String): String = {
    EncryptHelper.encrypt(v_url + request_id, "MD5")
  }

  /**
   * 如果v_source是注册成功时向user_opt表中插入一条注册记录
   */
  protected def tryInsertUserRegister(visitEntity: VisitEntity): Unit = {
    if (visitEntity.v_action == Global.visitRegisterAction && visitEntity.u_user_id.nonEmpty) {
      val userOpt = new UserOptEntity
      userOpt.id = UserOptEntity.FLAG_REGISTER + "_" + visitEntity.u_user_id
      userOpt.occur_time = visitEntity.occur_time
      userOpt.occur_date = visitEntity.occur_date
      userOpt.occur_month = visitEntity.occur_month
      userOpt.occur_year = visitEntity.occur_year
      userOpt.user_id = visitEntity.u_user_id
      userOpt.action = UserOptEntity.FLAG_REGISTER
      userOpt.platform = visitEntity.c_platform
      userOpt.source = visitEntity.v_source
      userOpt.amount = 0
      UserOptEntity.db.save(userOpt)
    }
  }

}
