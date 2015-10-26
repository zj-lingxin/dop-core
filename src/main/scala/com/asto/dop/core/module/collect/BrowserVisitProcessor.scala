package com.asto.dop.core.module.collect

import java.net.URL
import java.util.{Base64, Date}

import com.asto.dop.core.IP
import com.asto.dop.core.entity.VisitEntity
import com.asto.dop.core.module.EventBus
import com.asto.dop.core.module.collect.AppVisitProcessor._
import com.ecfront.common.Resp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 浏览器访问处理器
 */
object BrowserVisitProcessor extends VisitCollectProcessor {

  def process(req: BrowserVisitReq, remoteIP: String): Future[Resp[String]] = {
    val p = Promise[Resp[String]]()
    val checkPassResp = checkReq(req)
    if (checkPassResp) {
      packageReq(req, remoteIP).onSuccess {
        case packageResp =>
          if (packageResp) {
            //save
            VisitEntity.db.save(packageResp.body).onSuccess {
              case resResp =>
                if (resResp) {
                  EventBus.visitEntitySaveEvent.pub(packageResp.body)
                  p.success(Resp.success(packageResp.body.id))
                } else {
                  p.success(resResp)
                }
            }
          } else {
            p.success(packageResp)
          }
      }
    } else {
      p.success(checkPassResp)
    }
    p.future
  }

  private def checkReq(req: BrowserVisitReq): Resp[String] = {
    if (req.request_id == null || req.request_id.trim.length != 6) {
      return Resp.badRequest("【request_id】为6位随机编码")
    }
    if (req.c_platform == null || !VisitEntity.platformEnum.contains(req.c_platform.trim)) {
      return Resp.badRequest(s"【c_platform】必须为 ${VisitEntity.platformEnum.mkString(",")}")
    }
    if (req.c_system == null || !VisitEntity.systemEnum.contains(req.c_system.trim)) {
      return Resp.badRequest(s"【c_system】必须为 ${VisitEntity.systemEnum.mkString(",")}")
    }
    if (req.u_cookie_id == null || req.u_cookie_id.trim.isEmpty) {
      return Resp.badRequest("【u_cookie_id】不能为空")
    }
    if (req.v_referer == null) {
      return Resp.badRequest("【v_referer】不能为空")
    }
    if (req.v_source == null) {
      return Resp.badRequest("【v_source】不能为空")
    }
    if (req.v_url == null || req.v_url.trim.isEmpty) {
      return Resp.badRequest("【v_url】不能为空")
    }
    if (req.v_action == null) {
      return Resp.badRequest("【v_action】不能为空")
    }
    Resp.success("")
  }

  private def packageReq(req: BrowserVisitReq, remoteIP: String): Future[Resp[VisitEntity]] = {
    val p = Promise[Resp[VisitEntity]]()
    val visitEntity = VisitEntity()
    val time = df.format(new Date())
    visitEntity.occur_time = time.toLong
    visitEntity.occur_date = time.substring(0, 8).toLong
    visitEntity.occur_month = time.substring(0, 6).toLong
    visitEntity.occur_year = time.substring(0, 4).toLong
    visitEntity.id = visitEntity.occur_time + req.request_id
    visitEntity.pv_hash = getPVHash(req.v_url, req.request_id)
    visitEntity.c_platform = req.c_platform
    visitEntity.c_system = req.c_system
    visitEntity.c_device_id = ""
    visitEntity.c_ipv4 = remoteIP
    val ip = IP.find(remoteIP)
    visitEntity.c_ip_addr = ip.mkString(" ")
    visitEntity.c_ip_country = ip(0)
    visitEntity.c_ip_province = ip(1)
    visitEntity.c_ip_city = ip(2)
    visitEntity.c_ip_county = ip(3)
    visitEntity.c_ip_isp = ""
    visitEntity.c_gps = ""
    visitEntity.u_user_id = req.u_user_id
    visitEntity.u_cookie_id = req.u_cookie_id
    visitEntity.u_cookies = new String(Base64.getDecoder.decode(req.u_cookies), "utf-8")
    visitEntity.v_referer = req.v_referer
    visitEntity.v_url = req.v_url
    visitEntity.v_url_path = new URL(req.v_url).getPath
    visitEntity.v_action = req.v_action
    visitEntity.v_residence_time = 0
    visitEntity.v_source = req.v_source
    for {
      _ <- tryRecomputeVisitorInfo(visitEntity.v_action, visitEntity.u_user_id, visitEntity.u_cookie_id, visitEntity.c_device_id)
      getVisitorIdResp <- getVisitorId( visitEntity.u_cookie_id, visitEntity.c_device_id)
    } yield {
      visitEntity.visitor_id = getVisitorIdResp.body._1
      visitEntity.v_new_visitor = getVisitorIdResp.body._2
      tryInsertUserRegister(visitEntity)
      p.success(Resp.success(visitEntity))
    }
    p.future
  }

}
