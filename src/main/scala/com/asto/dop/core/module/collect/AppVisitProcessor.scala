package com.asto.dop.core.module.collect

import java.util.Date

import com.asto.dop.core.IP
import com.asto.dop.core.entity.VisitEntity
import com.asto.dop.core.module.EventBus
import com.ecfront.common.Resp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * APP访问处理器
 */
object AppVisitProcessor extends VisitCollectProcessor {

  def process(req: AppVisitReq, remoteIP: String): Future[Resp[String]] = {
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

  private def checkReq(req: AppVisitReq): Resp[String] = {
    if (req.request_id == null || req.request_id.trim.length != 6) {
      return Resp.badRequest("【request_id】为6位随机编码")
    }
    if (req.c_system == null || !VisitEntity.systemEnum.contains(req.c_system.trim)) {
      return Resp.badRequest(s"【c_system】必须为 ${VisitEntity.systemEnum.mkString(",")}")
    }
    if (req.c_device_id == null) {
      return Resp.badRequest(s"【c_device_id】不能为空")
    }
    if (req.v_source == null) {
      return Resp.badRequest("【v_source】不能为空")
    }
    if (req.u_user_id == null) {
      return Resp.badRequest(s"【u_user_id】不能为空")
    }
    if (req.v_url_path == null || req.v_url_path.trim.isEmpty) {
      return Resp.badRequest("【v_url_path】不能为空")
    }
    if (req.v_action == null) {
      return Resp.badRequest("【v_action】不能为空")
    }
    Resp.success("")
  }

  private def packageReq(req: AppVisitReq, remoteIP: String): Future[Resp[VisitEntity]] = {
    val p = Promise[Resp[VisitEntity]]()
    val visitEntity = VisitEntity()
    val time = df.format(new Date())
    visitEntity.occur_time = time.toLong
    visitEntity.occur_datehour = time.substring(0, 10).toLong
    visitEntity.occur_date = time.substring(0, 8).toLong
    visitEntity.occur_month = time.substring(0, 6).toLong
    visitEntity.occur_year = time.substring(0, 4).toLong
    visitEntity.id = visitEntity.occur_time + req.request_id
    visitEntity.c_platform = VisitEntity.FLAG_PLATFORM_MOBILE
    visitEntity.c_system = req.c_system
    visitEntity.c_device_id = req.c_device_id
    visitEntity.c_ipv4 = remoteIP
    val ip = IP.find(remoteIP)
    visitEntity.c_ip_addr = if (req.c_ip_addr != null && req.c_ip_addr.nonEmpty) req.c_ip_addr else ip.mkString(" ")
    visitEntity.c_ip_country = if (req.c_ip_country != null && req.c_ip_country.nonEmpty) req.c_ip_country else ip(0)
    visitEntity.c_ip_province = if (req.c_ip_province != null && req.c_ip_province.nonEmpty) req.c_ip_province.replace("省", "") else ip(1)
    visitEntity.c_ip_city = if (req.c_ip_city != null && req.c_ip_city.nonEmpty) req.c_ip_city.replace("市", "") else ip(2)
    visitEntity.c_ip_county = if (req.c_ip_county != null && req.c_ip_county.nonEmpty) req.c_ip_county.replace("县", "").replace("市", "").replace("区", "") else ip(3)
    visitEntity.c_ip_isp = ""
    visitEntity.c_gps = req.c_gps
    visitEntity.u_user_id = req.u_user_id
    visitEntity.u_cookie_id = ""
    visitEntity.u_cookies = ""
    visitEntity.v_referer = ""
    if (!req.v_url_path.startsWith("/")) {
      req.v_url_path = "/" + req.v_url_path
    }
    visitEntity.v_url = "res://localhost" + req.v_url_path
    visitEntity.v_url_path = req.v_url_path
    visitEntity.v_action = req.v_action
    visitEntity.v_residence_time = 0
    visitEntity.pv_hash = getPVHash(visitEntity.v_url, req.request_id)
    visitEntity.v_source = req.v_source
    for {
      _ <- tryRecomputeVisitorInfo(visitEntity.v_action, visitEntity.u_user_id, visitEntity.u_cookie_id, visitEntity.c_device_id)
      getVisitorIdResp <- getVisitorId(visitEntity.u_cookie_id, visitEntity.c_device_id)
    } yield {
      visitEntity.visitor_id = getVisitorIdResp.body._1
      visitEntity.v_new_visitor = getVisitorIdResp.body._2
      tryInsertUserRegister(visitEntity)
      p.success(Resp.success(visitEntity))
    }
    p.future
  }

}
