package com.asto.dop.core.business.collect

import java.util.concurrent.CountDownLatch

import com.asto.dop.core.business.BusinessBasicSpec
import com.asto.dop.core.entity.VisitEntity
import com.asto.dop.core.module.collect._

import scala.concurrent.ExecutionContext.Implicits.global

class CollectProcessSpec extends BusinessBasicSpec {

  test("Browser Visit Process Test") {
    val cdl = new CountDownLatch(1)

    val req = BrowserVisitReq(
      request_id = "123456",
      c_platform = "pc",
      c_system = "windows",
      u_user_id = "",
      u_cookie_id = "1dsfsfsd052dd=",
      u_cookies = "dHRhOmRmZGRmLGRzZnNm",
      v_source = "baidu",
      v_referer = "http://www.baidu.com/q=xx",
      v_url = "http://www.yuanbaopu.com/index.htm",
      v_action = "login"
    )
    BrowserVisitProcessor.process(req, "115.236.188.99").onSuccess {
      case resultResp =>
        assert(resultResp)
        VisitEntity.db.get(resultResp.body).onSuccess {
          case getResp =>
            assert(getResp)
            val visitEntity = getResp.body
            assert(visitEntity.c_ip_addr == "中国 浙江 杭州 ")
            assert(visitEntity.v_url_path == "/index.htm")
            assert(visitEntity.v_url_path == "/index.htm")
            assert(visitEntity.u_cookies == "tta:dfddf,dsfsf")
            assert(visitEntity.v_new_visitor)
            VisitEntity.db.delete(visitEntity.id).onSuccess {
              case _ =>
                cdl.countDown()
            }
        }
    }
    cdl.await()
  }

  test("APP Visit Process Test") {
    val cdl = new CountDownLatch(1)

    val req = AppVisitReq(
      request_id = "123456",
      c_system = "iphone",
      c_device_id = "12323-24344-3455544-566744",
      c_ipv4 = "115.236.188.99",
      c_gps = "",
      u_user_id = "test_user",
      v_source = "91助手",
      v_url_path = "/user/login/",
      v_action = "login"
    )
    AppVisitProcessor.process(req).onSuccess {
      case resultResp =>
        assert(resultResp)
        VisitEntity.db.get(resultResp.body).onSuccess {
          case getResp =>
            assert(getResp)
            val visitEntity = getResp.body
            assert(visitEntity.c_ip_addr == "中国 浙江 杭州 ")
            assert(visitEntity.v_url_path == "/user/login/")
            assert(visitEntity.v_new_visitor)
            VisitEntity.db.delete(visitEntity.id).onSuccess {
              case _ =>
                cdl.countDown()
            }
        }
    }
    cdl.await()
  }

  test("Heartbeat Process Test") {
    val cdl = new CountDownLatch(1)

    val req = BrowserVisitReq(
      request_id = "123456",
      c_platform = "pc",
      c_system = "windows",
      u_user_id = "",
      u_cookie_id = "1dsfsfsd052dd=",
      u_cookies = "dHRhOmRmZGRmLGRzZnNm",
      v_source = "baidu",
      v_referer = "http://www.baidu.com/q=xx",
      v_url = "http://www.yuanbaopu.com/index.htm",
      v_action = "login"
    )
    BrowserVisitProcessor.process(req, "115.236.188.99").onSuccess {
      case resultResp =>
        assert(resultResp)
        HeartbeatProcessor.process(Map(
          "url" -> "http%3A%2F%2Fwww.yuanbaopu.com%2Findex.htm",
          "request_id" -> req.request_id,
          "interval" -> "3"
        )).onSuccess {
          case ht1Resp =>
            assert(ht1Resp == "ok")
            HeartbeatProcessor.process(Map(
              "url" -> "http%3A%2F%2Fwww.yuanbaopu.com%2Findex.htm",
              "request_id" -> req.request_id,
              "interval" -> "3"
            )).onSuccess {
              case ht2Resp =>
                assert(ht2Resp == "ok")
                VisitEntity.db.get(resultResp.body).onSuccess {
                  case getResp =>
                    assert(getResp)
                    val visitEntity = getResp.body
                    assert(visitEntity.v_residence_time == 6)
                    VisitEntity.db.delete(visitEntity.id).onSuccess {
                      case _ =>
                        cdl.countDown()
                    }
                }
            }
        }
    }
    cdl.await()
  }

  test("API Process Test") {
    val cdl = new CountDownLatch(1)

    APIProcessor.processApply()
    APIProcessor.processBind()
    APIProcessor.processSelfExaminePass()
    APIProcessor.processBankExaminePass()
    /*APIProcessor.process(Global.businessApi_bind._1)
     APIProcessor.process(Global.businessApi_selfExaminePass._1)
     APIProcessor.process(Global.businessApi_bankExaminePass._1)*/

    cdl.await()
  }

}

