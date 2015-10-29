package com.asto.dop.core.function

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.asto.dop.core.entity.VisitEntity

import scala.concurrent.ExecutionContext.Implicits.global

class VisitDBSpec extends DBBasicSpec {

  private val df = new SimpleDateFormat("yyyyMMddHHmmss")

  test("Visit Entity DB Test") {
    val cdl = new CountDownLatch(1)
    val visitEntity = VisitEntity()
    val time = df.format(new Date())
    visitEntity.occur_time = time.toLong
    visitEntity.occur_datehour = time.substring(0, 10).toLong
    visitEntity.occur_date = time.substring(0, 8).toLong
    visitEntity.occur_month = time.substring(0, 6).toLong
    visitEntity.occur_year = time.substring(0, 4).toLong
    visitEntity.id = visitEntity.occur_time + "123456"
    visitEntity.pv_hash = "sdfddddddddd"
    visitEntity.c_platform = "pc"
    visitEntity.c_system = "windows"
    visitEntity.c_device_id = ""
    visitEntity.c_ipv4 = "115.236.188.99"
    visitEntity.c_ip_addr = "浙江省杭州市"
    visitEntity.c_ip_country = "中国"
    visitEntity.c_ip_province = "浙江"
    visitEntity.c_ip_city = "杭州"
    visitEntity.c_ip_county = ""
    visitEntity.c_ip_isp = ""
    visitEntity.c_gps = ""
    visitEntity.u_user_id = ""
    visitEntity.visitor_id = ""
    visitEntity.u_cookie_id = "1dsfsfsd052dd="
    visitEntity.u_cookies = "dHRhOmRmZGRmLGRzZnNm"
    visitEntity.v_new_visitor = false
    visitEntity.v_source = "百度搜索"
    visitEntity.v_referer = "http://www.baidu.com/q=xx"
    visitEntity.v_url = "http://www.yuanbaopu.com/"
    visitEntity.v_url_path = new URL("http://www.yuanbaopu.com/").getPath
    visitEntity.v_action = "login"
    visitEntity.v_residence_time = 0
    VisitEntity.db.save(visitEntity).onSuccess {
      case _ =>
        VisitEntity.db.update("c_platform= ? ", "id= ? ", List("mobile", visitEntity.id)).onSuccess {
          case _ =>
            VisitEntity.db.get(visitEntity.id).onSuccess {
              case getResp =>
                assert(getResp && getResp.body.c_platform == "mobile" && getResp.body.v_source == "百度搜索")
                VisitEntity.db.find().onSuccess {
                  case findResp =>
                    assert(findResp && findResp.body.length == 1)
                    VisitEntity.db.page().onSuccess {
                      case pageResp =>
                        assert(pageResp && pageResp.body.recordTotal == 1)
                        VisitEntity.db.delete(visitEntity.id).onSuccess {
                          case deleteResp =>
                            assert(deleteResp)
                            cdl.countDown()
                        }
                    }
                }
            }
        }
    }
    cdl.await()
  }

}

