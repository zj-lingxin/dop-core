package com.asto.dop.core.function

import java.util.concurrent.CountDownLatch

import com.asto.dop.core.entity.SourceFlagEntity
import com.asto.dop.core.module.manage.SourceProcessor

import scala.concurrent.ExecutionContext.Implicits.global

class SourceSpec extends DBBasicSpec {

  test("Source Test") {
    val cdl = new CountDownLatch(1)
    val source = SourceFlagEntity()
    source.id = 1111111
    source.flag = "baidu"
    source.source = "百度搜索"
    source.source_type = "百度"
    source.link_page = "首页"
    source.link_type = "页面"
    source.link_url = "www.ybp.com"
    SourceProcessor.save(source).onSuccess {
      case saveResp =>
        source.source = null
        source.link_url = "www.yuanbaopu.com"
        SourceProcessor.update(Map("id" -> "1111111"), source).onSuccess {
          case updateResp =>
            SourceProcessor.get(Map("id" -> "1111111")).onSuccess {
              case getResp =>
                assert(getResp && getResp.body.link_url == "www.yuanbaopu.com" && getResp.body.source == "百度搜索")
                SourceProcessor.find(Map()).onSuccess {
                  case findResp =>
                    assert(findResp.body.length == 1 && findResp.body.head.flag == "baidu")
                    SourceProcessor.page(Map("pageNumber" -> "1")).onSuccess {
                      case pageResp =>
                        assert(pageResp.body.recordTotal == 1 && pageResp.body.objects.head.flag == "baidu")
                        SourceProcessor.delete(Map("id" -> "1111111")).onSuccess {
                          case delResp =>
                            SourceProcessor.get(Map("id" -> "1111111")).onSuccess {
                              case get2Resp =>
                                assert(get2Resp && get2Resp.body == null)
                                cdl.countDown()
                            }
                        }
                    }
                }
            }
        }
    }

    cdl.await()
  }

}


