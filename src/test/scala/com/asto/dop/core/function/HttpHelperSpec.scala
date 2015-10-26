package com.asto.dop.core.function

import java.util.concurrent.CountDownLatch

import com.asto.dop.core.helper.HttpHelper
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.Vertx
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

class HttpHelperSpec extends FunSuite with LazyLogging {

  test("HttpHelper Test") {
    val cdl = new CountDownLatch(1)

    HttpHelper.httpClient = Vertx.vertx().createHttpClient()

    HttpHelper.get("http://www.baidu.com", classOf[String]).onSuccess {
      case getResp =>
        assert(getResp)
        cdl.countDown()
    }

    cdl.await()
  }

}


