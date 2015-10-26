package com.asto.dop.core.business

import com.asto.dop.core.helper.HttpHelper
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.{Handler, Vertx}

object BusinessAPIMock {

  def start(): Unit = {
    Vertx.vertx().createHttpServer().requestHandler(new Handler[HttpServerRequest] {
      override def handle(request: HttpServerRequest): Unit = {
        request.path() match {
          case "/api/bind/" if request.method().name() == "GET" =>
            val start = request.params().get("start")
            val end = request.params().get("end")
            HttpHelper.returnContent(
              """
                |[
                |{"id":"1","occur_time":"20151021121040","user_id":"zhangsan","platform":"pc","amount":1000.1},
                |{"id":"2","occur_time":"20151021121040","user_id":"zhangsan","platform":"pc","amount":2000.1},
                |{"id":"3","occur_time":"20151021121040","user_id":"user2","platform":"pc","amount":3000.1},
                |{"id":"4","occur_time":"20151021121040","user_id":"user3","platform":"pc","amount":4000.1}
                |]
              """.stripMargin, request.response())
          case "/api/apply/" if request.method().name() == "GET" =>
            val start = request.params().get("start")
            val end = request.params().get("end")
            HttpHelper.returnContent(
              """
                |[
                |{"id":"1","occur_time":"20151021121040","user_id":"zhangsan","platform":"pc","amount":1000.1},
                |{"id":"2","occur_time":"20151021121040","user_id":"zhangsan","platform":"pc","amount":2000.1},
                |{"id":"3","occur_time":"20151021121040","user_id":"user2","platform":"pc","amount":3000.1},
                |{"id":"4","occur_time":"20151021121040","user_id":"user3","platform":"pc","amount":4000.1}
                |]
              """.stripMargin, request.response())
          case "/api/self-examine-pass/" if request.method().name() == "GET" =>
            val start = request.params().get("start")
            val end = request.params().get("end")
            HttpHelper.returnContent(
              """
                |[
                |{"id":"1","occur_time":"20151022121040","user_id":"zhangsan","platform":"pc","amount":100.1},
                |{"id":"2","occur_time":"20151022121040","user_id":"user2","platform":"pc","amount":300.1}
                |]
              """.stripMargin, request.response())
          case "/api/bank-examine-pass/" if request.method().name() == "GET" =>
            val start = request.params().get("start")
            val end = request.params().get("end")
            HttpHelper.returnContent(
              """
                |[
                |{"id":"1","occur_time":"20151023121040","user_id":"zhangsan","platform":"pc","amount":10.1}
                |]
              """.stripMargin, request.response())
        }
      }
    }).listen(8088)
  }
}
