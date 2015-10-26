package com.asto.dop.core.helper

import com.ecfront.common.{JsonHelper, Resp, StandardCode}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.http._

import scala.concurrent.{Future, Promise}

/**
 * HTTP 异步操作辅助类
 *
 * 包含了对HTTP GET POST PUT DELETE 四类常用操作
 *
 */
object HttpHelper extends LazyLogging {

  var httpClient: HttpClient = _

  def get[E](url: String, responseClass: Class[E], contentType: String = "application/json"): Future[Resp[E]] = {
    request(HttpMethod.GET, url, null, responseClass, contentType)
  }

  def post[E](url: String, body: Any, responseClass: Class[E], contentType: String = "application/json"): Future[Resp[E]] = {
    request(HttpMethod.POST, url, body, responseClass, contentType)
  }

  def put[E](url: String, body: Any, responseClass: Class[E], contentType: String = "application/json"): Future[Resp[E]] = {
    request(HttpMethod.PUT, url, body, responseClass, contentType)
  }

  def delete[E](url: String, responseClass: Class[E], contentType: String = "application/json"): Future[Resp[E]] = {
    request(HttpMethod.DELETE, url, null, responseClass, contentType)
  }

  private def request[E](method: HttpMethod, url: String, body: Any, responseClass: Class[E], contentType: String): Future[Resp[E]] = {
    val p = Promise[Resp[E]]()
    val client = httpClient.requestAbs(method, url, new Handler[HttpClientResponse] {
      override def handle(response: HttpClientResponse): Unit = {
        if (response.statusCode + "" != StandardCode.SUCCESS) {
          logger.error("Server NOT responded.")
          p.success(Resp.serverUnavailable(s"Server [$method:$url] NOT responded."))
        } else {
          response.bodyHandler(new Handler[Buffer] {
            override def handle(data: Buffer): Unit = {
              p.success(Resp.success(JsonHelper.toObject(data.toString("UTF-8"), responseClass)))
            }
          })
        }
      }
    }).putHeader("content-type", contentType)
    if (body != null) {
      contentType.toLowerCase match {
        case c if c == "application/x-www-form-urlencoded" && body.isInstanceOf[Map[_, _]] =>
          client.end(body.asInstanceOf[Map[String, String]].map(i => i._1 + "=" + i._2).mkString("&"))
        case _ =>
          client.end(JsonHelper.toJsonString(body))
      }
    } else {
      client.end()
    }
    p.future
  }

  def returnContent(result: Any, response: HttpServerResponse, contentType: String = "application/json; charset=UTF-8") {
    //支持CORS
    val res = result match {
      case r: String => r
      case _ => JsonHelper.toJsonString(result)
    }
    response.setStatusCode(200).putHeader("Content-Type", contentType)
      .putHeader("Cache-Control", "no-cache")
      .putHeader("Access-Control-Allow-Origin", "*")
      .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
      .putHeader("Access-Control-Allow-Headers", "Content-Type, X-Requested-With, X-authentication, X-client")
      .end(res)
  }

}

