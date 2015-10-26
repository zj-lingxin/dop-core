package com.asto.dop.core

import com.asto.dop.core.entity.SourceFlagEntity
import com.asto.dop.core.helper.{HttpHelper, WSHelper}
import com.asto.dop.core.module.collect._
import com.asto.dop.core.module.manage.SourceProcessor
import com.asto.dop.core.module.query._
import com.ecfront.common.JsonHelper
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServerRequest

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.matching.Regex

/**
 * HTTP路由分发器
 */
//TODO Refactoring : Use router table like RoR
class HttpRouter extends Handler[HttpServerRequest] with LazyLogging {

  private val rSourceIdMatch = new Regex( """/manage/source/(\S+)/""")

  private def router(request: HttpServerRequest): Unit = {
    request.path() match {
      //================================Collect================================
      case "/collect/browser/visit/" if request.method().name() == "POST" =>
        request.bodyHandler(new Handler[Buffer] {
          override def handle(data: Buffer): Unit = {
            val browserVisitReq = JsonHelper.toObject(data.getString(0, data.length), classOf[BrowserVisitReq])
            BrowserVisitProcessor.process(browserVisitReq, request.remoteAddress().host()).onSuccess {
              case result => HttpHelper.returnContent(result, request.response())
            }
          }
        })
      case "/collect/app/visit/" if request.method().name() == "POST" =>
        request.bodyHandler(new Handler[Buffer] {
          override def handle(data: Buffer): Unit = {
            val appVisitReq = JsonHelper.toObject(data.getString(0, data.length), classOf[AppVisitReq])
            AppVisitProcessor.process(appVisitReq).onSuccess {
              case result => HttpHelper.returnContent(result, request.response())
            }
          }
        })
      case "/collect/hb/" if request.method().name() == "GET" =>
        HeartbeatProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      //================================Query================================
      case "/query/realtime/" if request.method().name() == "GET" =>
        RealTimeProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/30days/" if request.method().name() == "GET" =>
        ThirtyDaysProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/traffic-analysis/" if request.method().name() == "GET" =>
        TrafficAnalysisProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/realtime/visit/" if request.method().name() == "GET" =>
        RealTimeVisitProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/source-trans-comp/summary/" if request.method().name() == "GET" =>
        AnalysisSourceTransCompProcessor.summaryProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/source-trans-comp/detail/" if request.method().name() == "GET" =>
        AnalysisSourceTransCompProcessor.detailProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/ws/" if request.method().name() == "GET" =>
        WSHelper.createWS(request.upgrade())
      //================================Source Manage================================
      case "/manage/source/" if request.method().name() == "GET" =>
        SourceProcessor.find(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/manage/source/page/" if request.method().name() == "GET" =>
        SourceProcessor.page(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/manage/source/" if request.method().name() == "POST" =>
        request.bodyHandler(new Handler[Buffer] {
          override def handle(data: Buffer): Unit = {
            val sourceFlagEntity = JsonHelper.toObject(data.getString(0, data.length), classOf[SourceFlagEntity])
            SourceProcessor.save(sourceFlagEntity).onSuccess {
              case result => HttpHelper.returnContent(result, request.response())
            }
          }
        })
      case rSourceIdMatch(id) if request.method().name() == "PUT" =>
        request.bodyHandler(new Handler[Buffer] {
          override def handle(data: Buffer): Unit = {
            val sourceFlagEntity = JsonHelper.toObject(data.getString(0, data.length), classOf[SourceFlagEntity])
            SourceProcessor.update(Map("id" -> id), sourceFlagEntity).onSuccess {
              case result => HttpHelper.returnContent(result, request.response())
            }
          }
        })
      case rSourceIdMatch(id) if request.method().name() == "GET" =>
        SourceProcessor.get(Map("id" -> id)).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case rSourceIdMatch(id) if request.method().name() == "DELETE" =>
        SourceProcessor.delete(Map("id" -> id)).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      //================================Others================================
      case _ =>
        logger.warn("Requested address is not found.")
        HttpHelper.returnContent("您访问的地址不存在", request.response(), "text/html")
    }
  }

  override def handle(request: HttpServerRequest): Unit = {
    if (request.method().name() == "OPTIONS") {
      HttpHelper.returnContent("", request.response(), "text/html")
    } else {
      logger.trace(s"Receive a request , from ${request.remoteAddress().host()} ")
      try {
        router(request)
      } catch {
        case ex: Throwable =>
          logger.error("Http process error.", ex)
          HttpHelper.returnContent(s"请求处理错误：${ex.getMessage}", request.response(), "text/html")
      }
    }
  }

}

case class HttpProcessor[E](parameters: Map[String, String], body: E)
