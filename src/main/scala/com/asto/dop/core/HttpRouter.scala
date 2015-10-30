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
  private val rVisitIdMatch = new Regex( """/query/realtime/visit/(\S+)/""")

  private def router(request: HttpServerRequest,ip:String): Unit = {
    request.path() match {
      case "/favicon.ico" =>
      //================================Collect================================
      case "/collect/browser/visit/" if request.method().name() == "POST" =>
        request.bodyHandler(new Handler[Buffer] {
          override def handle(data: Buffer): Unit = {
            val browserVisitReq = JsonHelper.toObject(data.toString("UTF-8"), classOf[BrowserVisitReq])
            BrowserVisitProcessor.process(browserVisitReq, ip).onSuccess {
              case result => HttpHelper.returnContent(result, request.response())
            }
          }
        })
      case "/collect/app/visit/" if request.method().name() == "POST" =>
        request.bodyHandler(new Handler[Buffer] {
          override def handle(data: Buffer): Unit = {
            val appVisitReq = JsonHelper.toObject(data.toString("UTF-8"), classOf[AppVisitReq])
            AppVisitProcessor.process(appVisitReq, ip).onSuccess {
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
      case "/query/area-dist/" if request.method().name() == "GET" =>
        AreaDistProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/realtime/visit/" if request.method().name() == "GET" =>
        RealTimeVisitProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case rVisitIdMatch(visitorId) if request.method().name() == "GET" =>
        RealTimeVisitProcessor.visitorDetails(Map("visitor_id" -> visitorId)).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/realtime/source/platform/" if request.method().name() == "GET" =>
        RealTimeSourceProcessor.platformProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/realtime/source/area/" if request.method().name() == "GET" =>
        RealTimeSourceProcessor.areaProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/realtime/summary/" if request.method().name() == "GET" =>
        RealTimeSummaryProcessor.process(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/customer-trans/summary/" if request.method().name() == "GET" =>
        AnalysisCustomerTransProcessor.summaryProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/customer-trans/trend/" if request.method().name() == "GET" =>
        AnalysisCustomerTransProcessor.trendProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/customer-trans-comp/platform/summary/" if request.method().name() == "GET" =>
        AnalysisCustomerTransCompProcessor.platformSummaryProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/customer-trans-comp/platform/trend/" if request.method().name() == "GET" =>
        AnalysisCustomerTransCompProcessor.platformTrendProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/customer-trans-comp/visitor/summary/" if request.method().name() == "GET" =>
        AnalysisCustomerTransCompProcessor.visitorSummaryProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
          case result => HttpHelper.returnContent(result, request.response())
        }
      case "/query/analysis/customer-trans-comp/visitor/trend/" if request.method().name() == "GET" =>
        AnalysisCustomerTransCompProcessor.visitorTrendProcess(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
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
            val sourceFlagEntity = JsonHelper.toObject(data.toString("UTF-8"), classOf[SourceFlagEntity])
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
      //================================Special Process================================
      //历史数据迁移，正常情况下用户注册信息由visit记录中v_action=register_success时写入user_opt表
      case "/special/useropt/register/migration/" if request.method().name() == "GET" =>
        SpecialProcessor.processRegisterMigration(request.params().map(entry => entry.getKey -> entry.getValue).toMap).onSuccess {
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
      val ip =
        if (request.headers().contains("X-Forwarded-For") && request.getHeader("X-Forwarded-For").nonEmpty) {
          request.getHeader("X-Forwarded-For")
        } else {
          request.remoteAddress().host()
        }
      logger.trace(s"Receive a request [${request.uri()}] , from $ip ")
      try {
        router(request,ip)
      } catch {
        case ex: Throwable =>
          logger.error("Http process error.", ex)
          HttpHelper.returnContent(s"请求处理错误：${ex.getMessage}", request.response(), "text/html")
      }
    }
  }

}

case class HttpProcessor[E](parameters: Map[String, String], body: E)
