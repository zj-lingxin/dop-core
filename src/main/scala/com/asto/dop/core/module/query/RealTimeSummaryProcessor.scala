package com.asto.dop.core.module.query

import java.util.Calendar

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
 * 实时概况
 */
object RealTimeSummaryProcessor extends QueryProcessor {

  private val TIMES=Set("today","yesterday","last7","last30")
  private val INDEXS=Set("pv","uv","register","bind","apply")


  private def theDayBeforeYesterday = dateOffset(-2)

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    if (!req.contains("time") || !req.contains("index")|| !TIMES.contains(req("time"))|| !INDEXS.contains(req("index"))) {
      p.success(Resp.badRequest(s"【time】【index】不能为空，【time】必须是 [${TIMES.mkString(",")}] 中的一项，【index】必须是 [${INDEXS.mkString(",")}] 中的一项"))
    } else {
      val visitPlatformSql = if (req.contains("platform")) s" c_platform = '${req("platform")}' AND " else ""
      val userOptPlatformSql = if (req.contains("platform")) s" platform = '${req("platform")}'  AND " else ""
      val sqlSeg= req("time") match {
        //today获取的是今天和昨天的数据，yesterday获取的是昨天和前天的数据
        case "today" => ("occur_datehour AS date ",s"  occur_date >= ${dateOffset(-1)} AND occur_date <= ${dateOffset(0)} GROUP BY date ")
        case "yesterday" => ("occur_datehour AS date ",s"  occur_date >= ${dateOffset(-2)} AND occur_date <= ${dateOffset(-1)} GROUP BY date ")
        case "last7" => ("occur_date AS date ",s"  occur_date >= ${dateOffset(-7)} AND occur_date <= ${dateOffset(-1)} GROUP BY date ")
        case "last30" => ("occur_date AS date ",s"  occur_date >= ${dateOffset(-30)} AND occur_date <= ${dateOffset(-1)} GROUP BY date ")
      }

     val respFuture= req("index") match {
        case "pv" =>
          DBHelper.find(s"SELECT ${sqlSeg._1},COUNT(1) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE $visitPlatformSql ${sqlSeg._2}",List(),classOf[JsonObject])
        case "uv" =>
          DBHelper.find(s"SELECT ${sqlSeg._1},COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE $visitPlatformSql ${sqlSeg._2}",List(),classOf[JsonObject])
        case "register" =>
          DBHelper.find(s"SELECT ${sqlSeg._1},COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE $userOptPlatformSql action = ? AND ${sqlSeg._2}",List(UserOptEntity.FLAG_REGISTER),classOf[JsonObject])
        case "bind" =>
          DBHelper.find(s"SELECT ${sqlSeg._1},COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE $userOptPlatformSql action = ? AND ${sqlSeg._2}",List(UserOptEntity.FLAG_BIND),classOf[JsonObject])
        case "apply" =>
          DBHelper.find(s"SELECT ${sqlSeg._1},COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE $userOptPlatformSql action = ? AND ${sqlSeg._2}",List(UserOptEntity.FLAG_APPLY),classOf[JsonObject])
     }

      respFuture.onSuccess{
        case resp =>
          val result = resp.body.map(i => s"""{"date":${i.getLong("date")},"count":${i.getLong("count")}}""").mkString("[",",","]")
          p.success(Resp.success(JsonHelper.toJson(result)))
      }

    }
  }

}
