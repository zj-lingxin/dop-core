package com.asto.dop.core.module.query

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
 * 地域分布（以下所有的数据都为最近7天的数据）
 */
object AreaDistProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    val yesterday = dateOffset(-1)
    val last7Day = dateOffset(-7)

    val visitorCountTopResp = DBHelper.find(s"SELECT c_ip_province,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? GROUP BY c_ip_province ORDER BY count DESC LIMIT 10 ", List(last7Day, yesterday), classOf[JsonObject])

    val selfCountTopResp = DBHelper.find(s"SELECT ip_province,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND action =? GROUP BY ip_province ORDER BY count DESC LIMIT 10 ", List(last7Day, yesterday, UserOptEntity.FLAG_SELF_EXAMINE_PASS), classOf[JsonObject])

    for {
      visitorCountTop <- visitorCountTopResp
      selfCountTop <- selfCountTopResp
      registerCountTop <- DBHelper.find(
        s"""
           |SELECT ip_province,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |    WHERE occur_date >= ? AND occur_date <= ? AND action =? AND  ip_province IN ( ${visitorCountTop.body.map(i => s"'${i.getString("c_ip_province")}'").mkString(",")} ) GROUP BY ip_province
         """.stripMargin, List(last7Day, yesterday, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      bankCountTop <- DBHelper.find(
        s"""
           |SELECT ip_province,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |    WHERE occur_date >= ? AND occur_date <= ? AND action =? AND  ip_province IN ( ${registerCountTop.body.map(i => s"'${i.getString("ip_province")}'").mkString(",")} ) GROUP BY ip_province
         """.stripMargin, List(last7Day, yesterday, UserOptEntity.FLAG_BANK_EXAMINE_PASS), classOf[JsonObject])
    } yield {
      val visitorCountTopResult = visitorCountTop.body.map {
        count =>
          s"""
             |{
             |    "area":"${count.getString("c_ip_province")}",
             |    "visitors":${count.getLong("count")},
             |    "registers":${registerCountTop.body.find(i => i.getString("ip_province") == count.getString("c_ip_province")).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")}
             |}
           """.stripMargin
      }.mkString("[", ",", "]")
      val selfCountTopResult = selfCountTop.body.map {
        count =>
          s"""
             |{
             |    "area":"${count.getString("ip_province")}",
             |    "selfExaminePass":${count.getLong("count")},
             |    "bankExaminePass":${bankCountTop.body.find(i => i.getString("ip_province") == count.getString("ip_province")).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")}
             |}
           """.stripMargin
      }.mkString("[", ",", "]")
      p.success(Resp.success(JsonHelper.toJson(
        s"""
           |{
           |    "visitorCountTop":$visitorCountTopResult,
           |    "selfCountTop":$selfCountTopResult
           |}
         """.stripMargin)))
    }
  }
}
