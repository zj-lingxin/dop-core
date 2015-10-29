package com.asto.dop.core.module.query

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 实时概况
 */
object RealTimeSourceProcessor extends QueryProcessor {


  def platformProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]

    val pcVisitorCountResp = DBHelper.find(s"SELECT v_source,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE c_platform = ? GROUP BY  v_source", List(VisitEntity.FLAG_PLATFORM_PC), classOf[JsonObject])
    val mobileVisitorCountResp = DBHelper.find(s"SELECT v_source,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE c_platform = ? GROUP BY  v_source", List(VisitEntity.FLAG_PLATFORM_MOBILE), classOf[JsonObject])
    val pcRegisterCountResp = DBHelper.find(s"SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? AND platform = ? GROUP BY  source", List(UserOptEntity.FLAG_REGISTER, VisitEntity.FLAG_PLATFORM_PC), classOf[JsonObject])
    val mobileRegisterCountResp = DBHelper.find(s"SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? AND platform = ? GROUP BY  source", List(UserOptEntity.FLAG_REGISTER, VisitEntity.FLAG_PLATFORM_MOBILE), classOf[JsonObject])
    val pcApplyCountResp = DBHelper.find(s"SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? AND platform = ? GROUP BY  source", List(UserOptEntity.FLAG_APPLY, VisitEntity.FLAG_PLATFORM_PC), classOf[JsonObject])
    val mobileApplyCountResp = DBHelper.find(s"SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? AND platform = ? GROUP BY  source", List(UserOptEntity.FLAG_APPLY, VisitEntity.FLAG_PLATFORM_MOBILE), classOf[JsonObject])
    val pcSelfCountResp = DBHelper.find(s"SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? AND platform = ? GROUP BY  source", List(UserOptEntity.FLAG_SELF_EXAMINE_PASS, VisitEntity.FLAG_PLATFORM_PC), classOf[JsonObject])
    val mobileSelfCountResp = DBHelper.find(s"SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? AND platform = ? GROUP BY  source", List(UserOptEntity.FLAG_SELF_EXAMINE_PASS, VisitEntity.FLAG_PLATFORM_MOBILE), classOf[JsonObject])

    for {
      pcVisitorCount <- pcVisitorCountResp
      mobileVisitorCount <- mobileVisitorCountResp
      pcRegisterCount <- pcRegisterCountResp
      mobileRegisterCount <- mobileRegisterCountResp
      pcApplyCount <- pcApplyCountResp
      mobileApplyCount <- mobileApplyCountResp
      pcSelfCount <- pcSelfCountResp
      mobileSelfCount <- mobileSelfCountResp
    } yield {
      //key:source value:访客数 注册数 申请数 自审通过数
      val pcSource = collection.mutable.Map[String,Array[Long]]()
      val mobileSource = collection.mutable.Map[String,Array[Long]]()
      pcVisitorCount.body.foreach {
        count =>
          pcSource += count.getString("v_source") -> Array(count.getLong("count"), 0, 0, 0)
      }
      pcRegisterCount.body.foreach {
        count =>
          if (!pcSource.contains(count.getString("source"))) {
            pcSource += count.getString("source") -> Array(0, count.getLong("count"), 0, 0)
          } else {
            pcSource(count.getString("source"))(1) = count.getLong("count")
          }
      }
      pcApplyCount.body.foreach {
        count =>
          if (!pcSource.contains(count.getString("source"))) {
            pcSource += count.getString("source") -> Array(0, 0, count.getLong("count"), 0)
          } else {
            pcSource(count.getString("source"))(2) = count.getLong("count")
          }
      }
      pcSelfCount.body.foreach {
        count =>
          if (!pcSource.contains(count.getString("source"))) {
            pcSource += count.getString("source") -> Array(0, 0, 0, count.getLong("count"))
          } else {
            pcSource(count.getString("source"))(3) = count.getLong("count")
          }
      }
      mobileVisitorCount.body.foreach {
        count =>
          mobileSource += count.getString("v_source") -> Array(count.getLong("count"), 0, 0, 0)
      }
      mobileRegisterCount.body.foreach {
        count =>
          if (!mobileSource.contains(count.getString("source"))) {
            mobileSource += count.getString("source") -> Array(0, count.getLong("count"), 0, 0)
          } else {
            mobileSource(count.getString("source"))(1) = count.getLong("count")
          }
      }
      mobileApplyCount.body.foreach {
        count =>
          if (!mobileSource.contains(count.getString("source"))) {
            mobileSource += count.getString("source") -> Array(0, 0, count.getLong("count"), 0)
          } else {
            mobileSource(count.getString("source"))(2) = count.getLong("count")
          }
      }
      mobileSelfCount.body.foreach {
        count =>
          if (!mobileSource.contains(count.getString("source"))) {
            mobileSource += count.getString("source") -> Array(0, 0, 0, count.getLong("count"))
          } else {
            mobileSource(count.getString("source"))(3) = count.getLong("count")
          }
      }
      p.success(Resp.success(JsonHelper.toJson(
        s"""
           |{
           |    "pc":${pcSource.map(i => s"""{"source":"${i._1}","count":${i._2.mkString("[", ",", "]")}}""").mkString("[", ",", "]")},
           |    "mobile":${mobileSource.map(i => s"""{"source":"${i._1}","count":${i._2.mkString("[", ",", "]")}}""").mkString("[", ",", "]")}
           |}
         """.stripMargin)))
    }
    p.future
  }

  def areaProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]

    val visitorTop10Resp = DBHelper.find(s"SELECT c_ip_province,c_ip_city,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} GROUP BY c_ip_province,c_ip_city ORDER BY count DESC LIMIT 10", List(), classOf[JsonObject])
    val registerTop10Resp = DBHelper.find(s"SELECT ip_province,ip_city,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? GROUP BY  ip_province,ip_city ORDER BY count DESC LIMIT 10", List(UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
    val applyTop10Resp = DBHelper.find(s"SELECT ip_province,ip_city,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? GROUP BY  ip_province,ip_city ORDER BY count DESC LIMIT 10", List(UserOptEntity.FLAG_APPLY), classOf[JsonObject])
    val selfTop10Resp = DBHelper.find(s"SELECT ip_province,ip_city,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE action = ? GROUP BY  ip_province,ip_city ORDER BY count DESC LIMIT 10", List(UserOptEntity.FLAG_SELF_EXAMINE_PASS), classOf[JsonObject])

    for {
      visitorTop10 <- visitorTop10Resp
      registerTop10 <- registerTop10Resp
      applyTop10 <- applyTop10Resp
      selfTop10 <- selfTop10Resp
    } yield {
      p.success(Resp.success(JsonHelper.toJson(
        s"""
           |{
           |    "visitorTop10":${visitorTop10.body.map(i => s"""{"area":"${i.getString("c_ip_province") + " " + i.getString("c_ip_city")}","count":${i.getLong("count")}}""").mkString("[", ",", "]")},
           |    "registerTop10":${registerTop10.body.map(i => s"""{"area":"${i.getString("ip_province") + " " + i.getString("ip_city")}","count":${i.getLong("count")}}""").mkString("[", ",", "]")},
           |    "applyTop10":${applyTop10.body.map(i => s"""{"area":"${i.getString("ip_province") + " " + i.getString("ip_city")}","count":${i.getLong("count")}}""").mkString("[", ",", "]")},
           |    "selfTop10":${selfTop10.body.map(i => s"""{"area":"${i.getString("ip_province") + " " + i.getString("ip_city")}","count":${i.getLong("count")}}""").mkString("[", ",", "]")}
           |}
         """.stripMargin)))
    }
    p.future
  }

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {}
}
