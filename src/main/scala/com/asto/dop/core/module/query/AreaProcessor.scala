package com.asto.dop.core.module.query
import java.util.Date

import com.asto.dop.core.entity.{ UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.Resp
import io.vertx.core.json.JsonObject
import scala.concurrent._
import scala.concurrent.duration._
import com.asto.dop.core.Global
import io.vertx.core.{AsyncResult, Future, Handler}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * 流量分析（以下所有的数据都为最近7天的数据）
 * 访客-自审通过率：访客，某段时间内有访问记录，不关心是不是新老访客，自审通过率，某段时间内申请并通过的记录
 */
object AreaProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    val today = df.format(new Date()).substring(0, 8).toLong
    val last7Day = today - 7
    val visitTop10CountResp = DBHelper.find(s"SELECT c_ip_province AS area,COUNT(1) AS count  from ${VisitEntity.db.TABLE_NAME} " +
      s"group by c_ip_province order by count(*) desc limit 10",
      List(),classOf[JsonObject])

    val selfTop10CountResp = DBHelper.find(s" select a.c_ip_province as area,count(1) AS count from ${UserOptEntity.db.TABLE_NAME} b," +
      s"(select  distinct a.u_user_id, a.c_ip_province from ${VisitEntity.db.TABLE_NAME} a where a.c_ip_province !='' " +
      s"and a.u_user_id !='')a where a.u_user_id = b.user_id and b.action ='self_examine_pass' and b.occur_time =? " +
      s"group by a.c_ip_province",
      List(last7Day),classOf[JsonObject])




    var vistorTop10Json = ""
    var selfTop10Json =""

    for{
      visitTop10 <- visitTop10CountResp
      selfTop10 <- selfTop10CountResp
    }yield{
      val visitList= visitTop10.body.map(i =>( i.getString("area"),i.getLong("count")))
      val selfList = selfTop10.body.map(i =>( i.getString("area"),i.getLong("count")))

      Global.vertx.executeBlocking(new Handler[Future[Void]] {
        override def handle(event: Future[Void]): Unit = {
          visitList.foreach {
            visit =>
              val findResult = Await.result(DBHelper.find(s" select count(1) AS regCount from user_opt where user_id in(select distinct a.u_user_id from visit a where a.c_ip_province=?)",
                List(visit._1), classOf[JsonObject]), 30seconds).body
              vistorTop10Json = s"""$vistorTop10Json

                  |{area:"${visit._1}+",vistorCount:"|${visit._2}|"regCount:"|${findResult.map(i => i.getLong("regCount"))}|},""""
          }
          selfList.foreach {
            self =>
              val findResult = Await.result(DBHelper.find(s"select a.c_ip_province as area,count(1) AS count from ${UserOptEntity.db.TABLE_NAME} b," +
                s"(select  distinct a.u_user_id, a.c_ip_province from ${VisitEntity.db.TABLE_NAME} a where a.c_ip_province !='' " +
                s"and a.u_user_id !='')a where a.u_user_id = b.user_id and b.action ='self_examine_pass' and b.occur_time =?  and a.c_ip_province =?" +
                s"group by a.c_ip_province)",
                List(last7Day,self._1),classOf[JsonObject]), 30 seconds).body
              selfTop10Json = s"""$selfTop10Json
                  |{area:"${self._1}+",selfCount:"|${self._2}|"bankCount:"|${findResult.map(i => i.getLong("regCount"))}|},""""
          }

          p.success(Resp.success(
            s""""Top10VistorArea":["
               |${vistorTop10Json.substring(0,vistorTop10Json.length-1)}
               |"],"Top10VistorArea":[",
               |${selfTop10Json.substring(0,selfTop10Json.length-1)}
               |""".stripMargin
          ))
        }
      }, false, new Handler[AsyncResult[Void]] {
        override def handle(event: AsyncResult[Void]): Unit = {}
      })
    }
  }
}
