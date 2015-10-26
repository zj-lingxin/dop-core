package com.asto.dop.core.business

import com.asto.dop.core.helper.{DBHelper, HttpHelper}
import com.asto.dop.core.{Global, IP}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.io.Source

abstract class BusinessBasicSpec extends FunSuite with BeforeAndAfter with LazyLogging {

  protected def before2(): Any = {

  }

  before {
    val config = new JsonObject(Source.fromFile(this.getClass.getResource("/").getPath + "config.json").mkString)
    Global.vertx = Vertx.vertx()
    Global.config = config
    IP.enableFileWatch = true
    IP.load(this.getClass.getResource("/").getPath + "ip.dat")
    HttpHelper.httpClient = Vertx.vertx().createHttpClient()

    startDBClient()
    BusinessAPIMock.start()
    Thread.sleep(1000)
    before2()
  }

  private def startDBClient(): Unit = {
    val driverClass = Global.config.getJsonObject("db").getString("driver_class")
    val jdbc = Global.config.getJsonObject("db").getString("jdbc")
    val userName = Global.config.getJsonObject("db").getString("userName")
    val userPassword = Global.config.getJsonObject("db").getString("userPassword")
    val maxPoolSize = Global.config.getJsonObject("db").getInteger("max_pool_size")
    DBHelper.dbClient = JDBCClient.createShared(Vertx.vertx(), new JsonObject()
      .put("url", jdbc)
      .put("driver_class", driverClass)
      .put("user", userName)
      .put("password", userPassword)
      .put("max_pool_size", maxPoolSize))
  }

}

