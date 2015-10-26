package com.asto.dop.core

import com.asto.dop.core.helper.{DBHelper, HttpHelper}
import com.asto.dop.core.module.EventBus
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core._
import io.vertx.core.http.{HttpServer, HttpServerOptions}
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient

/**
 * 系统启动类，负责启动Http服务、数据库连接池，注册Binlog监控，加载IP地址库并完成系统启动
 */
class Startup extends AbstractVerticle with LazyLogging {

  var mysqlMonitor: BinaryLogClient = _

  /**
   * 启动入口
   */
  override def start(): Unit = {
    Global.vertx = vertx
    Global.config = config()

    HttpHelper.httpClient = vertx.createHttpClient()

    startHttpServer()
    startDBClient()
    startMySQLMonitor()
    loadIPData()
    EventBus.init()
  }

  private def startHttpServer(): Unit = {
    val host = Global.config.getJsonObject("http").getString("host")
    val port = Global.config.getJsonObject("http").getInteger("port")
    vertx.createHttpServer(new HttpServerOptions().setCompressionSupported(true).setTcpKeepAlive(true))
      //注册了自定义路由器：HttpRouter
      .requestHandler(new HttpRouter).listen(port, host, new Handler[AsyncResult[HttpServer]] {
      override def handle(event: AsyncResult[HttpServer]): Unit = {
        if (event.succeeded()) {
          logger.info(s"DOP core app http start successful. http://$host:$port/")
        } else {
          logger.error("Http start fail .", event.cause())
        }
      }
    })
  }

  private def startDBClient(): Unit = {
    val driverClass = Global.config.getJsonObject("db").getString("driver_class")
    val jdbc = Global.config.getJsonObject("db").getString("jdbc")
    val userName = Global.config.getJsonObject("db").getString("userName")
    val userPassword = Global.config.getJsonObject("db").getString("userPassword")
    val maxPoolSize = Global.config.getJsonObject("db").getInteger("max_pool_size")
    DBHelper.dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", jdbc)
      .put("driver_class", driverClass)
      .put("user", userName)
      .put("password", userPassword)
      .put("max_pool_size", maxPoolSize))
  }

  private def startMySQLMonitor(): Unit = {
    val host = Global.config.getJsonObject("binlog").getString("host")
    val port = Global.config.getJsonObject("binlog").getInteger("port")
    val userName = Global.config.getJsonObject("binlog").getString("userName")
    val userPassword = Global.config.getJsonObject("binlog").getString("userPassword")
    val monitorTables = Global.config.getJsonObject("binlog").getString("monitorTables").split(",")
    mysqlMonitor = new BinaryLogClient(host, port, userName, userPassword)
    //注册了自定义路由器：MySQLBinLogRouter
    mysqlMonitor.registerEventListener(new BinLogRouter(monitorTables))
    logger.info(s"DOP core app monitor mysql $host:$port")
    new Thread(new Runnable {
      override def run(): Unit = {
        mysqlMonitor.connect()
      }
    }).start()
  }

  private def loadIPData(): Unit = {
    IP.enableFileWatch = true
    IP.load(Global.config.getString("ip_path"))
  }

  /**
   * 关闭操作
   */
  override def stop(): Unit = {
    logger.info(s"DOP core app stopped , Bye .")
    mysqlMonitor.disconnect()
  }

}
