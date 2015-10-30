package com.asto.dop.core.entity

import com.asto.dop.core.helper.{DBHelper, Page}
import com.ecfront.common.Resp

import scala.concurrent.Future

/**
 * 访问实体，页面访问请求会持久化到此对象
 */
case class VisitEntity() {
  // | Y | 记录主键，格式`occur_time+6位随机数`
  var id: String = _
  // | Y | 访客主键，用于区别访客的唯一标识
  var visitor_id: String = _
  // | Y | 页面访问标识，用于心跳请求，区分是否是同一次页面访问，格式`md5 32(v_url+6位随机数)`
  var pv_hash: String = _
  // | Y | 发生时间，格式`yyyyMMddHHmmss`,如 20151012100000
  var occur_time: Long = _
  // | Y | 发生小时，格式`yyyyMMddHH`,如 2015101212
  var occur_datehour: Long = _
  // | Y | 发生日期，格式`yyyyMMdd`,如 20151012
  var occur_date: Long = _
  // | Y | 发生年月，格式`yyyyMM`,如 201510
  var occur_month: Long = _
  // | Y | 发生年份，格式`yyyyMMdd`,如 2015
  var occur_year: Long = _
  //Client info 访问客户端信息
  //| Y | 客户端使用的平台，枚举：`pc/mobile` ，除pc以外的设备都归属mobile
  var c_platform: String = _
  //| Y | 客户端使用的系统，枚举：`windows/linux/mac/android/iphone/ipad/wp/otherpad/otherphone/others`
  var c_system: String = _
  // | N | 客户端的设备号，仅针对无线平台(ci_platform:mobile)，可用于判断无线平台的新/老访客
  var c_device_id: String = _
  // | Y | 客户端使用的IP，目前只允许IPv4地址
  var c_ipv4: String = _
  //| Y | 客户端使用IP或gps解析到的完整地址，优先使用gps数据
  var c_ip_addr: String = _
  // | Y | 客户端使用IP或gps对应的国家，优先使用gps数据
  var c_ip_country: String = _
  // | Y | 客户端使用IP或gps对应的省，优先使用gps数据
  var c_ip_province: String = _
  // | Y | 客户端使用IP或gps对应的城市，优先使用gps数据
  var c_ip_city: String = _
  // | Y | 客户端使用IP或gps对应的县，优先使用gps数据
  var c_ip_county: String = _
  // | Y | 客户端使用IP对应的运营商
  var c_ip_isp: String = _
  // | N | 客户端使用GPS位置
  var c_gps: String = _
  //User info 用户信息
  // | N | 用户id，要求是真实用户表的主键或可以与之映射，未登录用户此字段为空
  var u_user_id: String = _
  // | N | cookie中用于标识用户唯一性的id值，所有通过浏览器访问的记录都应存在此值
  var u_cookie_id: String = _
  // | N | cookies信息，所有通过浏览器访问的记录都应存在此值
  var u_cookies: String = _
  //Visit info 访问信息
  // | Y | 是否是新访客，1表示是，0表示否，此定义见`述语`章节
  var v_new_visitor: Boolean = _
  // | Y | 访问转入来源，如：直接访问、百度推广、91助手app等
  var v_source: String = _
  // | N | 访问referer，所有通过浏览器访问的记录都应存在此值
  var v_referer: String = _
  // | Y | 访问URL，用浏览器而言为完整的网页地址，对APP而言为一个遵从URL规范的可标识唯一资源的地址，格式类似`res://<some path>/<res id>/`
  var v_url: String = _
  // | Y | 访问URL的path部分
  var v_url_path: String = _
  // | Y | 访问行为，用于记录此访问的业务含义，如：绑店、申请xx等
  var v_action: String = _
  // | N | 页面停留时间（s）
  var v_residence_time: Long = _
}

object VisitEntity {

  val FLAG_PLATFORM_MOBILE = "mobile"
  val FLAG_PLATFORM_PC = "pc"
  val platformEnum = Set(FLAG_PLATFORM_PC, FLAG_PLATFORM_MOBILE)
  val systemEnum = Set("windows", "linux", "mac", "android", "iphone", "ipad", "wp", "otherpad", "otherphone", "others")

  object db {

    val TABLE_NAME = "visit"

    def init(): Future[Resp[Void]] = {
      DBHelper.update {
        s"""CREATE TABLE IF NOT EXISTS $TABLE_NAME
            |(
            |    id varchar(20) NOT NULL COMMENT '记录（形式上的）主键，格式`occur_time+6位随机数` ' ,
            |    visitor_id varchar(200) NOT NULL COMMENT '访客主键，用于区别访客的唯一标识' ,
            |    pv_hash varchar(50) NOT NULL COMMENT '页面访问标识，用于心跳请求，区分是否是同一次页面访问，格式`hash32(v_url+6位随机数)`' ,
            |    occur_time BIGINT NOT NULL COMMENT '发生时间，格式`yyyyMMddHHmmss`,如 20151012100000' ,
            |    occur_datehour INT NOT NULL COMMENT '发生小时，格式`yyyyMMddHH`,如 2015101212' ,
            |    occur_date INT NOT NULL COMMENT '发生日期，格式`yyyyMMdd`,如 20151012' ,
            |    occur_month INT NOT NULL COMMENT '发生年月，格式`yyyyMM`,如 201510' ,
            |    occur_year INT NOT NULL COMMENT '发生年份，格式`yyyyMMdd`,如 2015' ,
            |    c_platform varchar(10) NOT NULL COMMENT '客户端使用的平台，枚举：`pc/mobile` ，除pc以外的设备都归属mobile' ,
            |    c_system varchar(10) NOT NULL COMMENT '客户端使用的系统，枚举：`windows/linux/mac/android/iphone/ipad/wp/otherpad/otherphone/others`' ,
            |    c_device_id varchar(200) NOT NULL COMMENT '客户端的设备号，仅针对无线平台(ci_platform:mobile)，可用于判断无线平台的新/老访客' ,
            |    c_ipv4 varchar(15) NOT NULL COMMENT '客户端使用的IP，目前只允许IPv4地址' ,
            |    c_ip_addr varchar(1000) NOT NULL COMMENT '客户端使用IP或gps解析到的完整地址，优先使用gps数据' ,
            |    c_ip_country varchar(10) NOT NULL COMMENT '客户端使用IP或gps对应的国家，优先使用gps数据' ,
            |    c_ip_province varchar(10) NOT NULL COMMENT '客户端使用IP或gps对应的省，优先使用gps数据' ,
            |    c_ip_city varchar(10) NOT NULL COMMENT '客户端使用IP或gps对应的城市，优先使用gps数据' ,
            |    c_ip_county varchar(10) NOT NULL COMMENT '客户端使用IP或gps对应的县，优先使用gps数据' ,
            |    c_ip_isp varchar(20) NOT NULL COMMENT '客户端使用IP对应的运营商' ,
            |    c_gps varchar(100) NOT NULL COMMENT '客户端使用GPS位置' ,
            |    u_user_id varchar(255) NOT NULL COMMENT '用户id，要求是真实用户表的主键或可以与之映射，未登录用户此字段为空' ,
            |    u_cookie_id varchar(255) NOT NULL COMMENT 'cookie中用于标识用户唯一性的id值，所有通过浏览器访问的记录都应存在此值' ,
            |    u_cookies varchar(8000) NOT NULL COMMENT 'cookies信息，所有通过浏览器访问的记录都应存在此值' ,
            |    v_new_visitor BOOLEAN NOT NULL COMMENT '是否是新访客，1表示是，0表示否，此定义见' ,
            |    v_source varchar(255) NOT NULL COMMENT '访问转入来源，如：直接访问、百度推广、91助手app等' ,
            |    v_referer varchar(5000) NOT NULL COMMENT '访问referer，所有通过浏览器访问的记录都应存在此值' ,
            |    v_url varchar(5000) NOT NULL COMMENT '访问URL，用浏览器而言为完整的网页地址，对APP而言为一个遵从URL规范的可标识唯一资源的地址，格式类似`res://<some path>/<res id>/`' ,
            |    v_url_path varchar(255) NOT NULL COMMENT '访问URL的path部分' ,
            |    v_action varchar(200) NOT NULL COMMENT '访问行为，用于记录此访问的业务含义，如：绑店、申请xx等' ,
            |    v_residence_time INT NOT NULL COMMENT '页面停留时间（s）' ,
            |    PRIMARY KEY(id) ,
            |    INDEX i_visitor_id(visitor_id) ,
            |    INDEX i_pv_hash(pv_hash) ,
            |    INDEX i_occur_time(occur_time) ,
            |    INDEX i_occur_date(occur_date) ,
            |    INDEX i_occur_datehour(occur_datehour) ,
            |    INDEX i_c_platform(c_platform) ,
            |    INDEX i_c_system(c_system) ,
            |    INDEX i_c_device_id(c_device_id) ,
            |    INDEX i_c_ipv4(c_ipv4) ,
            |    INDEX i_c_ip_country(c_ip_country) ,
            |    INDEX i_c_ip_province(c_ip_province) ,
            |    INDEX i_c_ip_city(c_ip_city) ,
            |    INDEX i_c_ip_county(c_ip_county) ,
            |    INDEX i_c_ip_isp(c_ip_isp) ,
            |    INDEX i_c_gps(c_gps) ,
            |    INDEX i_u_user_id(u_user_id) ,
            |    INDEX i_u_cookie_id(u_cookie_id) ,
            |    INDEX i_v_new_visitor(v_new_visitor) ,
            |    INDEX i_v_source(v_source) ,
            |    INDEX i_v_url_path(v_url_path) ,
            |    INDEX i_v_action(v_action) ,
            |    INDEX im_ot_pf(occur_time,c_platform) ,
            |    INDEX im_ot_st(occur_time,c_system) ,
            |    INDEX im_ot_ip_prov(occur_time,c_ip_province)
            |)ENGINE=innodb DEFAULT CHARSET=utf8;
            | """.stripMargin
      }
    }

    def save(obj: VisitEntity): Future[Resp[Void]] = {
      DBHelper.update(
        s"""
           |INSERT INTO $TABLE_NAME (
           |	id ,visitor_id,pv_hash,  occur_time ,occur_datehour, occur_date ,occur_month,occur_year,
           |	c_platform , c_system ,  c_device_id , c_ipv4 , c_ip_addr , c_ip_country , c_ip_province , c_ip_city , c_ip_county , c_ip_isp , c_gps ,
           |	u_user_id , u_cookie_id , u_cookies ,
           |	v_new_visitor , v_source , v_referer , v_url , v_url_path , v_action , v_residence_time
           |	) VALUES (
           |	 ? , ? ,?, ?, ? , ? , ? , ? , ? , ? , ? , ? ,? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?
           |	)
        """.stripMargin,
        List(
          obj.id, obj.visitor_id, obj.pv_hash, obj.occur_time,obj.occur_datehour, obj.occur_date, obj.occur_month, obj.occur_year,
          obj.c_platform, obj.c_system, obj.c_device_id, obj.c_ipv4, obj.c_ip_addr, obj.c_ip_country, obj.c_ip_province, obj.c_ip_city, obj.c_ip_county, obj.c_ip_isp, obj.c_gps,
          obj.u_user_id, obj.u_cookie_id, obj.u_cookies,
          obj.v_new_visitor, obj.v_source, obj.v_referer, obj.v_url, obj.v_url_path, obj.v_action, obj.v_residence_time
        )
      )
    }

    def update(newValues: String, condition: String, parameters: List[Any]): Future[Resp[Void]] = {
      DBHelper.update(
        s"UPDATE $TABLE_NAME Set $newValues WHERE $condition",
        parameters
      )
    }

    def delete(id: String): Future[Resp[Void]] = {
      DBHelper.update(
        s"DELETE FROM $TABLE_NAME WHERE id = ? ",
        List(id)
      )
    }

    def delete(condition: String, parameters: List[Any]): Future[Resp[Void]] = {
      DBHelper.update(
        s"DELETE FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def get(id: String): Future[Resp[VisitEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE id = ? ",
        List(id),
        classOf[VisitEntity]
      )
    }

    def get(condition: String, parameters: List[Any]): Future[Resp[VisitEntity]] = {
      DBHelper.get(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[VisitEntity]
      )
    }

    def exist(condition: String, parameters: List[Any]): Future[Resp[Boolean]] = {
      DBHelper.exist(
        s"SELECT 1 FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    def find(condition: String = " 1=1 ", parameters: List[Any] = List()): Future[Resp[List[VisitEntity]]] = {
      DBHelper.find(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        classOf[VisitEntity]
      )
    }

    def page(condition: String = " 1=1 ", parameters: List[Any] = List(), pageNumber: Long = 1, pageSize: Int = 10): Future[Resp[Page[VisitEntity]]] = {
      DBHelper.page(
        s"SELECT * FROM $TABLE_NAME WHERE $condition ",
        parameters,
        pageNumber, pageSize,
        classOf[VisitEntity]
      )
    }

    def count(condition: String = " 1=1 ", parameters: List[Any] = List()): Future[Resp[Long]] = {
      DBHelper.count(
        s"SELECT count(1) FROM $TABLE_NAME WHERE $condition ",
        parameters
      )
    }

    init()

  }


}
