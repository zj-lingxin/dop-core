package com.asto.dop.core.module.collect

/**
 * 浏览器访问请求
 */
case class BrowserVisitReq(
                            var request_id: String, // | Y | 页面中生成的6位随机编码，页面刷新重新生成 |
                            var c_platform: String, //| Y | 客户端使用的平台，枚举：`pc/mobile` ，除pc以外的设备都归属mobile |
                            var c_system: String, //| Y | 客户端使用的系统，枚举：`windows/linux/mac/android/iphone/ipad/wp/otherpad/otherphone/others`
                            var u_user_id: String, // | N | 用户id，要求是真实用户表的主键或可以与之映射，未登录用户此字段为空
                            var u_cookie_id: String, // | Y | cookie中用于标识用户唯一性的id值，所有通过浏览器访问的记录都应存在此值
                            var u_cookies: String, // | Y | cookies信息的base64编码，所有通过浏览器访问的记录都应存在此值
                            var v_source: String, // | Y | 访问转入来源，如：直接访问、百度推广、91助手app等
                            var v_referer: String, // | Y | 访问referer，所有通过浏览器访问的记录都应存在此值
                            var v_url: String, // | Y | 访问URL，用浏览器而言为完整的网页地址，对APP而言为一个遵从URL规范的可标识唯一资源的地址，格式类似`res://<some path>/<res id>/`
                            var v_action: String // | N | 访问行为，用于记录此访问的业务含义，如：绑店、申请xx等
                          )

/**
 * APP访问请求
 */
case class AppVisitReq(
                        var request_id: String, // | Y | 页面中生成的6位随机编码，页面刷新重新生成 |
                        var c_system: String, //| Y | 客户端使用的系统，枚举：`windows/linux/mac/android/iphone/ipad/wp/otherpad/otherphone/others`
                        var c_device_id: String, // | Y | 客户端的设备号，仅针对无线平台(ci_platform:mobile)，可用于判断无线平台的新/老访客
                        var c_ip_addr: String, //| Y | 客户端使用IP或gps解析到的完整地址，优先使用gps数据
                        var c_ip_country: String, // | Y | 客户端使用IP或gps对应的国家，优先使用gps数据
                        var c_ip_province: String, // | Y | 客户端使用IP或gps对应的省，优先使用gps数据
                        var c_ip_city: String, // | Y | 客户端使用IP或gps对应的城市，优先使用gps数据
                        var c_ip_county: String, // | Y | 客户端使用IP或gps对应的县，优先使用gps数据
                        var c_gps: String, // | N | 客户端使用GPS位置
                        var u_user_id: String, // | N | 用户id，要求是真实用户表的主键或可以与之映射，未登录用户此字段为空
                        var v_source: String, // | Y | 访问转入来源，如：直接访问、百度推广、91助手app等
                        var v_url_path: String, // | Y | 访问URL的path部分
                        var v_action: String // | Y | 访问行为，用于记录此访问的业务含义，如：绑店、申请xx等
                      )

