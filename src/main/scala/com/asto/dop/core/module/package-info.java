/**
 * 业务操作包
 *
 * 所有业务操作都在此包下
 *
 * 目前分两大类业务：collect 采集 ， query 查询分析
 *
 * 一般而言业务大类间除实体共享外不应存在直接的业务互访，如存在需要互访的场景请：
 * 1）我的业务规划是否有问题？是否没有划清业务边界？
 * 2）如果业务规划没有问题且必须互访请使用EventBus方式，
 * @see com.asto.dop.core.module.EventBus
 */
package com.asto.dop.core.module;