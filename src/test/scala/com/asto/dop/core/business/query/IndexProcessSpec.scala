package com.asto.dop.core.business.query

import java.util.concurrent.CountDownLatch

import com.asto.dop.core.module.query.{RealTimeProcessor, ThirtyDaysProcessor}

import scala.concurrent.ExecutionContext.Implicits.global

class IndexProcessSpec extends QueryBasicSpec {

  test("Real Time Test") {
    val cdl = new CountDownLatch(1)

    RealTimeProcessor.process(Map()).onSuccess {
      case resultResp =>
        assert(resultResp)
        cdl.countDown()
    }

    cdl.await()
  }

  test("Thirty Days Test") {
    val cdl = new CountDownLatch(1)

    ThirtyDaysProcessor.process(Map()).onSuccess {
      case resultResp =>
        assert(resultResp)
        cdl.countDown()
    }

    cdl.await()
  }

}

