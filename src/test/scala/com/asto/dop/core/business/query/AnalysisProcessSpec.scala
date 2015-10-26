package com.asto.dop.core.business.query

import java.util.concurrent.CountDownLatch

import com.asto.dop.core.business.BusinessBasicSpec
import com.asto.dop.core.module.query.AnalysisSourceTransCompProcessor

import scala.concurrent.ExecutionContext.Implicits.global

class AnalysisProcessSpec extends BusinessBasicSpec {

  test("source-trans-comp Test") {
    val cdl = new CountDownLatch(2)

    AnalysisSourceTransCompProcessor.summaryProcess(Map("start" -> "20151022", "end" -> "20151024")).onSuccess {
      case resultResp =>
        assert(resultResp)
        cdl.countDown()
    }

    AnalysisSourceTransCompProcessor.detailProcess(Map("start" -> "20151022", "end" -> "20151024")).onSuccess {
      case resultResp =>
        assert(resultResp)
        cdl.countDown()
    }

    cdl.await()
  }


}

