package com.asto.dop.core.function

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

class FutureSpec extends FunSuite with BeforeAndAfter with LazyLogging {

  test("Collection future test") {
    val cdl = new CountDownLatch(1)
    /*val collection = List(opt(1), opt(2), opt(3))
    val result = collection.flatMap{
      r =>
       r.onSuccess{
         case x =>
           println(x)
       }
        r.value
    }
    println("result1=" + result)*/

    //串行
    for {
      i1 <- opt(1)
      i2 <- opt(i1)
      i3 <- opt(3)
    } yield {
      println("result2=" + (i1 + i2 + i3))
      cdl.countDown()
    }

    //并行
    val opt1 = opt(1)
    val opt2 = opt(2)
    val opt3 = opt(3)
    for {
      i1 <- opt1
      i2 <- opt2
      i3 <- opt3
    } yield {
      println("result3=" + (i1 + i2 + i3))
      cdl.countDown()
    }

    cdl.await()
  }


  def opt(i: Int): Future[Int] = {
    val p = Promise[Int]
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(3000)
        println("i=" + i)
        p.success(i)
      }
    }).start()
    p.future
  }

}

