/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import java.util.{Timer, TimerTask}
import java.util.concurrent.{FutureTask, LinkedBlockingQueue, _}

import com.google.common.util.concurrent.{ForwardingExecutorService, ForwardingFuture}

import scala.collection.immutable

package object utils {

  def printThread = { s"Thread info: ${Thread.currentThread()}" }

  val interruptTimer = new Timer()

  class TimeOutExecutorService(inputDelegate: ExecutorService, timeout: Int) extends ForwardingExecutorService {
    override def delegate(): ExecutorService = inputDelegate

    override def submit[T](task: Callable[T]): Future[T] = {

      println(s"Before submitting on thread $printThread")
      val fut: Future[T] = delegate().submit(task)
      println(s"After submitting on thread $printThread")

      val canceller = new TimerTask {
        override def run(): Unit = {
          println("Called future.cancel(true)")
          fut.cancel(true)
        }
      }
      val request = interruptTimer.schedule(canceller, timeout)

      fut
    }
  }

//  class TimeOutExecutorService2(inputDelegate: ExecutorService, timeout: Int) extends ForwardingExecutorService {
//    override def delegate(): ExecutorService = inputDelegate
//
//    override def submit[T](task: Callable[T]): Future[T] = {
//
//      val wrappedCallable = new Callable[T] {
//        override def call(): T = {
//          val interruptTimer = new Timer()
//          interruptTimer.schedule(new TimerTask {
//            override def run(): Unit =
//          })
//          try {
//            task.call()
//            interruptTimer.cancel()
//          }
//
//          ???
//        }
//      }
//
//      println(s"Before submitting on thread $printThread")
//      val fut: Future[T] = delegate().submit(task)
//      println(s"After submitting on thread $printThread")
//
//      val canceller = new TimerTask {
//        override def run(): Unit = {
//          println("Called future.cancel(true)")
//          fut.cancel(true)
//        }
//      }
//      val request = interruptTimer.schedule(canceller, timeout)
//
//      fut
//    }
//  }

  class Work(i: Int) extends Callable[Long] {
    override def call(): Long = {
      try {
        println(s"Starting work for $i. ${printThread}")
        Thread.sleep(i * 1000)
        println(s"Finishing work for $i ${printThread}")
        42
      } catch {
        case t: Throwable => println(s"While working on $i Caught ${t.toString}")
          throw t
      }
    }
  }

  val workQueue = new LinkedBlockingQueue[Runnable](1000)
  val boundedQueue = new ArrayBlockingQueue[Runnable](1000)
  val delegate = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, boundedQueue)
  val toes = new TimeOutExecutorService(delegate, 10000)
  val work = new Work(0)


  val printer = new TimerTask {
    override def run(): Unit = println(s"delegate at time ${System.currentTimeMillis()}: $delegate")
  }
  val timer = new Timer()
  timer.schedule(printer, 0, 2000)


  val future = toes.submit(work)

  future.get()

  val works = (0 to 10).map(i => new Work(i))
  val submissions = works.map(toes.submit(_))
  submissions.foreach{_.get}



}
