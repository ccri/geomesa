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

  class WrappedCallabled[T](callable: Callable[T], timeout: Int) extends Callable[T] {
    var future: Future[T] = _
    override def call(): T = {
      try {
        // set timer
        while (future == null) { }
        val canceller = new TimerTask {
          override def run(): Unit = {
            future.cancel(true)
          }
        }
        interruptTimer.schedule(canceller, timeout)
        val ret = callable.call()
        println("Didn't timeout; cancelling canceller")
        canceller.cancel()
        ret
      } finally {
        future = null
      }
    }
  }

  class TimeOutExecutorService2(inputDelegate: ExecutorService, timeout: Int) extends ForwardingExecutorService {
    override def delegate(): ExecutorService = inputDelegate

    override def submit[T](task: Callable[T]): Future[T] = {

      val wrappedCallable = new WrappedCallabled(task, timeout)
      println(s"Before submitting on thread $printThread")
      val fut: Future[T] = delegate().submit(wrappedCallable)
      println(s"After submitting on thread $printThread")

      wrappedCallable.future = fut

      fut
    }
  }

  val workQueue2 = new LinkedBlockingQueue[Runnable](1000)
  val boundedQueue2 = new ArrayBlockingQueue[Runnable](1000)
  val delegate2 = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, boundedQueue)
  val toes2 = new TimeOutExecutorService2(delegate, 10000)
  val work2 = new Work(0)
  toes2.submit(work2).get
  toes2.submit(new Work(15)).get

  val works2 = (0 to 20).map(i => new Work(i))
  val submissions2 = works2.map(toes2.submit(_))
  submissions2.foreach{_.get}


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
