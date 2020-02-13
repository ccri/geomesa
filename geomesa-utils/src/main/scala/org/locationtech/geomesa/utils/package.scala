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

  class TimeOutFuture[T](timeout: Int, callable: Callable[T]) extends ForwardingFuture[T] {
    override def get(): T = {
      println("Called get!  Delegating to get without Timeout")
      get(timeout, TimeUnit.MILLISECONDS)
    }

    override def delegate(): Future[T] =  new FutureTask[T](callable)
  }

  val interruptTimer = new Timer()

  class InterruptibleRunnable[T](timeout: Int, future: FutureTask[T]) extends ForwardingFuture[T] with Runnable {

    override def get(): T = {
      println("Shorter timeout called get!  Delegating to get without Timeout")
      get(timeout / 2, TimeUnit.MILLISECONDS)
    }


    override def run(): Unit = {
      val thread = Thread.currentThread()
      val killer = new TimerTask {
        override def run(): Unit = {
          println("Interrupting thread")
          thread.interrupt()
        }
      }
      interruptTimer.schedule(killer, timeout)
      println(s"Calling future.run.  We are probably losing. $printThread")
      future.run
    }

    override def delegate(): Future[T] = future
  }

  class TimeOutExecutorService(inputDelegate: ExecutorService, timeout: Int) extends ForwardingExecutorService {
    override def delegate(): ExecutorService = inputDelegate

    override def submit[T](task: Callable[T]): Future[T] = {
      val future =  new FutureTask[T](task)
      val toFuture = new InterruptibleRunnable[T](10000, future)

      println(s"Before submitting on thread $printThread")
      delegate().execute(toFuture)
      println(s"After submitting on thread $printThread")

      toFuture
    }
  }


  class Work(i: Int) extends Callable[Long] {
    override def call(): Long = {
      try {
        println(s"Starting work for $i. ${printThread}")
        Thread.sleep(30000)
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
