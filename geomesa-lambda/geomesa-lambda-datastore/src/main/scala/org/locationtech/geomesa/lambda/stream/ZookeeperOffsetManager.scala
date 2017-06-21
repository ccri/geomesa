/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream

import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.EventType
import org.locationtech.geomesa.index.utils.Releasable
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.ZookeeperOffsetManager.CuratorOffsetListener

class ZookeeperOffsetManager(zookeepers: String, namespace: String = "geomesa") extends OffsetManager {

  private val client = CuratorFrameworkFactory.builder()
      .namespace(namespace)
      .connectString(zookeepers)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
  client.start()

  private val listeners = new ConcurrentHashMap[(String, OffsetListener), CuratorOffsetListener]

  override def getOffsets(topic: String): Seq[(Int, Long)] = {
    val path = offsetsPath(topic)
    if (client.checkExists().forPath(path) == null) { Seq.empty } else {
      ZookeeperOffsetManager.deserializeOffsets(client.getData.forPath(path))
    }
  }

  override def setOffsets(topic: String, offsets: Seq[(Int, Long)]): Unit = {
    val path = offsetsPath(topic)
    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }
    client.setData().forPath(path, ZookeeperOffsetManager.serializeOffsets(offsets))
  }

  override def deleteOffsets(topic: String): Unit = {
    val path = offsetsPath(topic)
    if (client.checkExists().forPath(path) != null) {
      client.delete().deletingChildrenIfNeeded().forPath(path)
    }
  }

  override def addOffsetListener(topic: String, listener: OffsetListener): Unit = {
    val path = offsetsPath(topic)
    val curatorListener = new CuratorOffsetListener(client, listener, path)
    listeners.put((topic, listener), curatorListener)
    client.checkExists().usingWatcher(curatorListener).forPath(path)
  }

  override def removeOffsetListener(topic: String, listener: OffsetListener): Unit =
    Option(listeners.remove((topic, listener))).foreach(_.close())

  override protected def acquireDistributedLock(topic: String): Releasable =
    acquireLock(topic, (lock) => { lock.acquire(); true })

  override protected def acquireDistributedLock(topic: String, timeOut: Long): Option[Releasable] =
    Option(acquireLock(topic, (lock) => lock.acquire(timeOut, TimeUnit.MILLISECONDS)))

  override def close(): Unit = client.close()

  private def offsetsPath(topic: String): String = s"/$topic/offsets"

  private def acquireLock(topic: String, acquire: (InterProcessSemaphoreMutex) => Boolean): Releasable = {
    val lock = new InterProcessSemaphoreMutex(client, s"/$topic/locks")
    if (acquire(lock)) {
      new Releasable { override def release(): Unit = lock.release() }
    } else {
      null
    }
  }
}

object ZookeeperOffsetManager {

  def serializeOffsets(offsets: Seq[(Int, Long)]): Array[Byte] =
    offsets.map { case (p, o) => s"$p:$o"}.mkString(",").getBytes(StandardCharsets.UTF_8)

  def deserializeOffsets(bytes: Array[Byte]): Seq[(Int, Long)] = {
    new String(bytes, StandardCharsets.UTF_8).split(",")
        .map(s => s.split(":") match { case Array(p, o) => (p.toInt, o.toLong)})
  }

  private class CuratorOffsetListener(client: CuratorFramework, listener: OffsetListener, path: String)
      extends CuratorWatcher with Closeable {

    private val open = new AtomicBoolean(true)

    // note: we have to re-register the watch, they are for only a single event
    override def process(event: WatchedEvent): Unit = {
      if (open.get) {
        if (event.getType == EventType.NodeDataChanged && event.getPath == path) {
          val data = client.getData.usingWatcher(this).forPath(path)
          val offsets = ZookeeperOffsetManager.deserializeOffsets(data)
          listener.offsetsChanged(offsets)
        } else {
          client.checkExists().usingWatcher(this).forPath(path)
        }
      }
    }

    override def close(): Unit = open.set(false)
  }
}