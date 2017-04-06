/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

class GroupBy(val attribute: Int, statCreator: () => Stat) extends Stat {
  override type S = this.type

  // TODO: Optionally add a type parameter [T] to GroupBy and replace [String, Stat] with [T, Stat]
  val groupedStats: mutable.HashMap[String, Stat] = mutable.HashMap[String, Stat]()

  /**
    * Compute statistics based upon the given simple feature.
    * This method will be called for every SimpleFeature a query returns.
    *
    * @param sf feature to evaluate
    */
  override def observe(sf: SimpleFeature): Unit = {
    val key = sf.getAttribute(attribute).toString
    groupedStats.get(key) match {
      case Some(groupedStat) => groupedStat.observe(sf)
      case None              => val newStat = statCreator()
        groupedStats.update(key, newStat)
    }
  }

  /**
    * Tries to remove the given simple feature from the compiled statistics.
    * Note: may not be possible to un-observe a feature, in which case this method will
    * have no effect.
    *
    * @param sf feature to un-evaluate
    */
  override def unobserve(sf: SimpleFeature): Unit = {
    // TODO This is basically the same as observe.
  }

  /**
    * Add another stat to this stat. Avoids allocating another object.
    *
    * @param other the other stat to add
    */
  override def +=(other: GroupBy.this.type): Unit = {
    other.groupedStats.map { case (key, stat) =>
      // TODO: Handle case where key is in other stat.
      this.groupedStats.put(key, stat)
    }
  }

  /**
    * Combine two stats into a new stat
    *
    * @param other the other stat to add
    */
  override def +(other: GroupBy.this.type): GroupBy.this.type = ???

  /**
    * Returns a json representation of the stat
    *
    * @return stat as a json string
    */
  // TODO
  override def toJson: String = {
    groupedStats.map{ case (key, stat) => "{ \"" + key + "\" : " + stat.toJson + "}" }.mkString("[",",","]")
  }

  /**
    * Necessary method used by the StatIterator. Indicates if the stat has any values or not
    *
    * @return true if stat contains values
    */
  override def isEmpty: Boolean = groupedStats.isEmpty

  /**
    * Compares the two stats for equivalence. We don't use standard 'equals' as it gets messy with
    * mutable state and hash codes
    *
    * @param other other stat to compare
    * @return true if equals
    */
  // TODO
  override def isEquivalent(other: Stat): Boolean = false

  /**
    * Clears the stat to its original state when first initialized.
    * Necessary method used by the StatIterator.
    */
  override def clear(): Unit = { // TODO!
    }
}
