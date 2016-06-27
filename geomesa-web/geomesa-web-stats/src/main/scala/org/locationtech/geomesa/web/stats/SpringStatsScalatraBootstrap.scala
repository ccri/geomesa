/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.stats

import javax.servlet.ServletContext

import com.typesafe.scalalogging.LazyLogging
import org.scalatra.servlet.RichServletContext
import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.web.context.ServletContextAware

import scala.beans.BeanProperty

class SpringStatsScalatraBootstrap extends ApplicationContextAware with ServletContextAware with LazyLogging {

  @BeanProperty var applicationContext: ApplicationContext = _
  @BeanProperty var servletContext: ServletContext = _
  @BeanProperty var rootPath: String = _

  implicit val swagger = new GeoMesaStatsSwagger

  def init(): Unit = {
    val richCtx = new RichServletContext(servletContext)
//    val servlets = applicationContext.getBeansOfType(classOf[GeoMesaScalatraServlet])
//    for ((name, servlet) <- servlets) {
//      val path = s"/$rootPath/${servlet.root}"
//      logger.info(s"Mounting servlet bean '$name' at path '$path'")
//      richCtx.mount(servlet, s"$path/*", s"$path")
//    }
    richCtx.mount(new GeoMesaStatsEndpoint, "/stats", "stats")
    richCtx.mount(new ResourcesApp, "/api-docs")
  }
}
