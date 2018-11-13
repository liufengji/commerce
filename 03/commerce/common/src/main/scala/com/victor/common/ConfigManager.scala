package com.victor.common

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}

/**
  * 配置管理器
  */
object ConfigManager {

  private val params = new Parameters()

  private val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties()
    .setFileName("commerce.properties"))

  val config = builder.getConfiguration()

}
