package com.flink.common.factory.mongo


import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.{ConnectorDescriptorValidator, DescriptorProperties}

class MongoValidator extends ConnectorDescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    import MongoValidator._
    properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_MONGODB, false)
    properties.validateString(CONNECTOR_HOST, false, 1)
    properties.validateString(CONNECTOR_DATABASE, false, 1)
    properties.validateString(CONNECTOR_COLLECTION, false, 1)
    properties.validateString(CONNECTOR_VERSION_VALUE, true, 1)
    properties.validateString(CONNECTOR_TABLE_TYPE, true, 1)
    properties.validateString(CONNECTOR_USER, false, 1)
    properties.validateString(CONNECTOR_PASSW, false, 1)

  }
}

object MongoValidator {
  val CONNECTOR_TYPE_VALUE_MONGODB = "mongodb"
  val CONNECTOR_VERSION_VALUE = "3.2.2"
  val CONNECTOR_HOST = "connector.host"
  val CONNECTOR_DATABASE = "connector.database"
  val CONNECTOR_COLLECTION = "connector.collection"
  val CONNECTOR_TABLE_TYPE = "table.type"
  val CONNECTOR_USER = "connector.user"
  val CONNECTOR_PASSW = "connector.passw"
}
