/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package za.co.absa.spline.gateway.rest.converter

import org.springframework.stereotype.Component
import za.co.absa.spline.gateway.rest.model.OperationDetails
import za.co.absa.spline.persistence.model.{ArrayDataType, DataType, SimpleDataType, StructDataType}

@Component("operationDetailsConverter")
class OperationDetailsConverter {

  def reduceSchemaDefinition(operationDetails: OperationDetails): OperationDetails = {

    val dataTypesIdToKeep = operationDetails.schemas.flatten.map(attributeRef => attributeRef.dataTypeKey.toString).toSet
    val schemaDefinitionDataTypes = operationDetails.schemasDefinition.toSet


    val schemaDef = schemaDefFilter(schemaDefinitionDataTypes, dataTypesIdToKeep)
    operationDetails.copy(schemasDefinition = schemaDef.toArray)
  }


  private def schemaDefFilter(schemaDefinitionDataTypes: Set[DataType], dataTypesIdToKeep: Set[String]): Set[DataType] = {
    var schemaDef = schemaDefinitionDataTypes.filter(dataType => {
      dataTypesIdToKeep.contains(dataType._key)
    })
    if (getAllIds(schemaDef).size != dataTypesIdToKeep.size) {
      schemaDef = schemaDefFilter(schemaDefinitionDataTypes, getAllIds(schemaDef))
    }
    schemaDef
  }

  private def getAllIds(schemaDef: Set[DataType]): Set[String] = {
    schemaDef.flatMap {
      case dt@(_: SimpleDataType) => Set(dt._key)
      case dt@(adt: ArrayDataType) => Set(dt._key, adt.elementDataTypeKey)
      case dt@(sdt: StructDataType) => sdt.fields.map(attributeRef => attributeRef.dataTypeKey).toSet ++ Set(dt._key)
    }
  }

}
