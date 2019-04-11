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

import { HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, throwError, of, empty } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { OperationType } from 'src/app/types/operationType';
import { ExecutedLogicalPlan, Operation, OperationDetails, AttributeRef, DataType, Transition } from 'src/app/generated/models';
import { CytoscapeGraphVM } from 'src/app/viewModels/cytoscape/cytoscapeGraphVM';
import { ExecutedLogicalPlanVM } from 'src/app/viewModels/executedLogicalPlanVM';
import { CytoscapeOperationVM } from 'src/app/viewModels/cytoscape/cytoscapeOperationVM';
import * as _ from 'lodash';
import { ExecutionPlanControllerService, OperationDetailsControllerService } from 'src/app/generated/services';
import { StrictHttpResponse } from 'src/app/generated/strict-http-response';
import { ConfigService } from '../config/config.service';
import { OperationDetailsVM } from 'src/app/viewModels/operationDetailsVM';
import { AttributeVM } from 'src/app/viewModels/attributeVM';
import { DataTypeVM } from 'src/app/viewModels/dataTypeVM';
import { GenericDataTypeVM } from 'src/app/viewModels/GenericDataTypeVM';
import { PropertyType } from 'src/app/types/propertyType';


@Injectable({
  providedIn: 'root'
})

export class LineageGraphService {

  detailsInfo: OperationDetailsVM

  executedLogicalPlan: ExecutedLogicalPlanVM

  constructor(
    private executionPlanControllerService: ExecutionPlanControllerService,
    private operationDetailsControllerService: OperationDetailsControllerService
  ) {
    executionPlanControllerService.rootUrl = ConfigService.settings.apiUrl
    operationDetailsControllerService.rootUrl = ConfigService.settings.apiUrl
  }

  public getExecutedLogicalPlan(executionPlanId: string): Observable<ExecutedLogicalPlanVM> {
    return this.executionPlanControllerService.lineageUsingGETResponse(executionPlanId).pipe(
      map(response => {
        this.executedLogicalPlan = this.toLogicalPlanView(response)
        return this.executedLogicalPlan
      }),
      catchError(this.handleError)
    )
  }

  private toLogicalPlanView = (executedLogicalPlanHttpResponse: StrictHttpResponse<ExecutedLogicalPlan>): ExecutedLogicalPlanVM => {
    const cytoscapeGraphVM = {} as CytoscapeGraphVM
    cytoscapeGraphVM.nodes = []
    cytoscapeGraphVM.edges = []
    _.each(executedLogicalPlanHttpResponse.body.plan.nodes, (node: Operation) => {
      let cytoscapeOperation = {} as CytoscapeOperationVM
      cytoscapeOperation._type = node._type
      cytoscapeOperation.id = node._id
      cytoscapeOperation._id = node._id
      cytoscapeOperation.name = node.name
      cytoscapeOperation.color = this.getColorFromOperationType(node.name)
      cytoscapeOperation.icon = this.getIconFromOperationType(node.name)
      cytoscapeGraphVM.nodes.push({ data: cytoscapeOperation })
    })
    _.each(executedLogicalPlanHttpResponse.body.plan.edges, (edge: Transition) => {
      cytoscapeGraphVM.edges.push({ data: edge })
    })
    const executedLogicalPlanVM = {} as ExecutedLogicalPlanVM
    executedLogicalPlanVM.execution = executedLogicalPlanHttpResponse.body.execution
    executedLogicalPlanVM.plan = cytoscapeGraphVM
    return executedLogicalPlanVM

  }

  public getDetailsInfo(nodeId: string): Observable<OperationDetailsVM> {
    if (!nodeId) {
      this.detailsInfo = null
      return empty()
    }
    return this.operationDetailsControllerService.operationUsingGETResponse(nodeId).pipe(
      map(response => {
        this.detailsInfo = this.toOperationDetailsView(response)
        return this.detailsInfo
      }),
      catchError(this.handleError)
    )
  }

  public toOperationDetailsView = (operationDetailsVMHttpResponse: StrictHttpResponse<OperationDetails>): OperationDetailsVM => {

    const operationDetailsVm = {} as OperationDetailsVM
    operationDetailsVm.inputs = operationDetailsVMHttpResponse.body.inputs
    operationDetailsVm.output = operationDetailsVMHttpResponse.body.output
    operationDetailsVm.operation = operationDetailsVMHttpResponse.body.operation

    let schemas: Array<Array<AttributeVM>> = []
    _.each(operationDetailsVMHttpResponse.body.schemas, (attributeRefArray: Array<AttributeRef>) => {
      let attributes = _.map(attributeRefArray, attRef => {
        return this.getAttribute(attRef.dataTypeKey, operationDetailsVMHttpResponse.body.schemasDefinition, attributeRefArray, attRef.name)
      })
      schemas.push(attributes)
    })
    operationDetailsVm.schemas = schemas
    return operationDetailsVm
  }

  public getAttribute = (attributeId: string, schemaDefinition: Array<DataType>, attributeRefArray: Array<AttributeRef>, attributeName: string = null): AttributeVM => {

    let dataType = this.getDataType(schemaDefinition, attributeId)
    let attribute = {} as AttributeVM
    let dataTypeVM = {} as DataTypeVM
    dataTypeVM._type = dataType._type
    dataTypeVM.name = dataType.name

    switch (dataType._type) {
      case PropertyType.Simple:
        attribute.name = attributeName ? attributeName : dataType._type
        attribute.dataType = dataTypeVM
        return attribute
      case PropertyType.Array:
        attribute.name = attributeName
        dataTypeVM.elementDataType = this.getAttribute(dataType.elementDataTypeKey, schemaDefinition, attributeRefArray, attributeName)
        dataTypeVM.name = PropertyType.Array
        attribute.dataType = dataTypeVM
        return attribute
      case PropertyType.Struct:
        attribute.name = attributeName
        dataTypeVM.children = [] as Array<AttributeVM>
        _.each(dataType.fields[1], (attributeRef: AttributeRef) => {
          dataTypeVM.children.push(this.getAttribute(attributeRef.dataTypeKey, schemaDefinition, attributeRefArray, attributeRef.name))
        })
        dataTypeVM.name = PropertyType.Struct
        attribute.dataType = dataTypeVM
        return attribute
    }
  }

  private getDataType(schemaDefinition: Array<DataType>, dataTypeId: string): GenericDataTypeVM {
    return _.find(schemaDefinition, function (schemaDef: GenericDataTypeVM) {
      return schemaDef._key == dataTypeId
    })
  }


  public getIconFromOperationType(operation: string): number {
    switch (operation) {
      case OperationType.Projection: return 0xf13a
      case OperationType.BatchRead: return 0xf085
      case OperationType.LogicalRelation: return 0xf1c0
      case OperationType.StreamRead: return 0xf085
      case OperationType.Join: return 0xf126
      case OperationType.Union: return 0xf0c9
      case OperationType.Generic: return 0xf0c8
      case OperationType.Filter: return 0xf0b0
      case OperationType.Sort: return 0xf161
      case OperationType.Aggregate: return 0xf1ec
      case OperationType.WriteCommand: return 0xf0c7
      case OperationType.BatchWrite: return 0xf0c7
      case OperationType.StreamWrite: return 0xf0c7
      case OperationType.Alias: return 0xf111
      default: return 0xf15b
    }
  }

  public getColorFromOperationType(operation: string): string {
    switch (operation) {
      case OperationType.Projection: return "#337AB7"
      case OperationType.BatchRead: return "#337AB7"
      case OperationType.LogicalRelation: return "#e39255"
      case OperationType.StreamRead: return "#337AB7"
      case OperationType.Join: return "#e39255"
      case OperationType.Union: return "#337AB7"
      case OperationType.Generic: return "#337AB7"
      case OperationType.Filter: return "#F04100"
      case OperationType.Sort: return "#E0E719"
      case OperationType.Aggregate: return "#008000"
      case OperationType.BatchWrite: return "#e39255"
      case OperationType.WriteCommand: return "#e39255"
      case OperationType.StreamWrite: return "#e39255"
      case OperationType.Alias: return "#337AB7"
      default: return "#808080"
    }
  }


  private handleError(err: HttpErrorResponse) {
    let errorMessage = ''
    if (err.error instanceof ErrorEvent) {
      // A client-side or network error occurred. Handle it accordingly.
      errorMessage = `An error occurred: ${err.error.message}`
    } else {
      // The backend returned an unsuccessful response code.
      // The response body may contain clues as to what went wrong,
      errorMessage = `Server returned code: ${err.status}, error message is: ${err.message}`
    }
    return throwError(errorMessage)
  }

}
