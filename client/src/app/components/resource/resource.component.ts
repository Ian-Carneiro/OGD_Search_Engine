import { HttpErrorResponse } from '@angular/common/http';
import { Component, OnInit } from '@angular/core';
import { PageChangedEvent } from 'ngx-bootstrap/pagination';

import { Resource } from './../../domain/resource';
import { SearchEngineService } from './../../services/search-engine.service';
import { MoreAndLessSpecificPlace } from 'src/app/domain/moreAndLessSpecificPlace';
import { ResourceId } from 'src/app/domain/resource';

@Component({
  selector: 'resource',
  templateUrl: './resource.component.html',
  styleUrls: ['./resource.component.css']
})
export class ResourceComponent implements OnInit {
  formValues: Object
  resourcesPerPage = 30
  searching: boolean
  moreAndLessSpecificPlaces: MoreAndLessSpecificPlace[]
  resources: Resource[]
  ids: string[]
  totalResources: number

  constructor(private searchEngineService: SearchEngineService) {
    this.resources = []
  }

  ngOnInit() {
    SearchEngineService.emitNewQueryDatesetLevel.subscribe(res=>{
      this.resources = []
      this.ids = []
      this.totalResources = 0
      this.moreAndLessSpecificPlaces = null,
      this.searching = false 
    })
    SearchEngineService.emitQueryResourceLevel.subscribe(formValues=>{
      SearchEngineService.emitNewQueryResourceLevel.emit(null)
      this.searching = true;
      this.resources = []
      this.ids = []
      this.totalResources = 0
      this.formValues = formValues
      this.moreAndLessSpecificPlaces = null
      this.queryResourceId(formValues)
    })
  }
  
  queryResourceId(formValues){
    this.searchEngineService.search(formValues['interval_start'], 
    formValues['interval_end'], 
    formValues['name_place'], 
    formValues['topic'],
    formValues['queryLevel'])
    .then((res: ResourceId[])=>{
      this.ids = res.map((r) => {return r.id})
      this.totalResources = this.ids.length
      this.getResources(1)
    }).catch((err:HttpErrorResponse) =>{
      switch(err.status){
        case 300:
          this.moreAndLessSpecificPlaces = err.error
        case 400:
          console.log(err)
      }
    })
  }

  queryResourceIdWhithGidPlace(formValues, gid_place){
    this.searchEngineService.searchWithPlaceId(formValues['interval_start'], 
    formValues['interval_end'], 
    gid_place, 
    formValues['topic'],
    formValues['queryLevel'])
    .then((res: ResourceId[])=>{
      this.ids = res.map((r) => {return r.id})
      this.totalResources = this.ids.length
      this.getResources(1)
    }).catch((err:HttpErrorResponse) =>{
      switch(err.status){
        case 400:
          console.log(err)
      }
    })
  }

  pageChanged(event: PageChangedEvent){
    this.searching = true
    this.getResources(event.page)
  }

  getResources(page:number){
    this.searchEngineService.getResources(
      this.ids.slice((page - 1)*this.resourcesPerPage, (this.resourcesPerPage*page)))
    .then((res:Resource[]) => {
      this.resources = res
    })
    .finally(()=>{
      this.searching = false
    })
  }

  assignPlaceIdAndSearch(gidPlace){
    this.searching = true;
    this.resources = []
    this.ids = []
    this.queryResourceIdWhithGidPlace(this.formValues, gidPlace)
  }
}
