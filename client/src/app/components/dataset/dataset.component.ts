import { PageChangedEvent } from 'ngx-bootstrap/pagination';
import { Component, OnInit } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';

import { DatasetId, Dataset } from 'src/app/domain/dataset';
import { MoreAndLessSpecificPlace } from 'src/app/domain/moreAndLessSpecificPlace';
import { SearchEngineService } from 'src/app/services/search-engine.service';
import { ResourcesModalOptionsService } from 'src/app/services/resources-modal-options.service';

@Component({
  selector: 'dataset',
  templateUrl: './dataset.component.html',
  styleUrls: ['./dataset.component.css']
})
export class DatasetComponent implements OnInit {
  formValues: Object
  datasetPerPage = 30
  searching: boolean
  moreAndLessSpecificPlaces: MoreAndLessSpecificPlace[]
  dataset: Dataset[]
  ids: string[]
  totalDataset: number

  constructor(private searchEngineService: SearchEngineService) {
    this.dataset = []
  }

  ngOnInit() {
    SearchEngineService.emitNewQueryResourceLevel.subscribe(res=>{
      this.dataset = []
      this.ids = []
      this.totalDataset = 0
      this.moreAndLessSpecificPlaces = null,
      this.searching = false 
    })
    SearchEngineService.emitQueryDatasetLevel.subscribe(formValues=>{
      SearchEngineService.emitNewQueryDatesetLevel.emit(null)
      this.searching = true;
      this.dataset = []
      this.ids = []
      this.totalDataset = 0
      this.formValues = formValues
      this.moreAndLessSpecificPlaces = null
      this.queryDatasetId(formValues)
    })
  }
  
  queryDatasetId(formValues){
    this.searchEngineService.search(formValues['interval_start'], 
    formValues['interval_end'], 
    formValues['name_place'], 
    formValues['topic'],
    formValues['queryLevel'])
    .then((res: DatasetId[])=>{
      this.ids = res.map((r) => {return r.id})
      this.totalDataset = this.ids.length
      this.getDataset(1)
    }).catch((err:HttpErrorResponse) =>{
      switch(err.status){
        case 300:
          this.moreAndLessSpecificPlaces = err.error
        case 400:
          console.log(err)
      }
    })
  }

  queryDatasetIdWhithGidPlace(formValues, gid_place){
    this.searchEngineService.searchWithPlaceId(formValues['interval_start'], 
    formValues['interval_end'], 
    gid_place, 
    formValues['topic'],
    formValues['queryLevel'])
    .then((res: DatasetId[])=>{
      this.ids = res.map((r) => {return r.id})
      this.totalDataset = this.ids.length
      this.getDataset(1)
    }).catch((err:HttpErrorResponse) =>{
      switch(err.status){
        case 400:
          console.log(err)
      }
    })
  }

  pageChanged(event: PageChangedEvent){
    this.searching = true
    this.getDataset(event.page)
  }

  getDataset(page:number){
    this.searchEngineService.getDataset(
      this.ids.slice((page - 1)*this.datasetPerPage, (this.datasetPerPage*page)))
    .then((res:Dataset[]) => {
      this.dataset = res
    })
    .finally(()=>{
      this.searching = false
    })
  }

  assignPlaceIdAndSearch(gidPlace){
    this.searching = true;
    this.dataset = []
    this.ids = []
    this.queryDatasetIdWhithGidPlace(this.formValues, gidPlace)
  }

  getResources(resources){
    ResourcesModalOptionsService.openResourcesModal.emit(resources)
  }
}
