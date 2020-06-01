import { SearchEngineService } from './../../services/search-engine.service';
import { Component, OnInit } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';

@Component({
  selector: 'search-page',
  templateUrl: './search-page.component.html',
  styleUrls: ['./search-page.component.css']
})
export class SearchPageComponent implements OnInit {
  formGroup: FormGroup
  queryLevel: string
  constructor(private formBuilder: FormBuilder) { }

  ngOnInit() {
    this.formGroup = this.formBuilder.group({
      name_place: ['', [Validators.minLength(2)]],
      topic: ['', [Validators.minLength(2)]],
      interval_start: [''],
      interval_end: [''],
      queryLevel: ['resource']
    })
  }

  search(){
    let formValues = this.formGroup.value
    this.queryLevel = formValues['queryLevel']
    if (this.queryLevel == 'resource'){
      SearchEngineService.emitQueryResourceLevel.emit(formValues)
    }else if (this.queryLevel == 'dataset'){
      SearchEngineService.emitQueryDatasetLevel.emit(formValues)
    }else{

    }
    // this.formGroup.reset()
  }
}
