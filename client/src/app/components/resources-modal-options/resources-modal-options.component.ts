import { Component, OnInit, ViewChild, TemplateRef } from '@angular/core';
import { Resource } from 'src/app/domain/dataset';
import { BsModalService, BsModalRef } from 'ngx-bootstrap/modal';
import { ResourcesModalOptionsService } from 'src/app/services/resources-modal-options.service';

@Component({
  selector: 'resources-modal-options',
  templateUrl: './resources-modal-options.component.html',
  styleUrls: ['./resources-modal-options.component.css']
})
export class ResourcesModalOptionsComponent implements OnInit {

  resources$: Resource[];
  modalRef: BsModalRef;
  @ViewChild('template') template:TemplateRef<any>;
 
  constructor(private modalService: BsModalService) {
  }
  
  ngOnInit() {
    ResourcesModalOptionsService.openResourcesModal.subscribe(resources=>{
      this.resources$ = resources
      this.openModal(this.template)
    })
  }

  openModal(template: TemplateRef<any>) {
    this.modalRef = this.modalService.show(template);
  }
}
