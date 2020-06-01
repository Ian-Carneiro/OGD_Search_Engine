import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms'
import { HttpClientModule } from '@angular/common/http';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { SearchPageComponent } from './components/search-page/search-page.component';
import { ResourceComponent } from './components/resource/resource.component';
import { DatasetComponent } from './components/dataset/dataset.component';
import { PaginationModule } from 'ngx-bootstrap/pagination';
import { ResourcesModalOptionsComponent } from './components/resources-modal-options/resources-modal-options.component';
import { ModalModule } from 'ngx-bootstrap/modal';


@NgModule({
  declarations: [
    AppComponent,
    SearchPageComponent,
    ResourceComponent,
    DatasetComponent,
    ResourcesModalOptionsComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    ReactiveFormsModule,
    HttpClientModule,
    PaginationModule.forRoot(),
    ModalModule.forRoot(),
  ],
  providers: [ ],
  bootstrap: [AppComponent]
})
export class AppModule { }
