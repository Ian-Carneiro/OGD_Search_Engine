import { environment } from './../../environments/environment';
import { Injectable, EventEmitter } from '@angular/core';
import { HttpClient } from "@angular/common/http"
import { Resource } from '../domain/resource';

@Injectable({
  providedIn: 'root'
})
export class SearchEngineService {
  private readonly API = environment.api
  static emitQueryDatasetLevel = new EventEmitter<object>();
  static emitQueryResourceLevel = new EventEmitter<object>();
  static emitNewQueryResourceLevel = new EventEmitter<null>();
  static emitNewQueryDatesetLevel = new EventEmitter<null>();

  constructor(private http: HttpClient) { }

  search(intervalStart: string, intervalEnd: string, place: string, topic: string, queryLevel: string){
    return this.http.get(`${this.API}${queryLevel}?name_place=${place}&interval_start=${intervalStart}&interval_end=${intervalEnd}&topic=${topic}`).toPromise()
  }

  searchWithPlaceId(intervalStart: string, intervalEnd: string, placeId: string, topic: string, queryLevel: string){
    return this.http.get(`${this.API}${queryLevel}?gid_place=${placeId}&interval_start=${intervalStart}&interval_end=${intervalEnd}&topic=${topic}`).toPromise()
  }

  getResources(ids: string[]){
    return this.http.post(`${this.API}resource`, {'ids': ids}).toPromise()
  }

  getDataset(ids: string[]){
    return this.http.post(`${this.API}dataset`, {'ids': ids}).toPromise()
  }
}
