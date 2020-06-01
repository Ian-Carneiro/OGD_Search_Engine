import { Injectable, EventEmitter } from '@angular/core';
import { Resource } from '../domain/resource';

@Injectable({
  providedIn: 'root'
})
export class ResourcesModalOptionsService {
  static openResourcesModal = new EventEmitter<Resource[]>()
  constructor() { }
}
