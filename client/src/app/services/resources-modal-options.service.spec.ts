import { TestBed } from '@angular/core/testing';

import { ResourcesModalOptionsService } from './resources-modal-options.service';

describe('ResourcesModalOptionsService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ResourcesModalOptionsService = TestBed.get(ResourcesModalOptionsService);
    expect(service).toBeTruthy();
  });
});
