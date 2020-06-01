import { TestBed } from '@angular/core/testing';

import { SearchEngineService } from './search-engine.service';

describe('SearchEngineService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SearchEngineService = TestBed.get(SearchEngineService);
    expect(service).toBeTruthy();
  });
});
