import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResourcesModalOptionsComponent } from './resources-modal-options.component';

describe('ResourcesModalOptionsComponent', () => {
  let component: ResourcesModalOptionsComponent;
  let fixture: ComponentFixture<ResourcesModalOptionsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResourcesModalOptionsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourcesModalOptionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
