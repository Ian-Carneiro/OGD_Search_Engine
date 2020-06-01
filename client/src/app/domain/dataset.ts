export class DatasetId{
    constructor(public id:string, public freq: number){}
}

export class Resource{
    constructor(name:string, description:string, url:string, format:string){}
}

export class Dataset{
    constructor(id:string, notes:string, title:string, 
        organization_name:string, maintainer:string, resources: Resource[]){}
}