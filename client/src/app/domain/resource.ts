export class ResourceId{
    constructor(public id:string, public freq: number){}
}

export class Resource{
    constructor(url: string, description: string, name: string, organization_name: string, package_id:string){}
}