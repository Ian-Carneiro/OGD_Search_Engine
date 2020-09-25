CREATE TABLE public.metadata_dataset (
	id text NULL,
	maintainer text NULL,
	author text NULL,
	"name" text NULL,
	url text NULL,
	notes text NULL,
	metadata_created text NULL,
	tags text NULL,
	metadata_modified text NULL,
	title text NULL,
	num_resources float8 NULL,
	organization_name text NULL,
	organization_id text NULL,
	temporal_indexing bool NULL,
	thematic_indexing bool NULL
);


CREATE TABLE public.metadata_resources (
	id text NULL,
	package_id text NULL,
	url text NULL,
	description text NULL,
	"name" text NULL,
	format text NULL,
	created text NULL,
	last_modified text NULL,
	spatial_indexing bool NULL,
	temporal_indexing bool NULL,
	thematic_indexing bool NULL,
	updated bool NULL,
	excluded bool NULL
);


CREATE INDEX idx_metadata_dataset_id
ON metadata_dataset(id);

CREATE INDEX idx_metadata_resource_id 
ON metadata_resources(id);


create or replace function check_resource_last_modified() returns trigger as $$
# variable_conflict use_column
declare
	lastModified metadata_resources.last_modified%type;
	id metadata_resources.id%type;
begin
	select into lastmodified, id mr.last_modified, mr.id from metadata_resources mr
	where mr.id like new.id;
	if id is null then
		return new;
	else
		update metadata_resources
		set excluded = false
		where id like new.id;
	end if;

	if new.last_modified is null then 
		return null;
	elsif lastmodified is null then
		lastmodified := '';
	end if;
	
	if lastmodified < new.last_modified then
		update metadata_resources
		set url = new.url, 
			description = new.description, 
			"name" = new."name", 
			format = new.format, 
			last_modified = new.last_modified,
			spatial_indexing = false,
			temporal_indexing = false,
			thematic_indexing = false,
			updated = true
		where id like new.id;
		return null;
	end if;

	return null;
end
$$ language plpgsql;


create or replace function check_dataset_last_modified() returns trigger as $$
# variable_conflict use_column
declare
	metadata_modified metadata_dataset.metadata_modified%type;
	id metadata_dataset.id%type;
begin
	select into metadata_modified, id md.metadata_modified, md.id from metadata_dataset md
	where md.id like new.id;
	if id is null then
		return new;
	end if;

	if new.metadata_modified is null then 
		return null;
	elsif metadata_modified is null then
		metadata_modified := '';
	end if;
	
	if metadata_modified < new.metadata_modified then
		update metadata_dataset 
		set id = new.id,
			maintainer = new.maintainer,
			author = new.author,
			"name" = new."name",
			url = new.url,
			notes = new.notes,
			metadata_created = new.metadata_created,
			tags = new.tags,
			metadata_modified = new.metadata_modified,
			title = new.title,
			num_resources = new.num_resources,
			organization_name = new.organization_name,
			organization_id = new.organization_id,
			thematic_indexing = false,
			temporal_indexing = false
		where id like new.id;
		return null;
	end if;

	return null;
end
$$ language plpgsql;


create trigger update_resource before insert on metadata_resources for each row 
execute procedure check_resource_last_modified();


create trigger update_dataset before insert on metadata_dataset for each row 
execute procedure check_dataset_last_modified();

