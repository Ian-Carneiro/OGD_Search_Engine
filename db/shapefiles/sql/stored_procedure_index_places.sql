--create table resource_place(
--	id_resource text,
--	id_place int,
--	quant int
--);


create or replace function return_type_place(text) returns text as $$
declare 
	str alias for $1;
	c1 scroll cursor for select distinct place.nome, place.tipo from public.place where upper(place.nome) ilike upper(str);
	place1 RECORD;
begin
	open c1;
	fetch next from c1 into place1;
	if place1.nome is null then 
		return null;
	end if;
	fetch absolute 2 from c1 into place1;
	if place1.nome is null then 
		fetch first from c1 into place1;
		return place1.tipo;
		else return 'UNDEFINED';
	end if;
end
$$ language plpgsql;


create or replace function return_if_place(text) returns text as $$
declare 
	str alias for $1;
	c1 scroll cursor for select distinct place.nome, place.tipo from public.place where upper(place.nome) ilike upper(str);
	place1 RECORD;
begin
	open c1;
	fetch next from c1 into place1;
	return place1.nome;
end
$$ language plpgsql;



create or replace function insert_into_resource_place(int, text)
returns void as $$
declare 
	gid alias for $1;
	id_resource_ alias for $2;
    exists_place int;
begin
	select id_place into exists_place from resource_place where id_place = gid and id_resource_ = id_resource;  
	if exists_place is null then
		insert into resource_place(id_resource ,id_place, quant) values(id_resource_, gid, 1);	
		else update resource_place set quant = quant + 1 where id_place = gid and id_resource_ = id_resource;
	end if;
end
$$ language plpgsql;


create or replace function find_places_and_index(text, text, text[], text[])
returns boolean as $$
declare 
	regex alias for $1;
	id_resource alias for $2;
	places_in_order alias for $3;
	types_in_order alias for $4;
	finded_and_indexed boolean := false;
   	contains_place int[] default '{}';
   	are_indexed text[] default '{}';
   	place1 record;
	place2 record;
	c1 scroll cursor for select tab.*, row_number() over () as i from
							(select place.nome, place.tipo, place.gid, place.geom
							 from public.place
							 where upper(place.nome) similar to upper(regex)) tab
						 where array_positions(upper(places_in_order::text)::text[], upper(tab.nome::text)) 
						 			&& array_positions(upper(types_in_order::text)::text[], upper(tab.tipo::text))
					     	   or cardinality(places_in_order) = 0 or cardinality(places_in_order) <> cardinality(types_in_order)
						 order by i asc;
begin
	places_in_order := upper(places_in_order::text)::text[];
	types_in_order := upper(types_in_order::text)::text[];
	open c1;
	fetch next from c1 into place1;
    fetch next from c1 into place2;
	while place1.nome is not null loop
		raise notice '% %', place1.tipo, place1.nome;
	    while place2.nome is not null loop
	    	if st_contains(place2.geom, place1.geom) 
					and place2.gid <> place1.gid then
				if not(contains_place @> array[place2.gid]) then
					contains_place := array_append(contains_place, place2.gid);
				end if;
				if not(are_indexed @> array[place1.nome::text]) then
					are_indexed := array_append(are_indexed, place1.nome::text);
				end if;
				finded_and_indexed := true;
				perform insert_into_resource_place(place1.gid, id_resource);
			end if;
			fetch next from c1 into place2;
		end loop;
		fetch absolute place1.i+1 from c1 into place1;
		fetch first from c1 into place2;
	end loop;
	while place2.nome is not null loop
 		if not(are_indexed @> array[place2.nome::text]) and not(contains_place @> array[place2.gid]) then	
			finded_and_indexed := true;
			perform insert_into_resource_place(place2.gid, id_resource);
		end if;
		fetch next from c1 into place2;
	end loop;
	close c1;
	return finded_and_indexed;
end
$$ language plpgsql;
