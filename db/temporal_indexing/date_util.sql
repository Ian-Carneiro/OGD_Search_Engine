create or replace function distance_between_dates(date, date) returns float as $$
declare 
	interval_start alias for $1;
	interval_end alias for $2;
	last_day_of_month date;
	interval_ interval default make_interval(months => 1, days=> -1);
	first_month float;
	last_month float;
	interval_start_month int default extract(month from interval_start);
    interval_end_month int default extract(month from interval_end);
    interval_start_year int default extract(year from interval_start);
    interval_end_year int default extract(year from interval_end);
    interval_start_day int default extract(day from interval_start);
    interval_end_day int default extract(day from interval_end);
begin
	if interval_end_month = interval_start_month and interval_start_year = interval_end_year then
		last_day_of_month := (date_trunc('MONTH', interval_start) + interval_)::date;
		return (interval_end_day - (interval_start_day-1)) / extract(day from last_day_of_month);
	else
		last_day_of_month := (date_trunc('MONTH', interval_start) + interval_)::date;
        first_month := (extract(day from last_day_of_month) - (interval_start_day - 1)) / 
       													extract(day from last_day_of_month);
        last_day_of_month := (date_trunc('MONTH', interval_end) + interval_)::date;
        last_month := interval_end_day / extract(day from last_day_of_month);
        return first_month + last_month + ((interval_end_month - interval_start_month) - 1) +
                 (12 * (interval_end_year - interval_start_year));
	end if;
end
$$ language plpgsql;


create or replace function get_interval_intersection(date, date, date, date) returns float as $$
declare 
	interval_start_1 alias for $1;
	interval_end_1 alias for $2;
	interval_start_2 alias for $3;
	interval_end_2 alias for $4;
	aux_interval_start date;
	aux_interval_end date;
begin
	if interval_end_1 < interval_start_2 or interval_start_1 > interval_end_2 then 
		return 0.0;
	end if;
	if interval_start_1 <= interval_start_2 then
		aux_interval_start := interval_start_2;
	else
		aux_interval_start := interval_start_1;
	end if;

	if interval_end_1 >= interval_end_2 then
		aux_interval_end := interval_end_2;
	else
		aux_interval_end := interval_end_1;
	end if;
	return distance_between_dates(aux_interval_start, aux_interval_end);
end
$$ language plpgsql;


create or replace function get_interval_difference(date, date, date, date) returns float as $$
declare 
	interval_start_1 alias for $1;
	interval_end_1 alias for $2;
	interval_start_2 alias for $3;
	interval_end_2 alias for $4;
	one_day interval default make_interval(days=> 1);
	months float;
begin
	if interval_start_1 >= interval_start_2 and interval_end_1 <= interval_end_2 then
		return 0;
	elsif interval_end_2 < interval_start_1 or interval_start_2 > interval_end_1 then 
		months := distance_between_dates(interval_start_1, interval_end_1);
	elsif interval_start_1 < interval_start_2 and interval_end_1 > interval_end_2 then 
		months := distance_between_dates(interval_start_1, (interval_start_2 - one_day)::date);
        months := months + distance_between_dates((interval_end_2 + one_day)::date, interval_end_1);
	elsif interval_start_1 >= interval_start_2 then 
		months := distance_between_dates((interval_end_2 + one_day)::date, interval_end_1);
	elsif interval_end_1 <= interval_end_2 then 
		months := distance_between_dates(interval_start_1, (interval_start_2 - one_day)::date);
	end if;
	return months;
end
$$ language plpgsql;


create or replace function get_similarity(date, date, date, date, float, float) returns float as $$
declare 
	interval_start_1 alias for $1;
	interval_end_1 alias for $2;
	interval_start_2 alias for $3;
	interval_end_2 alias for $4;
	alpha alias for $5;
	beta alias for $6;
	intersection_ float;
	difference_ab float;
	difference_ba float;
begin
	intersection_ := get_interval_intersection(interval_start_1, interval_end_1, interval_start_2, interval_end_2);
    if intersection_ = 0 then
        return 0;
    end if;
    difference_ab := get_interval_difference(interval_start_1, interval_end_1, interval_start_2, interval_end_2);
    difference_ba := get_interval_difference(interval_start_2, interval_end_2, interval_start_1, interval_end_1);
    return intersection_/(intersection_ + alpha * difference_ab + beta * difference_ba);
end
$$ language plpgsql;

--select get_similarity(to_date('2019-01-01', 'YYYY-MM-DD'), now()::date, to_date('2019-01-03', 'YYYY-MM-DD'), now()::date, 0.5, 0.5)

