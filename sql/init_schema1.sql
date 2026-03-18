create table if not exists aircraft(
	id int primary key, 
	category int not null default 0, -- 0 la unknown, 1 la friendly, 2 la hostile
	last_lat double precision not null,
	last_lng double precision not null,
	last_alt double precision not null,
	last_timestamp bigint not null 
)

create table if not exists history_position(
	id bigserial primary key, 
	aircraft_id int not null, 
	lat double precision not null,
	lng double precision not null,
	alt double precision not null,
	timestamp bigint not null,	
	
	constraint fk_aircraft
	foreign key (aircraft_id)
	references aircraft(id)
	on delete cascade 
)

create index if not exists idx_history_aircraft_time
on history_position (aircraft_id, timestamp desc);
