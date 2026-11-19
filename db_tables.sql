create table medicao_rede (
	id int auto_increment,
    id_farm int,
    latencia_pool int,
    latencia_dns int,
    latencia_gateway int,
    timestamp_medicao datetime,
    constraint pk_id_id_farm primary key (id, id_farm),
    constraint fk_id_farm foreign key (id_farm) references farm(id),
	index idx_timestamp (timestamp_medicao)
) auto_increment = 100;


