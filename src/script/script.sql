/*

drop sequence seq_ds_mes;
drop sequence seq_ds_semana;
drop sequence seq_ds_dia;
drop sequence seq_ds_pais_regiao;
drop sequence seq_ds_estado_provincia;
drop sequence seq_ds_cidade;
drop sequence seq_ds_situacao_covid;

drop table ds_situacao_covid;
drop table ds_dia;
drop table ds_semana;
drop table ds_mes;
drop table ds_cidade;
drop table ds_estado_provincia;
drop table ds_pais_regiao;

*/
-- Necessario substituir o 'C:\dev\' para o seu path do projeto java
create or replace directory dir as 'C:\dev\projeto-covid\src\dataResult\';
-- Corrige formato de data para o padrao especificado
ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY';

create sequence seq_ds_mes;
create sequence seq_ds_semana;
create sequence seq_ds_dia;
create sequence seq_ds_pais_regiao;
create sequence seq_ds_estado_provincia;
create sequence seq_ds_cidade;
create sequence seq_ds_situacao_covid;

create table ds_mes (
    id_mes number(10),
    inicio_mes date,
    fim_mes date,
    constraint pk_ds_mes primary key (id_mes)
);

create table ds_semana (
    id_semana number(10),
    -- semana pode estar em até 2 meses
    id_mes number(10),
    id_mes_2 number(10),
    inicio_semana date,
    fim_semana date,
    constraint pk_ds_semana primary key (id_semana),
    constraint fk_ds_mes foreign key (id_mes) references ds_mes(id_mes),
    constraint fk_ds_mes_2 foreign key (id_mes_2) references ds_mes(id_mes)
);

create table ds_dia (
    id_dia number(10),
    id_semana number(10),
    dia date,
    constraint pk_ds_dia primary key (id_dia),
    constraint fk_ds_semana foreign key (id_semana) references ds_semana(id_semana)
);

create table ds_pais_regiao (
    id_pais_regiao number(10),
    nome varchar(100),
    constraint pk_ds_pais_regiao primary key (id_pais_regiao)
);

create table ds_estado_provincia (
    id_estado_provincia number(10),
    id_pais_regiao number(10),
    nome varchar(100),
    constraint pk_ds_estado_provincia primary key (id_estado_provincia),
    constraint fk_ds_pais_regiao foreign key (id_pais_regiao) references ds_pais_regiao(id_pais_regiao)
);

create table ds_cidade (
    id_cidade number(10),
    id_estado_provincia number(10),
    nome varchar2(100),
    chave varchar2(100),
    fips number(10),
    lat varchar2(100),
    long_ varchar2(100),
    constraint pk_ds_cidade primary key (id_cidade),
    constraint fk_ds_estado_provincia foreign key (id_estado_provincia) references ds_estado_provincia(id_estado_provincia)
);

create table ds_situacao_covid (
    id_situacao_covid number,
    confirmados number,
    mortos number,
    recuperados number,
    ativos number,
    id_dia number,
    id_cidade number,
    constraint pk_ds_situacao_covid primary key (id_situacao_covid),
    constraint fk_ds_dia foreign key (id_dia) references ds_dia(id_dia),
    constraint fk_ds_cidade foreign key (id_cidade) references ds_cidade(id_cidade)
);

drop table external_table;

create table external_table (
    FIPS varchar(100),
    Admin2 varchar2(100),
    Province_State varchar2(100),
    Country_Region varchar2(100),
    Day varchar2(100),
    Start_Week varchar2(100),
    And_Week varchar2(100),
    Start_Month varchar2(100),
    And_Month varchar2(100),
    Lat varchar2(100),
    Long_ varchar2(100),
    Confirmed varchar2(100),
    Deaths varchar2(100),
    Recovered varchar2(100),
    Active varchar2(100),
    Combined_Key varchar2(100)
) Organization external(
    type oracle_loader
    default directory dir
    access parameters (
        RECORDS DELIMITED BY '\n'
        fields terminated by ';'
    )
    location ('all_data.csv')
) REJECT LIMIT UNLIMITED;

-- tabela clone do conteudo csv, serve apenas para melhorar a performance nas buscas
drop table all_data;
create table all_data (
    FIPS varchar(100),
    Admin2 varchar2(100),
    Province_State varchar2(100),
    Country_Region varchar2(100),
    Day varchar2(100),
    Start_Week varchar2(100),
    And_Week varchar2(100),
    Start_Month varchar2(100),
    And_Month varchar2(100),
    Lat varchar2(100),
    Long_ varchar2(100),
    Confirmed varchar2(100),
    Deaths varchar2(100),
    Recovered varchar2(100),
    Active varchar2(100),
    Combined_Key varchar2(100)
);
insert into all_data select * from external_table;

-- Insere todos os paises
declare seq_id number;
BEGIN
    FOR val IN (SELECT DISTINCT country_region FROM all_data order by country_region)
    LOOP
        select seq_ds_pais_regiao.nextval into seq_id from dual;
        INSERT INTO ds_pais_regiao(id_pais_regiao, nome) VALUES (seq_id, val.country_region);
    END LOOP;
END;
/

-- insere todas as provincias/regioes
declare 
    seq_id number;
    id_pais_regiao number;
BEGIN
    FOR val IN (
        select DISTINCT country_region, province_state 
            FROM all_data ad 
        order by country_region, ad.province_state
    )LOOP
        select seq_ds_estado_provincia.nextval into seq_id from dual;
        select id_pais_regiao into id_pais_regiao from ds_pais_regiao where nome = val.country_region;
        INSERT INTO ds_estado_provincia(id_estado_provincia, nome, id_pais_regiao) VALUES (seq_id, val.province_state, id_pais_regiao);
    END LOOP;
END;
/

-- Insere todas as cidades
declare 
    seq_id number;
    estado_provincia_id number;
BEGIN
    FOR val IN (
        select DISTINCT country_region, province_state, admin2
            FROM all_data
        order by country_region, province_state, admin2
    )LOOP
        select seq_ds_cidade.nextval into seq_id from dual;
        select id_estado_provincia into estado_provincia_id from ds_estado_provincia ep
            inner join ds_pais_regiao pr on pr.id_pais_regiao = ep.id_pais_regiao
            where ep.nome = val.province_state and pr.nome = val.country_region;
        INSERT INTO ds_cidade(id_cidade, nome, id_estado_provincia) VALUES (seq_id, val.admin2, estado_provincia_id);
    END LOOP;
END;
/

-- Insere meses
declare 
    seq_id number;
BEGIN
    FOR val IN (
        select DISTINCT start_month, and_month
            FROM all_data
        order by start_month, and_month
    )LOOP
        select seq_ds_mes.nextval into seq_id from dual;
        INSERT INTO ds_mes(id_mes, inicio_mes, fim_mes) VALUES (seq_id, val.start_month, val.and_month);
    END LOOP;
END;
/

-- Insere semanas
declare 
    seq_id number;
    mes_id number;
BEGIN
    FOR val IN (
        select DISTINCT start_week, and_week
            FROM all_data
        order by to_date(start_week), to_date(and_week)
    )LOOP
        select seq_ds_semana.nextval into seq_id from dual;
        select m.id_mes into mes_id from ds_mes m 
            where to_date(val.start_week )
                BETWEEN to_date(m.inicio_mes) AND to_date(m.fim_mes);
        INSERT INTO ds_semana(id_semana, id_mes, id_mes_2, inicio_semana, fim_semana)
            VALUES (seq_id, mes_id, null, val.start_week, val.and_week);
    END LOOP;
END;
/

-- Insere dias
declare 
    seq_id number;
    semana_id number;
BEGIN
    FOR val 
    IN (
        select DISTINCT day, start_week, and_week
            FROM all_data 
            order by to_date(day),to_date(start_week), to_date(and_week)
    )
    LOOP
        select seq_ds_dia.nextval into seq_id from dual;
        
        select id_semana into semana_id 
            from ds_semana s
            where to_date(val.day) 
                BETWEEN s.inicio_semana 
                    AND s.fim_semana;
        INSERT INTO ds_dia(id_dia, id_semana, dia) VALUES (seq_id, semana_id, val.day);
    END LOOP;
END;
/

-- Insere casos
declare 
    seq_id number;
    dia_id number;
BEGIN
    FOR val 
    IN (
        select c.id_cidade, ad.day, ad.confirmed, ad.active, ad.recovered, ad.deaths
            from ds_pais_regiao pr 
            join ds_estado_provincia ep
                on pr.id_pais_regiao = ep.id_pais_regiao
            join ds_cidade c
                on ep.id_estado_provincia = c.id_estado_provincia 
            join all_data ad
                on ad.country_region = pr.nome 
                    and ad.province_state = ep.nome
                    and ad.admin2 = c.nome
            order by to_date(day)
    )
    LOOP
        if (val.confirmed > val.deaths and val.confirmed > val.recovered and val.confirmed > val.active)
        then 
            select seq_ds_situacao_covid.nextval into seq_id from dual;
            
            select id_dia into dia_id from ds_dia where dia = to_date(val.day);
            INSERT INTO ds_situacao_covid(id_situacao_covid, id_dia, id_cidade, confirmados, ativos, recuperados, mortos)
                VALUES (seq_id, dia_id, val.id_cidade, val.confirmed, val.active, val.recovered, val.deaths);
        end if;
    END LOOP;
END;
/


------------------ Questões:

-- 1) Qual a taxa de mortalidade em todo o periodo?
select 
    sum(sc.confirmados) as casos_confirmados,
    (
        case (sum(sc.ativos) + sum(sc.recuperados))
        when 0 then 0 
        else (sum(sc.mortos) / (sum(sc.ativos) + sum(sc.recuperados)) * 100)
        end
    ) as taxa_mortalidade
from ds_pais_regiao pr 
join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
join ds_dia d  on sc.id_dia = d.id_dia
join ds_semana se on se.id_semana = d.id_semana
join ds_mes me on me.id_mes = se.id_mes
where d.dia = (select max(d2.dia) from ds_dia d2);

-- 2) Qual a cidade tem a maior taxa de mortalidade e com numero de casos acima de 1000 em todo o periodo?
select 
    (c.nome || ', ' || ep.nome || ', ' || pr.nome) as cidade,
    sc.confirmados as casos_confirmados,
    (
        case (sc.ativos + sc.recuperados)
        when 0 then 0 
        else ((sc.mortos / (sc.ativos + sc.recuperados)) * 100)
        end
    ) as taxa_mortalidade
from ds_pais_regiao pr 
join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
join ds_dia d  on sc.id_dia = d.id_dia
join ds_semana se on se.id_semana = d.id_semana
join ds_mes me on me.id_mes = se.id_mes
where d.dia = (select max(d2.dia) from ds_dia d2)
and sc.confirmados > 1000
and ep.nome != 'UNDEFINED'
order by taxa_mortalidade desc, casos_confirmados desc;


-- 3) Qual o pais com mais infectados?
select pais, confirmados as infectados  from (
    select distinct nome as pais, id_pais_regiao, (
        select sum(sc.confirmados) as confirmados
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
    ) as confirmados from ds_pais_regiao pr1
    order by confirmados desc
) where confirmados is not null;

-- 4) Qual pais teve mais recuperados?
select pais, recuperados  from (
    select distinct nome as pais, id_pais_regiao, (
        select sum(sc.recuperados) as recuperados
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
    ) as recuperados from ds_pais_regiao pr1
    order by recuperados desc
) where recuperados is not null;

-- 5) Qual cidade obteve mais mortes por infectados?
select cidade, infectados, mortes from (
    select distinct (c1.nome || ', ' || ep1.nome || ', '|| pr1.nome) as cidade, c1.id_cidade, (
        select sum(sc.confirmados) as infectados
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
        and ep.id_estado_provincia = ep1.id_estado_provincia
        and ep.nome != 'UNDEFINED'
        and c.id_cidade = c1.id_cidade
    ) as infectados,
    (
        select sum(sc.mortos) as mortes
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
        and ep.id_estado_provincia = ep1.id_estado_provincia
        and ep.nome != 'UNDEFINED'
        and c.id_cidade = c1.id_cidade
    ) as mortes
    from ds_pais_regiao pr1
    inner join ds_estado_provincia ep1 on pr1.id_pais_regiao = ep1.id_pais_regiao
    inner join ds_cidade c1 on c1.id_estado_provincia = ep1.id_estado_provincia
    order by infectados desc
) where infectados is not null;

-- 6) Qual provincia tem o maior numero de ativos atualmente?
select (estado || ', ' || pais) as estado_provincia, ativos  from (
    select distinct ep1.nome as estado, pr1.nome as pais, pr1.id_pais_regiao, (
        select sum(sc.ativos) as ativos
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
        and ep.id_estado_provincia = ep1.id_estado_provincia
    ) as ativos 
    from ds_pais_regiao pr1
    inner join ds_estado_provincia ep1 on pr1.id_pais_regiao = ep1.id_pais_regiao
    order by ativos desc
) where 
    ativos is not null 
  and 
    rownum = 1;

-- 7) Qual região foi mais atingida até fevereiro?
select (estado || ', ' || pais) as estado_provincia, confirmados, recuperados, mortos from (
    select distinct ep1.nome as estado, pr1.nome as pais, pr1.id_pais_regiao, (
        select sum(sc.confirmados) as confirmados
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2 where d2.id_semana < 6
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
        and ep.id_estado_provincia = ep1.id_estado_provincia
    ) as confirmados,
    (
        select sum(sc.mortos) as mortos
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2 where d2.id_semana < 6
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
        and ep.id_estado_provincia = ep1.id_estado_provincia
    ) as mortos,
    (
        select sum(sc.recuperados) as recuperados
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d  on sc.id_dia = d.id_dia
        join ds_semana se on se.id_semana = d.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (
            select max(d2.dia) from ds_dia d2 where d2.id_semana < 6
        )
        and pr.id_pais_regiao = pr1.id_pais_regiao
        and ep.id_estado_provincia = ep1.id_estado_provincia
    ) as recuperados
    from ds_pais_regiao pr1
    inner join ds_estado_provincia ep1 on pr1.id_pais_regiao = ep1.id_pais_regiao
    order by confirmados desc
) where 
    confirmados is not null
  and 
    rownum = 1;

-- 8) Qual foi a semana mais letal?
select 
    (inicio_semana || ' - ' || fim_semana) as semana, 
    mortos as quantidade_de_vitimas 
from (
    select distinct inicio_semana, fim_semana, (
        select sum(sc.mortos)
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d on sc.id_dia = d.id_dia
        where d.dia = se1.fim_semana
    ) as mortos from ds_semana se1
    order by mortos desc
) where 
    mortos is not null
  and 
    rownum = 1;

-- 9) Em qual dia houve mais recuperados?
select dia, recuperados as quantidade_de_recuperados
from (
    select distinct d1.dia, (
        select sum(sc.recuperados)
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d on sc.id_dia = d.id_dia
        where d.dia = d1.dia
    ) as recuperados
    from ds_dia d1
    order by recuperados desc
) where 
    recuperados is not null
  and 
    rownum = 1;

-- 10) Como foi o crescimento da doença durante os meses?
select 
    (inicio_mes || ' - ' || fim_mes) as mes, 
    confirmados,
    mortos as quantidade_de_vitimas,
    recuperados,
    ativos
from (
    select distinct id_mes, inicio_mes, fim_mes, (
        select sum(sc.confirmados)
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d on sc.id_dia = d.id_dia
        join ds_semana se on d.id_semana = se.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where me.id_mes = m1.id_mes
    ) as confirmados, (
        select sum(sc.mortos)
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d on sc.id_dia = d.id_dia
        join ds_semana se on d.id_semana = se.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where me.id_mes = m1.id_mes
    ) as mortos,(
        select sum(sc.recuperados)
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d on sc.id_dia = d.id_dia
        join ds_semana se on d.id_semana = se.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where me.id_mes = m1.id_mes
    ) as recuperados,(
        select sum(sc.ativos)
            from ds_pais_regiao pr 
        join ds_estado_provincia ep on pr.id_pais_regiao = ep.id_pais_regiao
        join ds_cidade c on ep.id_estado_provincia = c.id_estado_provincia 
        join ds_situacao_covid sc on sc.id_cidade = c.id_cidade
        join ds_dia d on sc.id_dia = d.id_dia
        join ds_semana se on d.id_semana = se.id_semana
        join ds_mes me on me.id_mes = se.id_mes
        where d.dia = (select max(d2.dia) from ds_dia d2 where d2.dia <= m1.fim_mes)
    ) as ativos
    from ds_mes m1
    order by to_date(inicio_mes) asc
);