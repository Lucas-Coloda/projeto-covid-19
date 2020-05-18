drop table external_table;
drop table all_data;
create or replace directory testdir as 'C:\dev\projeto-covid\src\dataResult\';

create table external_table (
FIPS varchar(100),
Admin2 varchar2(100),
Province_State varchar2(100),
Country_Region varchar2(100),
Last_Update varchar2(100),
Lat varchar2(100),
Long_ varchar2(100),
Confirmed varchar2(100),
Deaths varchar2(100),
Recovered varchar2(100),
Active varchar2(100),
Combined_Key varchar2(100))
Organization external
(type oracle_loader
default directory testdir
access parameters (
RECORDS DELIMITED BY '\n'
fields terminated by ';')
location ('all_data.csv'));
--reject limit 1000;

create table all_data (
    FIPS varchar(100),
    Admin2 varchar2(100),
    Province_State varchar2(100),
    Country_Region varchar2(100),
    Last_Update varchar2(100),
    Lat varchar2(100),
    Long_ varchar2(100),
    Confirmed varchar2(100),
    Deaths varchar2(100),
    Recovered varchar2(100),
    Active varchar2(100),
    Combined_Key varchar2(100)
);

insert into all_data(select * from external_table);

-- select * from all_data 
-- select * from all_data where not combined_key like '%undefined%' ;
