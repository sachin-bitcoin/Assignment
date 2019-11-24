--DDL
create external table productsale(
userId string,
productid string,
action string)
ROW FORMAT DELIMITED fields terminated by '\054'
STORED AS TEXTFILE
location <location>;

--HIVE HQL
select productid,cnt from (select a.productid,cnt,row_number() over (order by a.cnt desc) as rownum from (select productid,count(1) as cnt from brdmpce_gold.productsale where action="Purchase" group by productid) a)b where rownum<11;