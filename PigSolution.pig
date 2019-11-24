A = load 'productsale' using org.apache.hive.hcatalog.pig.HCatLoader();
B = filter A by action == 'Purchase';
C = GROUP B by productid;
D = FOREACH C GENERATE group,COUNT(B);
E = ORDER D BY $1 desc;
F = LIMIT E 10;