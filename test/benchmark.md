psql -U postgres -h localhost -p 5432
select count(*) from data;
select max(id) from data;

append init 1jt (inc 1 jt) (avg 1jt/27.8s, rps=35.9k)
run 1
33s
24s
36s
17s
34s

run 2
16s
21s
34s
17s
46s

replace init 6jt (df 1 jt) (avg 1jt/20s, rps=50k)
run 1
31s
12s
truncate=0.02s, insert=13.63s
truncate=0.02s, insert=11.97s
truncate=0.03s, insert=14.33s

run 2
truncate=1.12s, insert=28.24s
truncate=1.02s, insert=10.97s
truncate=0.01s, insert=36.57s
truncate=0.02s, insert=15.97s
truncate=0.01s, insert=29.16s

upsert
1jt df -> 1jt db
156s
247s