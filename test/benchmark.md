psql -U postgres -h localhost -p 5432
select count(*) from data;
select max(id) from data;

append init 1jt (inc 1 jt) (avg 1jt/27.8s, rps=35.9k, run1&2)
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

run 3 (with chunking 100_000) (avg 1jt/21.4s, rps=46k, run3)
10s
10s
16s
35s
36s


replace init 6jt (df 1 jt) (avg 1jt/20s, rps=50k, run1&2)
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

run 3 (with chunking 100_000) (avg 1jt/28.8s, rps=34k, run3)
50s
16s
21s
29s
28s

upsert
1jt df -> 1jt db
156s
247s

ijt df -> ijt db (identifier=[id, email])
28s
82s (re run, updates only)