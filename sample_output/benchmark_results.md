# Benchmark results

Single process, SQLite target, 1 CPU container. Peak RSS is `ru_maxrss` of the ingest process.

## A. Scaling: memory stays flat as input grows (chunk_size=10000)

| base events | input rows | file MB | wall s | rows/s | peak RSS MB |
|---:|---:|---:|---:|---:|---:|
| 50,000 | 50,977 | 3.9 | 1.847 | 27,599 | 43.1 |
| 200,000 | 203,896 | 15.6 | 7.29 | 27,969 | 44.1 |
| 500,000 | 509,794 | 39.0 | 19.384 | 26,299 | 44.6 |

## B. Optimisation: chunked transactions (30,000 base events)

| chunk_size | wall s | rows/s | peak RSS MB |
|---:|---:|---:|---:|
| 1 | 3.855 | 7,928 | 24.2 |
| 100 | 1.366 | 22,374 | 24.6 |
| 1,000 | 1.111 | 27,510 | 26.1 |
| 10,000 | 1.062 | 28,779 | 43.6 |
