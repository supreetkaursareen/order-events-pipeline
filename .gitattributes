PY ?= python3

.PHONY: setup test sample demo scale bench clean
setup:            ## install test dependency
	$(PY) -m pip install -r requirements.txt

test:             ## run the automated test-suite
	$(PY) -m pytest -v

sample:           ## (re)generate the small, committed starter dataset
	$(PY) -m order_pipeline generate --out-dir sample_data --rows 500 --seed 2 --edge-case-files

demo: clean       ## end-to-end on the starter sample: ingest -> report -> reconcile -> queries
	$(PY) -m order_pipeline ingest --customers sample_data/customers.csv --events sample_data/order_events.csv --db output/warehouse.db
	$(PY) -m order_pipeline report --db output/warehouse.db --out-dir output/reports
	$(PY) -m order_pipeline reconcile --db output/warehouse.db --expected sample_data/expected_answers.json --out-dir output/reports
	$(PY) -m order_pipeline query --db output/warehouse.db --sql-file sql/sample_queries.sql

scale: clean      ## 100k+ record run (ROWS=150000 by default)
	$(PY) -m order_pipeline generate --out-dir output/scale_data --rows $${ROWS:-150000} --seed 7
	$(PY) -m order_pipeline ingest --customers output/scale_data/customers.csv --events output/scale_data/order_events.csv --db output/scale.db
	$(PY) -m order_pipeline report --db output/scale.db --out-dir output/scale_reports
	$(PY) -m order_pipeline reconcile --db output/scale.db --expected output/scale_data/expected_answers.json --out-dir output/scale_reports

bench:            ## throughput / memory / chunk-size measurement
	$(PY) -m order_pipeline benchmark

clean:
	rm -rf output
