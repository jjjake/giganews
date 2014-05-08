.PHONY: clean-pyc clean test

clean: 
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.gz' -exec rm -f {} +
	find . -name '*.csv' -exec rm -f {} +
	find . -not -name "giganews_listfile_`date +%Y%m%d`.txt" -name 'giganews_listfile_*.txt' -exec rm -f {} +
	find . -not -name "giganews_statefile_`date +%Y%m%d`.txt" -name 'giganews_statefile_*.txt' -exec rm -f {} +

test:
	tests/audit.py ${id}

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
