.PHONY: clean clean-all restart test test-all

clean: 
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.gz' -exec rm -f {} +
	find . -name '*.csv' -exec rm -f {} +
	find . -not -name "giganews_listfile_`date +%Y%m%d`.txt" -name 'giganews_listfile_*.txt' -exec rm -f {} +
	find . -not -name "giganews_statefile_`date +%Y%m%d`.txt" -name 'giganews_statefile_*.txt' -exec rm -f {} +

clean-all:
	clean
	find . -name 'giganews_listfile_*.txt' -exec rm -f {} +
	find . -name 'giganews_statefile_*.txt' -exec rm -f {} +
	find . -name 'audit-*.txt' -exec rm -f {} +

restart: clean
	./get_giganews.py

test:
	tests/audit.py ${id}

test-all:
	parallel 'tests/audit.py 1>> "audit-`date +%Y%m%d`.txt" 2>> "audit-`date +%Y%m%d`-errors.txt"' < ${itemlist}
