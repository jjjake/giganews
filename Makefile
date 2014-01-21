.PHONY: clean

clean: 
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.gz' -exec rm -f {} +
	find . -name '*.csv' -exec rm -f {} +
