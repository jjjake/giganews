.PHONY: clean clean-all

clean: 
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.gz' -exec rm -f {} +
	find . -name '*.csv' -exec rm -f {} +

clean-all:
	clean
	find . -name 'giganews_listfile*txt' -exec rm -f {} +

restart: clean
	./get_giganews.py
