.PHONY: clean-pyc clean

mboxs: giganews_listfile.txt
	python scripts/download_giganews.py

items: offline_servers.txt
	find . -type f -name '*.mbox.gz' -printf "%f\n" | parallel 'scripts/audit_and_upload.py {}'

giganews_listfile.txt:
	python -c "import nntplib; s = nntplib.NNTP('news.giganews.com', readermode=True); s.list(file='$@'); s.quit()"
	shuf --output=$@ $@

offline_servers.txt:
	psql -hdb0 -U archive -c "SELECT node FROM box WHERE duty='OFFLINE' AND node like 'ia%';" | grep ia | sort -u | sed 's/\ //' > $@

itemlist.txt:
	curl 'https://archive.org/metamgr.php?f=exportIDs&w_identifier=usenet*&w_mediatype=data&w_collection=giganews*&w_curatestate=!dark%20OR%20null' > $@

test:
	tests/audit.py $(id)

test-all: itemlist.txt
	parallel 'tests/audit.py {} 1>> "audit-$(BUILD_DATE).txt" 2>> "audit-$(BUILD_DATE)-errors.txt"' < $<

reset-item:
	ia rm -va ${id}
	ia md ${id} -m imagecount:REMOVE_TAG

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

count_articles:
	echo "$$(zcat *csv.gz | wc -l)+$$(cat *csv | wc -l)" | bc

imagecount:
	scripts/add_imagecount_to_item.py ${id}

imagecount-all: itemlist.txt
	parallel --gnu 'scripts/add_imagecount_to_item.py {}' < itemlist.txt 1> imagecount.log 2> imagecount.error &
