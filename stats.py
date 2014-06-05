#!/usr/bin/python
import re
import subprocess

from internetarchive.session import ArchiveSession
import requests
import matplotlib.pyplot as plt
from jinja2 import Template


TOTAL_ARTICLES = 1379103334 


def get_img_count():
    s = ArchiveSession()
    u = 'https://archive.org/metamgr.php'
    p = dict(
        w_collection='giganews*',
        mode='more',
    )
    r = requests.get(u, params=p, cookies=s.cookies)
    matches = re.findall(r'(?<=<b>imagecount:</b>\ ).*', r.content)[0].split('|')[0]
    if not matches:
        return
    img_count = int(matches.replace(',', ''))
    local_img_count = get_local_img_count()
    return img_count + local_img_count

def get_local_img_count():
    gz = subprocess.Popen('zcat /3/data/giganews/*csv.gz | wc -l', shell=True, stdout=subprocess.PIPE)
    csv = subprocess.Popen('cat /3/data/giganews/*csv | wc -l', shell=True, stdout=subprocess.PIPE)
    return int(gz.communicate()[0].strip()) + int(csv.communicate()[0].strip())

def get_download_rates(log_file='/3/data/giganews/giganews.log'):
    rates = []
    for x in open(log_file):
        if 'article download rate is' in x:
            rates.append(float(x.split()[-1].split('/')[0]))
    return rates

def render_html(rates):
    avg_rate = reduce(lambda x, y: x + y, rates[-100:]) / len(rates[-100:])
    seconds_left = TOTAL_ARTICLES/avg_rate
    days_left = seconds_left/86400
    stats = dict(
        current_rate=rates[-1],
        avg_rate=avg_rate,
        days_left=days_left,
        df=df(),
        open_cons=get_open_connections(),
    )
    template = Template(open('/3/data/giganews/giganews/stats.html').read())
    html = template.render(stats=stats)
    with open('/home/jake/public_html/analytics/giganews.html', 'wb') as fp:
        fp.write(html)

def df():
    _df = subprocess.Popen('df -h /3', shell=True, stdout=subprocess.PIPE)
    return _df.communicate()[0]
     
def graph_rate(rates):
    fig = plt.figure()
    plt.plot(rates)
    fig.savefig('/home/jake/public_html/analytics/stats.png', dpi=fig.dpi)

def get_open_connections():
    cons = subprocess.Popen('netstat  | grep giganews | grep ESTABLISHED | wc -l', shell=True, stdout=subprocess.PIPE)
    return int(cons.communicate()[0])

if __name__ == '__main__':
    rates = get_download_rates()
    graph_rate(rates)
    render_html(rates)
