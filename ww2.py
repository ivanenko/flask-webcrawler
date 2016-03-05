# -*- coding: utf-8 -*-
'''
Module documentation
You can use script with command line option: python ww2.py URL_TO_PARSE
In this case you will get output in stdout, so you can redirect it to file: 
>> python ww2.py URL_TO_PARSE > output.cvs

If you not specify URL parameter, flask web server will run on 5000 port
You can see web form to enter URL on you browser:
http://127.0.0.1:5000
You can download output file after parser finish
'''
import Queue
from threading import Thread
import requests, re, sys, getopt
from flask import Flask, request, make_response
import json, io
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Queue as ProcessQueue
import StringIO

NUMBER_PROCESSES = 8


def get_metro_stations(text):
    ''' get metro station urls from page '''
    result = []
    pattern = re.compile('metroStations=(.*)"')
    match = pattern.search(text)
    if match:
        w = re.sub("&quot;", '"', match.group(1))
        w = re.sub('\\\\/', '/', w)
        json_metro_list = json.loads(w)
        result = [url['url'] for url in json_metro_list]
    else:
        print 'error getting metro list'

    return result


def parse_company_worker(param):
    ''' parse single company page and put result info into Queue '''

    def get_text(pattern, text, result, field_name, default=None):
        match = pattern.search(text)
        if match:
            result[field_name] = match.group(1).strip()
        else:
            if default:
                result[field_name] = default

    def parse_request(url, output):
        print url
        result = {}
        r = requests.get(url)
        text = re.sub("\n|\r", '', r.text)
        get_text(pt_name, text, result, 'name')
        get_text(pt_raiting, text, result, 'rating', '0.0')
        get_text(pt_phone, text, result, 'phone')
        more_phones = re.findall(pt_morephones, text)
        if len(more_phones) > 0:
            if 'phone' not in result:
                result['phone'] = more_phones.pop()
            result['more_phones'] = ', '.join(more_phones)
        get_text(pt_addr, text, result, 'addr')
        get_text(pt_descr, text, result, 'descr')
        get_text(pt_raion, text, result, 'raion')

        output.put(result, block=False)

    pt_name = re.compile('<h1 itemprop="name">(.*?)</h1>', re.DOTALL)
    pt_raiting = re.compile('<span class="rating__value"\s+itemprop="ratingValue">(.*?)</span>')
    pt_phone = re.compile('<span itemprop="telephone">(.*?)</span>')
    pt_morephones = re.compile('<div\s+class="company__contacts-popup-item"\s+itemprop="telephone">(.*?)</div>')
    pt_addr = re.compile('<span\s+class="company__contacts-item-text"\s+itemprop="address">(.*?)</span>')
    pt_descr = re.compile('<div class="company__description">(.*?)</div>')
    pt_raion = re.compile(
        u'<div class="company__contacts-item is-white-space-nowrap"><span class="company__contacts-item-label">Район: </span><span class="company__contacts-item-text">(.*?)</span></div>')

    parse_request(param[0], param[1])


def collect_company_urls(param):
    ''' Parse all pages for one metro and get companies urls '''
    r = requests.get(param[0])
    pt = re.compile(u'Найдено (.*) компаний')
    match = pt.search(r.text)
    if match:
        pages = int(match.group(1)) / 20
        pt_companies = re.compile('<a class="companies__item-title-text" href="(.*)">')
        urls = []
        urls = re.findall(pt_companies, r.text)
        for i in range(pages):
            r = requests.get(param[0] + '?page=%s' % (i + 2))
            urls_page = re.findall(pt_companies, r.text)
            urls.extend(urls_page)
        print match.group(1), len(urls), param[0]
        for u in urls:
            param[1].put_nowait(u)
    else:
        param[1].put_nowait('error ---------------------')


def process_parsing(url, output_stream):
    # url='http://www.yell.ru/spb/top/restorany/'
    url_prefix = 'http://www.yell.ru'
    r = requests.get(url)
    metro_urls = get_metro_stations(r.text)
    print "metroes: ", len(metro_urls)

    # collect company urls for parsing
    pool = ThreadPool(NUMBER_PROCESSES)
    res_queue = ProcessQueue()
    results = pool.map(collect_company_urls, [(url_prefix + u, res_queue) for u in metro_urls])
    pool.close()
    pool.join()
    pool.terminate()

    # reduce urls
    count = 0
    reduced_url_set = set()
    while not res_queue.empty():
        count = count + 1
        url = res_queue.get_nowait()
        # FOR TEST ONLY - REMOVE THIS
        #if count < 10:
        reduced_url_set.add(url)

    print count
    print len(reduced_url_set)

    # start company parsing pool ----
    print 'start!!!!!!'
    pool = ThreadPool(NUMBER_PROCESSES)
    output_queue = ProcessQueue()
    results = pool.map(parse_company_worker, [(url_prefix + u, output_queue) for u in reduced_url_set])
    pool.close()
    pool.join()
    pool.terminate()
    print 'done!!!!!'

    # --- write results in CSV file

    #fields = ['name', 'addr', 'rating', 'phone', 'more_phones', 'descr', 'raion']
    fields = ['name', 'addr', 'rating', 'phone', 'more_phones']
    for field in fields:
        output_stream.write(field + ';')
    output_stream.write('\n')
    while not output_queue.empty():
        rec = output_queue.get_nowait()
        for field in fields:
            value = rec.get(field, '')
            output_stream.write(value.encode('cp1251') + ';')
        output_stream.write('\n')

    print 'finished'


app = Flask(__name__)

@app.route('/')
def index_page():
    html = '''
        <html><head><title>Flask web-crawler</title></head>
        <body>
            <div style="width:50%;margin: 0 auto;margin-top:100px;"><form method="POST" action="/download/">
                Enter URL here: <input type="text" name="url" /><input type="submit" value="Submit" />
            </form></div>
        </body>
        </html>
        '''
    return html


@app.route('/download/', methods=['GET', 'POST'])
def perform_parsing():
    if request.method == 'POST':
        output_stream = StringIO.StringIO()
        process_parsing(request.form['url'], output_stream)
        response = make_response(output_stream.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=companies.csv"
        return response

    return 'use POST method'


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help'])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)

    # process options
    for o, a in opts:
        if o in ('-h', '--help'):
            print __doc__
            sys.exit(0)

    if len(args) > 0:
        process_parsing(args[0], sys.stdout)
    else:
        app.run()
