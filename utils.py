from bs4 import BeautifulSoup as bs
import pickle
import requests
import re
import sqlite3
from random import randint
from hashlib import md5
import os
import time

def main():
	#test1()
	test2()

def test1():
	db_ = 'test_.db'
	tname_ = 'test_table'
	conn_ = create_conn(db_)
	url_ = 'https://en.wikipedia.org/wiki/Space_exploration'
	prefix_ = 'https://en.wikipedia.org'
	hash_val = hash_url(url_)
	soup_ = make_soup(url_, class_val='mw-parser-output')
	fpath = 'test_fpath/'

	keywords_ = [
				'Mars',
				'Rover',
				'Orbiter',
				'Pathfinder',
				'Mars Mission',
				'Mars Exploration',
				'Martian',
				'explore',
				'orbit',
				'red planet'
				]
	rele_ = check_relevant(soup_, keywords_)
	print(rele_)
	conn_.close()

def test2():
	doc_paths = {
		'BFS':'storage/docs_BFS/',
		'DFS':'storage/docs_DFS/'
	}
	method = 'BFS'
	create_doc_dir(method, doc_paths)


def request_w_trials(url, log_f=None, timeout_=60, trytimes=5, data_payload=None):
	e_str = ''
	for i in range(trytimes):
		try:
			if data_payload:
				res = requests.post(url, data_payload, timeout=timeout_)
				url_obj = res.content.decode('utf-8')
			else:
				user_agent = \
				'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
				headers = {'User-Agent': user_agent}
				res = requests.get(url, headers=headers, timeout=timeout_)
				url_obj = res.content.decode('utf-8')
			return url_obj
		except Exception as e:
			e_str = str(e)
			print('request error, trying again...')

	error_message = 'error occured when opening: ' + url + '\n'
	error_message += 'error: ' + e_str + '\n'
	print(error_message)
	if log_f:
		log_f.write(error_message)


def make_soup(url, log_f=None, data_payload=None, class_val=None):
	url_obj = request_w_trials(url, log_f, data_payload=data_payload)
	if url_obj:
		soup = bs(url_obj, 'lxml')
		if class_val:
			soup = soup.find_all(class_ = re.compile(class_val))[0]

		return soup


def pickle_dump(fname, obj_):
	with open(fname, 'wb') as f:
		pickle.dump(obj_, f)

def pickle_load(fname):
	with open(fname, 'rb') as f:
		obj = pickle.load(f)
		
	return obj

def create_doc_dir(crawl_method, path_lookup):
	file_path = path_lookup[crawl_method]
	if not os.path.isdir(os.path.abspath(file_path)):

		dirs = file_path.split('/')
		dirs = [x for x in dirs if len(x)>0]
		n = 0
		for dir_ in dirs:
			if not os.path.isdir(os.path.abspath(dir_)):
				os.mkdir(dir_)
			os.chdir(dir_)
			n+=1
		for i in range(n):
			os.chdir('..')
		print('created storage path')

	else:
		print('path exists')

def store_doc(hash_val, soup, file_path, if_store):
	if if_store:

		if not os.path.isdir(os.path.abspath(file_path)):
			os.mkdir(file_path)
			print('created path: {}'.format(file_path))

		html_content = soup.text
		fname = '{}{}.txt'.format(file_path, hash_val)

		with open(fname, 'w') as f:
			f.write(html_content)

		return fname

	else:
		return ''

def get_page_urls(soup, url_prefix=None):
	#hard coded rules, to improve
	urls = soup.find_all(href=re.compile('^/wiki'))
	urls = [x['href'] for x in urls]
	urls = [x for x in urls if ':' not in x]

	if url_prefix:
		urls = [url_prefix+x for x in urls]

	return urls

def check_table(conn, table_name):
	cursor = conn.cursor()
	sql_ = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name = ?;'
	cursor.execute(sql_, [table_name])
	if_exist = len(list(cursor)) > 0 
	cursor.close()
	return if_exist

def create_table(conn, table_name):
	cursor_create = conn.cursor()
	if not check_table(conn, table_name):
		sql_create = '''
				CREATE TABLE {} (
					id integer primary key autoincrement,
					hash_val varchar(64),
					url varchar(512),
					doc_path varchar(256)
				);
			   '''.format(table_name, table_name, table_name, table_name, table_name)
		sql_index_id = 'CREATE INDEX {}_id ON {}(id);'.format(table_name, table_name)
		sql_index_hash = 'CREATE INDEX {}_hash ON {}(hash_val);'.format(table_name, table_name)

		cursor_create.execute(sql_create)
		cursor_create.execute(sql_index_id)
		cursor_create.execute(sql_index_hash)
		conn.commit()
		print('url table created')
	else:
		print('url table already exists')
	cursor_create.close()

def store_url(conn, table_name, hash_val, url, doc_path, url_file=None):
	print(url)
	cursor = conn.cursor()
	row = [hash_val, url, doc_path]
	row_size = len(row)
	cols = ['hash_val', 'url', 'doc_path']
	col_str = ','.join(cols)
	place_holders = ','.join(['?' for x in range(row_size)])
	sql_str = 'INSERT INTO {}({}) VALUES ({});'.format(table_name, col_str, place_holders)
	cursor.execute(sql_str, row)
	conn.commit()
	cursor.close()

	if url_file:
		url_file.write(url+'\n')

def check_unique(conn, table_name, hash_val):
	cursor = conn.cursor()
	sql_ = 'SELECT 1 FROM {} WHERE hash_val=? LIMIT 1;'.format(table_name)
	cursor.execute(sql_, [hash_val])
	result = cursor.fetchall()
	cursor.close()

	return len(result)==0

def get_anchor_text(soup):
	#doing the same search as get_page_url(), to improve
	urls = soup.find_all(href=re.compile('^/wiki'))
	urls = [x for x in urls if ':' not in x['href']]
	anchor_text = set([x.text.lower() for x in urls])
	return anchor_text

#an naive way to check relevancy
def check_relevant(soup, keywords):
	anchor_text = get_anchor_text(soup)
	keys_lower = list(set([x.lower() for x in keywords]))

	for word in keys_lower:
		if word in anchor_text:
			return True

	return False

def create_conn(db_path):
	#cannott create db file 2 levels down the dir
	return sqlite3.connect(db_path)

def hash_url(url):
	url_bytes = bytes(url, 'utf-8')
	return md5(url_bytes).hexdigest()

def delay(length):
	time.sleep(length)




























if __name__ == '__main__':
	main()









