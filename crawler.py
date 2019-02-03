import utils
import os

def crawler_factory(seed_url=None, method='BFS', load_path=None):
	if load_path:
		return utils.pickle_load(load_path)
	else:
		return Crawler(seed_url, method)

class Crawler():

	def __init__(self, seed_url, method='BFS', if_store_doc=True):
		self.seed_url = seed_url
		self.depth = 1
		self.url_count = 0
		self.level_end_str = '__level_ends__' #for BFS only
		self.state_path = 'storage/state.pickle'
		self.max_depth = 6
		self.max_url_count = 1000
		self.crawl_method = method
		self.doc_paths = {
			'BFS':'storage/docs_BFS/',
			'DFS':'storage/docs_DFS/'
		}
		self.if_store_doc = if_store_doc
		self.store_docs_at = self.doc_paths[self.crawl_method]
		self.db_path = 'urls.db'
		self.frontier = []
		self.dfs_tree = {0:[self.seed_url]}
		self.focused = False
		self.conn = None
		self.url_file = None
		self.table_name = 'url_lookup_'
		self.url_prefix = 'https://en.wikipedia.org'
		self.content_class = 'mw-parser-output'
		self.sleep_time = 1
		self.keywords = [
						'Mars',
						'Rover',
						'Orbiter',
						'Pathfinder',
						'Mars Mission',
						'Mars Exploration',
						'Martian',
						'explore',
						'orbit',
						'red planet']

		self.init_storage()
		if self.seed_url:
			self.init_seed()
		else:
			print('no seed url')

	def init_storage(self):
		#change: create the "storage" dir first, then do the rest
		utils.create_doc_dir(self.crawl_method, self.doc_paths) #create file storage dir
		self.url_file = open('storage/urls_{}.txt'.format(self.crawl_method), 'w')

		#create/connect to db file
		os.chdir('storage')
		self.conn = utils.create_conn(self.db_path)
		os.chdir('..')
		utils.create_table(self.conn, self.table_name) #create url table

	def init_seed(self):
		#get frontier
		seed_soup = utils.make_soup(self.seed_url, class_val=self.content_class)
		self.frontier = utils.get_page_urls(seed_soup, url_prefix=self.url_prefix)

		#store seed page
		seed_hash_val = utils.hash_url(self.seed_url)
		seed_doc_path = utils.store_doc(seed_hash_val, seed_soup, self.store_docs_at, if_store=self.if_store_doc)
		utils.store_url(self.conn,
						self.table_name,
						seed_hash_val,
						self.seed_url,
						seed_doc_path,
						url_file=self.url_file)

	def pickle_self(self):
		self.url_file.close()
		self.url_file = None
		self.conn.close()
		utils.pickle_dump(self.state_path, self)

	def BFS(self):
		self.frontier.append(self.level_end_str)

		while True:
			#get current url
			if len(self.frontier) != 0:
				url = self.frontier.pop(0)
			else:
				self.pickle_self()
				break #end if no more url in frontier

			#check to break
			if self.depth > self.max_depth or self.url_count >= self.max_url_count:
				self.pickle_self()
				break #end if reach max

			#check to increment depth
			if url == self.level_end_str:
				self.frontier.append(self.level_end_str)
				self.depth += 1
				continue

			#do crawl
			hash_val = utils.hash_url(url)
			if utils.check_unique(self.conn, self.table_name, hash_val): #query db to check in url is unique

				doc_soup = utils.make_soup(url, class_val=self.content_class)
				utils.delay(self.sleep_time)

				if self.focused:
					if_relevant = utils.check_relevant(doc_soup, self.keywords) #read the document and match key words
				else:
					if_relevant = True

				if if_relevant:
					doc_path = utils.store_doc(hash_val, doc_soup, self.store_docs_at, if_store=self.if_store_doc) #store document content on disk
					utils.store_url(self.conn, self.table_name, hash_val, url, doc_path, url_file=self.url_file) #store url & path to doc content to db
					self.frontier += utils.get_page_urls(doc_soup, url_prefix=self.url_prefix) #append urls in current page to frontier
					self.url_count += 1
					print('url count:', self.url_count)


	def DFS(self):
		self.dfs_tree[1] = self.frontier

		while True:
			#check to break
			if self.url_count >= self.max_url_count or self.depth < 0:
				self.pickle_self()
				break #end if reach max

			#check to go back up a level
			if self.depth > self.max_depth:
				self.depth -= 1

			#get current url
			if len(self.dfs_tree[self.depth]) != 0:
				url = self.dfs_tree[self.depth].pop(0)
			else:
				self.depth -= 1 #if current level level done, go up a level

			#do crawl
			hash_val = utils.hash_url(url)
			if utils.check_unique(self.conn, self.table_name, hash_val): #query db to check in url is unique
				doc_soup = utils.make_soup(url, class_val=self.content_class)
				utils.delay(self.sleep_time)

				if self.focused:
					if_relevant = utils.check_relevant(doc_soup, self.keywords) #read the document and match key words
				else:
					if_relevant = True

				if if_relevant:
					doc_path = utils.store_doc(hash_val, doc_soup, self.store_docs_at, if_store=self.if_store_doc) #store document content on disk
					utils.store_url(self.conn, self.table_name, hash_val, url, doc_path, url_file=self.url_file) #store url & path to doc content to db
					self.depth += 1 #go down a level
					self.dfs_tree[self.depth] = utils.get_page_urls(doc_soup, url_prefix=self.url_prefix) #create url list for lower level
					self.url_count += 1
					print('url count:', self.url_count)

	#implement for fun
	def BFS_recursive(self):
		pass

	#implement for fun
	def DFS_recursive(self):
		pass

	def crawl(self):
		try:
			if self.crawl_method == 'BFS':
				self.BFS()
			elif self.crawl_method == 'DFS':
				self.DFS()
			elif self.crawl_method == 'BFS_r':
				self.BFS_recursive()

			self.url_file.close()

		except Exception as e:
			print(e)
			self.pickle_self()
			self.url_file.close()

		self.conn.close()
			

























if __name__ == '__main__':
	main()










