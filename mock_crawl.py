from random import randint	

def main():
	web = mock_web()
	#web.random_tree(0)
	web.tree_12()
	web.show_tree()
	db = mock_storage()
	crawler = mock_crawler(db, web)
	#crawler.BFS()
	crawler.DFS()
	crawler.storage.show()
	crawler.storage.show_all()


class mock_web():

	def __init__(self):
		self.max_depth = 3
		self.max_n = 200
		self.node_min = 1
		self.node_max = 5
		self.depth = 1
		self.n = 0
		self.i = 0
		self.url_lookup = {}
		#self.gen_node(0)
		self.show_start = 0

	def gen_node(self, in_url):
		child_size = randint(self.node_min, self.node_max)
		children = [in_url+x+1 for x in range(child_size)]
		node = mock_page(in_url, children)
		self.url_lookup[self.n] = node
		self.n += 1
		return node

	def random_tree(self, url):
		if self.n >= self.max_n:
			return

		child_urls = self.gen_node(url).page_urls
		for child_url in child_urls:
			self.random_tree(self.n)

	def tree_12(self):
		self.url_lookup[1] = mock_page(1, [2,7,8])
		self.url_lookup[2] = mock_page(2, [3,6])
		self.url_lookup[3] = mock_page(3, [4,5])
		self.url_lookup[4] = mock_page(4, [])
		self.url_lookup[5] = mock_page(5, [])
		self.url_lookup[6] = mock_page(6, [])
		self.url_lookup[7] = mock_page(7, [])
		self.url_lookup[8] = mock_page(8, [9,12])
		self.url_lookup[9] = mock_page(9, [10,11])
		self.url_lookup[10] = mock_page(10, [])
		self.url_lookup[11] = mock_page(11, [])
		self.n = 11
		self.show_start = 1

	def show_tree(self):
		print('\nshow tree')
		for i in range(self.show_start, self.n):
			node = self.url_lookup[i]
			print(node.url, node.page_urls)

	def get_page_urls(self, url):
		 node = self.url_lookup.get(url, None)
		 if node:
		 	return node.page_urls
		 else:
		 	return []

class mock_page():

	def __init__(self, url, page_urls):
		self.url = url
		self.page_urls = page_urls

class mock_storage():

	def __init__(self):
		self.crawled_list = []

	def check_unique(self, url):
		return url not in self.crawled_list

	def store_url(self, url):
		self.crawled_list.append(url)

	def show(self):
		print('last url:', self.crawled_list[-1])
		print('max url:', max(self.crawled_list))
		print('crawl size:', len(self.crawled_list))

	def show_all(self):
		for item in self.crawled_list:
			print(item)


class mock_crawler():

	def __init__(self, storage, web):
		self.seed_url = 1
		self.depth = 1
		self.url_count = 0
		self.level_end_str = '__level_ends__' #for BFS only
		self.state_path = 'state.pickle'
		self.max_depth = 12
		self.max_url_count = 100
		self.frontier = []
		self.dfs_tree = {0:[self.seed_url]}
		self.storage = storage
		self.web = web
		self.init_seed()

	def init_seed(self):
		self.frontier = self.web.get_page_urls(self.seed_url)
		self.storage.store_url(self.seed_url)

	def BFS(self):
		self.frontier.append(self.level_end_str)

		while True:
			#get current url
			if len(self.frontier) != 0:
				url = self.frontier.pop(0)
			else:
				print('no more url in frontier')
				break #end if no more url in frontier

			#check to break
			if self.depth > self.max_depth or self.url_count >= self.max_url_count:
				print('reach max')
				print('crawl depth:', self.depth)
				break #end if reach max

			#check to increment depth
			if url == self.level_end_str:
				self.frontier.append(self.level_end_str)
				self.depth += 1
				continue

			#do crawl
			if self.storage.check_unique(url):
				self.storage.store_url(url)
				self.frontier += self.web.get_page_urls(url)
				self.url_count += 1

	def DFS(self):
		self.dfs_tree[1] = self.frontier

		while True:
			print(self.dfs_tree)

			#check to break
			if self.url_count >= self.max_url_count or self.depth < 0:
				print('end depth:', self.depth)
				break #end if reach max

			#check to go back up a level
			if self.depth > self.max_depth:
				self.depth -= 1

			#get current url
			if len(self.dfs_tree[self.depth]) > 0:
				url = self.dfs_tree[self.depth].pop(0)
			else:
				self.depth -= 1 #if current level level done, go up a level

			#do crawl
			if self.storage.check_unique(url):
				self.storage.store_url(url)
				self.depth += 1
				self.dfs_tree[self.depth] = self.web.get_page_urls(url)
				self.url_count += 1











if __name__ == '__main__':
	main()






