from crawler import Crawler
from utils import overlap


def main():
	seed_url = 'https://en.wikipedia.org/wiki/Space_exploration'
	
	crawler_BFS = Crawler(seed_url, method='BFS')
	crawler_BFS.crawl()

	'''
	crawler_DFS = Crawler(seed_url, method='DFS', if_store_doc=False)
	crawler_DFS.crawl()

	crawler_bfs_focused = Crawler(seed_url, method='BFS_focused', if_store_doc=False, if_focused=True)
	crawler_bfs_focused.crawl()

	t1 = 'url_lookup_BFS'
	t2 = 'url_lookup_DFS'
	db_path = 'storage/urls.db'
	overlaps = overlap(db_path, t1, t2)
	print('Overlaps between the results of BFS & DFS:')
	for item in overlaps:
		print(item[0])

	print('overlap counts:', len(overlaps))
	'''








if __name__ == '__main__':
	main()



