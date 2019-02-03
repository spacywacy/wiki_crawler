from crawler import Crawler


def main():
	seed_url = 'https://en.wikipedia.org/wiki/Space_exploration'
	
	crawler_BFS = Crawler(seed_url, method='BFS')
	crawler_BFS.BFS()
	print('finished BFS\n')

	crawler_DFS = Crawler(seed_url, method='DFS')
	crawler_DFS.DFS()
	print('finished DFS\n')








if __name__ == '__main__':
	main()



