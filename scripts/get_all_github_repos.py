import urllib
import markdown
import re
import csv
from bs4 import BeautifulSoup
from pymongo import MongoClient

# url = 'https://raw.githubusercontent.com/avelino/awesome-go/master/README.md'
# url = 'https://raw.githubusercontent.com/alebcay/awesome-shell/master/README.md'

client = MongoClient('localhost', 27017)
curation_collection = client.curation.curation_projects
content_urls = set()
for item in curation_collection.find():
    content_urls.add(item['content_url'])



cnt = 0
all_links = set()
for url in content_urls:
    # url = 'https://raw.githubusercontent.com/markets/awesome-ruby/master/README.md'
    content = urllib.urlopen(url).read().decode('utf-8')
    m = markdown.markdown(content)
    soup = BeautifulSoup(m)


    for line in soup.findAll('a'):
        try:
            href = line['href']
            if re.match("([A-Za-z]{4,5})*:\/\/github\.com\/[A-Za-z0-9\.\-]+\/[A-Za-z0-9\.\-]+\/?", href):
                cnt += 1
                href = href.replace('http:', 'https:')
                all_links.add(href)
                # if url == 'https://raw.githubusercontent.com/krispo/awesome-haskell/master/README.md':
                #     print href
        except:
            pass

    # for line in soup.findAll('li'):
    #     try:
    #         a = line.findAll('a')[0]

    #         href = a['href'].lower()

    #         if re.match("([A-Za-z]{4,5})*:\/\/github\.com\/[A-Za-z0-9\.\-]+\/[A-Za-z0-9\.\-]+\/?", href):
    #             cnt += 1
    #             href = href.replace('http:', 'https:')

    #             if url == 'https://raw.githubusercontent.com/krispo/awesome-haskell/master/README.md':
    #                 print href
                # print href
            # if '//github.com/' in href:
    #            writer.writerow([href.lower()])
                # print href.lower()
        # except:
        #     line.extract()

    print cnt
print len(all_links)

csv_writer = csv.writer(open('all_github_links_indexed_by_curation_projects', 'w'))
for link in all_links:
    csv_writer.writerow([link])
del csv_writer
