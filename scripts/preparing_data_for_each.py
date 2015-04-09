import urllib
import markdown
import re
import csv
import json
from bs4 import BeautifulSoup
from pymongo import MongoClient, Connection
from collections import defaultdict

url = "./awesome-go.txt"
# content = urllib.urlopen(url).read().decode('utf-8')
client = MongoClient('localhost', 27017)
all_curation_profile = client.curation.curation_profile
repo_dict = defaultdict(dict)
for item in all_curation_profile.find():
    repo_url = item['url']
    repo_dict[repo_url]['yesterday'] = item['yesterday']
    repo_dict[repo_url]['past_week'] = item['past_week']
    repo_dict[repo_url]['past_month'] = item['past_month']


curation_collection = Connection()['curation']['awesomego']


content = open(url, 'r').read().decode('utf-8')
m = markdown.markdown(content)
soup = BeautifulSoup(m)

all_categories = soup.findAll('h2')
cur_category = all_categories[0]
cur_category_name = cur_category.contents[0]
next_category = cur_category.find_next('h2')


document = []
cur_item = {"category": cur_category_name, "children": []}

next_a = cur_category.find_next('a')
cnt = 0
while next_category:
    if next_a != next_category.find_next('a'):
        href = next_a['href']
        if re.match("([A-Za-z]{4,5})*:\/\/github\.com\/[A-Za-z0-9\.\-]+\/[A-Za-z0-9\.\-]+\/?", href):
            # cnt += 1
            # print next_a.contents[0], href, next_a.parent.contents[-1]
            project_item = {"name": next_a.contents[0], "url": href, "description": next_a.parent.contents[-1].encode('UTF-8')}
            if href not in repo_dict:
                project_item['past_month'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}
                project_item['past_week'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}
                project_item['yesterday'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}
            else:
                project_item['past_month'] = repo_dict[href]['past_month']
                project_item['past_week'] = repo_dict[href]['past_week']
                project_item['yesterday'] = repo_dict[href]['yesterday']
                cnt += 1

            cur_item['children'].append(project_item)

        next_a = next_a.find_next('a')

    else:
        if len(cur_item['children']) > 0:
            # document.append(cur_item)
            curation_collection.insert(cur_item)
        cur_category = next_category
        cur_item = {"category": cur_category.contents[0], "children": []}
        next_category = next_category.find_next('h2')
        # print next_category.contents[0]
        # print cnt
        # print '-------------------------------------'
        # cnt = 0

print cnt
# print next_category
# print next_a

# f = open('changed_awesome_go.html', 'w')
# f.write(str(soup))
# f.close()

