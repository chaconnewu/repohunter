import MySQLdb
from collections import defaultdict
from datetime import date, timedelta
from pymongo import MongoClient, Connection

con = MySQLdb.connect('localhost', '', '', '')
cur = con.cursor()

yesterday = date.today() - timedelta(1)

repo_dict = defaultdict(dict)

# # past month
past_month = date.today() - timedelta(30)
cur.execute("select actor_login, repository_name, type from curation_projects where left(created_at, 10) >= %s and left(created_at, 10) <= %s", (past_month, yesterday))

for line in cur.fetchall():
    actor, repo_name, event_type = line
    owner = repo_name.rsplit('/', 1)
    if not repo_dict[repo_name].get("past_month"):
        repo_dict[repo_name]['past_month'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}
        repo_dict[repo_name]['past_week'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}
        repo_dict[repo_name]['yesterday'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}

    key_prefix = "Owner_" if owner == actor else "Other_"
    cur_item = repo_dict[repo_name]['past_month']
    if event_type == 'PushEvent':
        cur_item["Owner_push"] += 1

    if event_type in ['IssuesEvent', 'IssueCommentEvent']:
        cur_item[key_prefix+'issue'] += 1

    if event_type in ['PullRequestEvent', 'PullRequestReviewCommentEvent']:
        cur_item[key_prefix+'pullrequest'] += 1

    if event_type == 'WatchEvent':
        cur_item["Other_star"] += 1

# pastweek
past_week = date.today() - timedelta(7)
cur.execute("select actor_login, repository_name, type from curation_projects where left(created_at, 10) >= %s and left(created_at, 10) <= %s", (past_week, yesterday))


for line in cur.fetchall():

    actor, repo_name, event_type = line
    owner = repo_name.rsplit('/', 1)
    # if not repo_dict[repo_name].get("past_week"):
    #     repo_dict[repo_name]['past_week'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}

    key_prefix = "Owner_" if owner == actor else "Other_"
    cur_item = repo_dict[repo_name]['past_week']
    if event_type == 'PushEvent':
        cur_item["Owner_push"] += 1

    if event_type in ['IssuesEvent', 'IssueCommentEvent']:
        cur_item[key_prefix+'issue'] += 1

    if event_type in ['PullRequestEvent', 'PullRequestReviewCommentEvent']:
        cur_item[key_prefix+'pullrequest'] += 1

    if event_type == 'WatchEvent':
        cur_item["Other_star"] += 1

# yesterday
cur.execute("select actor_login, repository_name, type from curation_projects where left(created_at, 10)=%s", (str(yesterday)))

for line in cur.fetchall():
    actor, repo_name, event_type = line
    owner = repo_name.rsplit('/', 1)
    # if not repo_dict[repo_name].get("yesterday"):
    #     repo_dict[repo_name]['yesterday'] = {"Owner_push": 0, "Owner_issue": 0, "Owner_pullrequest": 0, "Other_star": 0, "Other_pullrequest": 0, "Other_issue": 0}

    key_prefix = "Owner_" if owner == actor else "Other_"
    cur_item = repo_dict[repo_name]['yesterday']
    if event_type == 'PushEvent':
        cur_item["Owner_push"] += 1

    if event_type in ['IssuesEvent', 'IssueCommentEvent']:
        cur_item[key_prefix+'issue'] += 1

    if event_type in ['PullRequestEvent', 'PullRequestReviewCommentEvent']:
        cur_item[key_prefix+'pullrequest'] += 1

    if event_type == 'WatchEvent':
        cur_item["Other_star"] += 1


# Save to mongodb
client = MongoClient('localhost', 27017)
curation_collection = Connection()['curation']['curation_profile']

for k, v in repo_dict.iteritems():
    v['url'] = k
    curation_collection.insert(v)

del curation_collection





