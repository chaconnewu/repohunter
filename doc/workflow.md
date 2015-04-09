### Get all indexed GitHub repositories
File: get_all_github_repos.py

### Data importing
File: import.rb

e.g.
```
./2015_import 2015-04-07 2015-04-08
```

It fetches data from githubarchive.org and import to MySQL database.
It only imports projects that are:
1) indexed by curation projects listed in CurationManager.
2) hosted on GitHub.

### Preparing data for all indexed projects in MongoDB
Creating a collection in curation dabase in MongoDB, including necessary data for rendering RepoHunter.

File: aggregating_profile.py
Collection Name: curation_profile

### Preparing data for each curation project



