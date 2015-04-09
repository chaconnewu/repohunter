#!/usr/bin/env ruby
#
# $> ruby load.rb http://data.githubarchive.org/2012-04-01-15.json.gz
#

require 'yajl'
require 'zlib'
require 'mysql'
require 'open-uri'
require 'dbi'
require 'rubygems'
require 'chronic'
require 'yaml'
require 'csv'

# Parse start and end dates.
start_date, end_date = ARGV
start_date = Chronic.parse(start_date)
if end_date.nil?
  end_date = start_date
else
  end_date = Chronic.parse(end_date)
end
usage if start_date.nil?
start_date = start_date.to_date
end_date = end_date.to_date

# read repos
repo_urls = []
CSV.foreach("./all_github_links_indexed_by_curation_projects.csv") do |row|
  repo_urls << row[0]
end

# create the SQLite table schema
@schema = open('./schema.js')
@schema = Yajl::Parser.parse(@schema.read)
@keys = @schema.map {|r| r['name']}

# map GitHub JSON schema to flat CSV space based
# on provided Big Query column schema
def flatmap(h, e, prefix = '')
  e.each do |k,v|
    if k == 'repo'
      k = 'repository'
    end
    if v.is_a?(Hash)
      flatmap(h, v, prefix+k+"_")
    else
      key = prefix+k

      next if !@keys.include? key

      case v
      when TrueClass then h[key] = 1
      when FalseClass then h[key] = 0
      else
        next if v.nil?
        h[key] = v unless v.is_a? Array
      end
    end
  end
  h
end

# Create table schema
create_table = "create table if not exists curation_projects ( \n"
@schema.each do |column|
  create_table += case column['type']
  when 'INTEGER', 'BOOLEAN'
    "#{column['name']} integer, \n"
  when 'STRING', 'RECORD', 'TIMESTAMP'
    "#{column['name']} text, \n"
  end
end

create_table = create_table.chomp(", \n") + ");"

# load the data
db = DBI.connect("DBI:Mysql:gharchive:localhost", "", "")
db.execute(create_table)


# Loop over days.
(start_date..end_date).each do |date|
  # Loop over hours in the day.
  24.times do |hour|
    puts "PROCESSING: #{date.strftime('%Y-%m-%d')} #{hour}h"
    begin
      gz = open("http://data.githubarchive.org/#{date.strftime('%Y-%m-%d')}-#{hour}.json.gz")
    rescue
      next
    end
    js = Zlib::GzipReader.new(gz).read
    Yajl::Parser.parse(js, :check_utf8 => false) do |event|
      row = flatmap({}, event)

      keys, values = row.keys, row.values

      hash = Hash[keys.map.with_index.to_a]
      index = hash['repository_name']
      cur_name = values[index]
      now_name = "https://github.com/#{cur_name}"
      values[index] = now_name


      if repo_urls.include? values[index]
        db.execute "INSERT INTO curation_projects(#{keys.join(',')}) VALUES (#{(['?'] * keys.size).join(',')})", *values
      end

    end
  end
end

puts "import end..."
# Run a query...
#p db.execute("select count(*) from events")
