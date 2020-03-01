import csv
import json
from collections import Counter

#This script harvests keywords and their ids from the movies.csv file
#It finds keywords with frequency greater than five and writes them to a file

infile_name = 'movies.csv'
outfile_name = 'keywords.csv'

#open files
infile = open(infile_name, 'r', encoding="utf8")
outfile = open(outfile_name, 'w',newline='', encoding="utf8")

#wrap files with readers/writers
cr = csv.reader(infile)
key_wr = csv.writer(outfile)

#store (id,keyword) in
key_tuples = []


for row in cr:
    try:
        #keywords row is a JSON array
        keywordsJSON = row[4]
        try:
            parsed_json = json.loads(keywordsJSON)
            #each elem of array is  {"id" = "id_num" , "name" = "keyword"}
            for j in parsed_json:
                #make a tuple
                key_tuples.append( (j["id"],j["name"]) )

        except json.decoder.JSONDecodeError:
            pass

    except UnicodeWarning as ude:
        print('$$$$$$$$$$$$$$$$PROBLEM$$$$$$$$$$$\n')

#count key tuples
key_count = Counter(key_tuples)
key_list = []

for e in key_count.most_common():
    if key_count[e[0]] >= 5:
        print('MORE THAN 5',  e)
        #write id,keyword, frequency
        id_key = list(e[0])
        freq = e[1]
        key_wr.writerow(list(e[0]) + [freq])
        #add id to list
        key_list.append(id_key[0])


infile.close
outfile.close        
outfile_name = 'movID_key.csv'

infile = open(infile_name, 'r', encoding="utf8")
outfile = open(outfile_name, 'w',newline='', encoding="utf8")

#wrap files with readers/writers
cr = csv.reader(infile)
key_wr = csv.writer(outfile)
for row in cr:
    
    try:
        title_id = row[3]
        keywordsJSON = row[4]
        try:
            parsed_json = json.loads(keywordsJSON)
            for j in parsed_json:
                
                if j["id"] in key_list:
                    key_wr.writerow([title_id,j["id"]])
        except json.decoder.JSONDecodeError:
            pass
    except UnicodeWarning as ude:
        print('$$$$$$$$$$$$$$$$PROBLEM$$$$$$$$$$$\n')

