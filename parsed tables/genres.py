import csv
import json
from collections import Counter

#This script harvests keywords and their ids from the movies.csv file
#It finds keywords with frequency greater than five and writes them to a file

infile_name = 'movies.csv'
outfile_name = 'genres_id.csv'
#open file
infile = open(infile_name, 'r', encoding="utf8")
outfile = open(outfile_name, 'w',newline='', encoding="utf8")


#wrap files with readers/writers
cr = csv.reader(infile)
wr = csv.writer(outfile)

genre_set = set()



for row in cr:
    try:
        try:
            genres = json.loads(row[1])
            print(genres)
            #find director
            for j in genres:
                g_id = j["id"]
                g_name = j["name"]
                genre_set.add((g_id, g_name))
        except json.decoder.JSONDecodeError:
            pass
    except UnicodeWarning as ude:
        print('$$$$$$$$$$$$$$$$PROBLEM$$$$$$$$$$$\n')

for e in genre_set:
    wr.writerow(e)


infile.close
outfile.close        




