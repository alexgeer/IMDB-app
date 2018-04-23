import csv
import json
from collections import Counter

#This script harvests keywords and their ids from the movies.csv file
#It finds keywords with frequency greater than five and writes them to a file

infile_name = 'credits.csv'
outfile_name = 'directors.csv'
outfile_2_name = 'actors.csv'
#open files
infile = open(infile_name, 'r', encoding="utf8")
outfile = open(outfile_name, 'w',newline='', encoding="utf8")
outfile_2 = open(outfile_2_name, 'w',newline='', encoding="utf8")


#wrap files with readers/writers
cr = csv.reader(infile)
dir_wr = csv.writer(outfile)
act_wr = csv.writer(outfile_2)




for row in cr:
    try:
        movie = row[0]

        try:
            cast = json.loads(row[2])
            crew = json.loads(row[3])
            #find director
            for j in crew:
                if j["job"] == "Director":
                    dir_wr.writerow([movie, j["name"]])
                
            #do actors
            for i in range(5):
                try:
                    actor_name = cast[i]["name"]
                    character = cast[i]["character"]
                    act_wr.writerow([movie,actor_name,character])

                except IndexError:
                    pass
        except json.decoder.JSONDecodeError:
            pass
    except UnicodeWarning as ude:
        print('$$$$$$$$$$$$$$$$PROBLEM$$$$$$$$$$$\n')




infile.close
outfile.close        




