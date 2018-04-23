import csv
import json
from collections import Counter

#This script harvests keywords and their ids from the movies.csv file
#It finds keywords with frequency greater than five and writes them to a file

infile_name = 'movies.csv'
outfile_name = 'movies_main.csv'
outfile_2_name = 'movies_id.csv'
#open files
infile = open(infile_name, 'r', encoding="utf8")
outfile = open(outfile_name, 'w',newline='', encoding="utf8")
outfile_2 = open(outfile_2_name, 'w',newline='', encoding="utf8")


#wrap files with readers/writers
cr = csv.reader(infile)
mov_wr = csv.writer(outfile)
mov_id_wr = csv.writer(outfile_2)




for row in cr:
    try:
        mov_id = row[3]
        mov_title = row[6]
        mov_id_wr.writerow([mov_id,mov_title])

        overview = row[7]
        rel_date = row[11]
        runtime = row[13]
        vote_avg = row[18]
        vote_count = row[19]
        mov_wr.writerow([mov_id, overview, rel_date, runtime, vote_avg, vote_count ])
    
    except UnicodeWarning as ude:
        print('$$$$$$$$$$$$$$$$PROBLEM$$$$$$$$$$$\n')




infile.close
outfile.close        




