import json
import csv
from bs4 import BeautifulSoup, Comment
import re

FILEPATH = "D:\\PIXNET\\Raw data\\5_nomove.jl"
TEST_DATA_PATH = "C:\\Users\\Dexter\\OneDrive - 國立成功大學\\AI Competition\\Final Project\\samples.jl"
FILE_FULL_DIR = "D:\\PIXNET\\Full Data\\"
FILE_PART_DIR = "D:\\PIXNET\\Partitioned Data\\"
FILE_DB_DIR = "D:\\PIXNET\\DB-like Data\\"
FILE_A = "data"
FILE_B = "data_partition"
FILE_C = "blogs_partition"
FILE_D = "comments_partition"
FILE_E = "tags_partition"
FILE_F = "keywords_partition"
PARTITION_SIZE = 5000

data_columns = ["blog_id", "title", "body", "comments", "tags", "keywords"]
blog_columns = ["blog_id", "title", "body"]
comment_columns = ["blog_id", "comment_id", "comments", "replys"]
tag_columns = ["blog_id", "tag_id", "tags"]
keyword_columns = ["blog_id", "keywords_id", "keywords"]


with open(FILE_FULL_DIR + FILE_A + ".csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f);
    writer.writerow(data_columns)

def createPartitionFiles(id):
    with open(FILE_PART_DIR + FILE_B + "_" + str(id) + ".csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(data_columns)

    with open(FILE_DB_DIR + FILE_C + "_" + str(id) + ".csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(blog_columns)

    with open(FILE_DB_DIR + FILE_D + "_" + str(id) + ".csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(comment_columns)

    with open(FILE_DB_DIR + FILE_E + "_" + str(id) + ".csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(tag_columns)

    with open(FILE_DB_DIR + FILE_F + "_" + str(id) + ".csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(keyword_columns)

def writeBigCSV(ary):
    with open(FILE_FULL_DIR + FILE_A + ".csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(ary)

def writePartCSV(partition_count, partCSV, bodyCSV, commentCSV, tagCSV, keywordCSV):
    with open(FILE_PART_DIR + FILE_B + "_" + str(partition_count) + ".csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(partCSV)

    with open(FILE_DB_DIR + FILE_C + "_" + str(partition_count) + ".csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(bodyCSV)

    with open(FILE_DB_DIR + FILE_D + "_" + str(partition_count) + ".csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(commentCSV)
    
    with open(FILE_DB_DIR + FILE_E + "_" + str(partition_count) + ".csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(tagCSV)

    with open(FILE_DB_DIR + FILE_F + "_" + str(partition_count) + ".csv", "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(keywordCSV)

INLINE_ELES = ["a", "abbr", "acronym", "b", "bdo", "big", "button", "cite", "code",
                "dfn", "em", "i", "label", "kbd", "map", "object", "q", "samp",
                "small", "span", "strong", "sub", "sup", "time", "tt", "var"]

with open(FILEPATH, newline='', encoding='utf-8') as readFile:
    data = readFile.readline()
    line_id = 1
    partition_id = 1
    partition_count = 0
    bigCSV = [];
    partCSV = [];
    bodyCSV = [];
    commentCSV = [];
    tagCSV = [];
    keywordCSV = [];

    while (data):
        # Load the object using json
        data = json.loads(data)

        # Parse HTML
        body = re.sub("\n+", "\n", "".join([s for s in BeautifulSoup(data["body"], "html.parser").find_all(string=True) if (s.parent.name not in ["script", "style", "select", "option"] and not isinstance(s, Comment))]))
        

        # Write the data into the big csv
        bigCSV.append([line_id, data["title"], body, json.dumps(data["comments"]), json.dumps(data["tags"]), json.dumps(data["keywords"])]);

        # Write into file for each 1000 rows
        if (line_id%500 == 0):
            writeBigCSV(bigCSV)
            bigCSV = []
        
        # For partitioned data, create a new file if needed
        if (line_id % PARTITION_SIZE == 1):
            partition_count += 1
            createPartitionFiles(partition_count)
        
        # Write the data into partitioned csv
        partCSV.append([line_id, data["title"], body, json.dumps(data["comments"]), json.dumps(data["tags"]), json.dumps(data["keywords"])])

        # Write the data into partitioned body csv
        bodyCSV.append([line_id, data["title"], body])
        
        # Write the data into partitioned comments csv
        for cid,c in enumerate(data["comments"]):
            commentCSV.append([line_id, cid, c["body"], c["reply"]])
                
        # Write the data into partitioned tags csv
        for tid,tag in enumerate(data["tags"]):
            tagCSV.append([line_id, tid, tag])
       
        # Write the data into partitioned keywords csv
        for kid,keyword in enumerate(data["keywords"]):
            keywordCSV.append([line_id, kid, keyword])
        
        if (line_id % 1000 == 0):
            writePartCSV(partition_count, partCSV, bodyCSV, commentCSV, tagCSV, keywordCSV)
            partCSV = []
            bodyCSV = []
            commentCSV = []
            tagCSV = []
            keywordCSV = []

        data = readFile.readline()
        line_id += 1
        if (line_id % 50 == 0):
            print(line_id / 208743 * 100, " %")

    writeBigCSV(bigCSV)
    writePartCSV(partition_count, partCSV, bodyCSV, commentCSV, tagCSV, keywordCSV)

print(line_id)
