import sqlite3
import os
import csv
import random
import jieba
import math
import re
from bs4 import BeautifulSoup, Comment
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import collections
csv.field_size_limit(100000000)

# Data file processing
DATA_FOLDER = "C:\\Users\\Dexter\\OneDrive - 國立成功大學\\AI Competition\\Final Project\\DB-like Data"
DB_FILE = "pixnet.db";
DB_FILE2 = "wordDict.db";

# Initialize a database
def initDB(conn = sqlite3.connect(DB_FILE), closeConnection = False):
    # Setup cursor
    print("DB Initiation - Setup")
    c = conn.cursor()

    # Drop tables
    print("DB Initiation - Drop previous tables")
    c.execute('''DROP TABLE IF EXISTS blogs''')
    c.execute('''DROP TABLE IF EXISTS comments''')
    c.execute('''DROP TABLE IF EXISTS tags''')
    c.execute('''DROP TABLE IF EXISTS keywords''')
    conn.commit()

    # Create tables
    print("DB Initiation - Creating tables")
    c.execute('''CREATE TABLE blogs
                (blog_id    INTEGER, 
                title       TEXT, 
                body        TEXT,
                PRIMARY KEY(blog_id))''')
    c.execute('''CREATE TABLE comments
                (blog_id    INTEGER, 
                comment_id  INTEGER, 
                comments    TEXT, 
                replys      TEXT,
                PRIMARY KEY(blog_id, comment_id),
                FOREIGN KEY(blog_id) REFERENCES blogs(blog_id))''')
    c.execute('''CREATE TABLE tags
                (blog_id    INTEGER, 
                tag_id      INTEGER, 
                tags        TEXT,
                PRIMARY KEY(blog_id, tag_id),
                FOREIGN KEY(blog_id) REFERENCES blogs(blog_id))''')
    c.execute('''CREATE TABLE keywords
                (blog_id    INTEGER, 
                keywords_id INTEGER, 
                keywords    TEXT,
                PRIMARY KEY(blog_id, keywords_id),
                FOREIGN KEY(blog_id) REFERENCES blogs(blog_id))''')
    conn.commit()

    # Close connection
    if closeConnection:
        print("DB Initiation - Closing connections")
        conn.close()

    print("DB Initiation - Finished")
    return conn


# Turn a CSV row into SQL value
def csvToSQLValue(table, row):
    def quoteStr(cell):
        return "\"" + cell.replace("\"","\"\"") + "\""

    # Depend on the data structure, and quote string values if needed
    if table == "blogs":
        return ", ".join([row[0], *[quoteStr(c) for c in row[1:]]])
    elif table == "comments":
        return ", ".join([*row[:2], *[quoteStr(c) for c in row[2:]]])
    else:
        return ", ".join([*row[:2], quoteStr(row[2])])

# Initialize the data in the database
def initData(conn = sqlite3.connect(DB_FILE), dataFolder = DATA_FOLDER, closeConnection = False):
    # Setup cursor
    print("Data Initiation - Setup")
    c = conn.cursor()
    
    # Clear tables
    print("Data Initiation - Clearing tables")
    c.execute('''DELETE FROM blogs''')
    c.execute('''DELETE FROM comments''')
    c.execute('''DELETE FROM tags''')
    c.execute('''DELETE FROM keywords''')
    conn.commit()

    # Loop all the files in the DB-like Data folder
    for root, dirs, filenames in os.walk(dataFolder):
        for filename in filenames:
            print("Data Initiation - Data Reading - " + filename)
            fullpath = os.path.join(dataFolder, filename)
            filenameInfo = filename.split(".")[0].split("_")
            table = filenameInfo[0]
            with open(fullpath, 'r', encoding="utf-8", newline="") as file:
                # NOTED there is a NULL byte that can affect CSV reading
                csvReader = csv.reader(line.replace('\0','') for line in file)
                for i,row in enumerate(csvReader):
                    if i>0:
                        line = "INSERT INTO " + table + " VALUES(" + csvToSQLValue(table,row) + ")"
                        try:
                            c.execute(line)
                        except sqlite3.OperationalError:
                            print(line)
                            raise sqlite3.OperationalError
    conn.commit()

    # Close connection
    if closeConnection:
        conn.close()

    print("Data Initiation - Finished")
    return conn

def getDataTrial(conn = sqlite3.connect(DB_FILE), closeConnection = False):
    '''
        Try to get a sample data and print a record if it's fine, typically for the use of a database checking.

        Parameters
        ====================================

        conn    `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.
        closeConnection     `bool`  - Whether to close connection at the end of calling this function.

        Returns
        ====================================

        `sqlite3.Connection`  - Return the connection if the connection is not closed at the end.
    '''
    cur = conn.cursor()
    cur.execute("SELECT title FROM blogs")
    print(cur.fetchone())

    if (closeConnection):
        conn.close()
    else:
        return conn

def getData(conn = sqlite3.connect(DB_FILE), table = "", id1 = None, id2 = None):
    '''
        Get a specific row of data through specifying the id of an object.

        Parameters
        ====================================

        conn    `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.
        table   `str` - The table name to retrieve.
        id1     `int` - The blog_id of the object.
        id2     `int` - The secondary primary id of the object, eg. comment_id if a Comment is requested.

        Returns
        ====================================
        
        `tuple(*)`  - A tuple row of the table
    '''
    if (table in ["blogs", "comments", "tags", "keywords"]):
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + table + " WHERE blog_id = " + str(id1) + ("" if table == "blogs" else (("comment_id" if table == "comments" else "tag_id" if table == "tags" else "keywords_id" if table == "keywords" else "") + " = " + id2)))
        return cur.fetchone()


class Blog:
    '''
        Class representing a blog item.
    '''
    def __init__(self, blog_id, title, body, conn = sqlite3.connect(DB_FILE)):
        '''
            Create a Blog object.

            Parameters
            ====================================
            blog_id     `int`   - The blog_id
            title       `str`   - The title
            body        `str`   - The textual content of the blog
            conn        `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.
        '''
        self.blog_id = blog_id
        self.title = title
        self.body = body
        self.conn = conn

    def getThisKeywords(self):
        '''
            Get a list of keywords under this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Keyword)`  - a list of Keyword objects.
        '''
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM keywords WHERE blog_id = " + str(self.blog_id))
        keywords = cur.fetchall()
        return [Keyword(*c) for c in keywords]

    def getThisComments(self, retreiveCount = None):
        '''
            Get a list of comments under this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.
        '''
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM comments WHERE blog_id = " + str(self.blog_id) + ((" ORDER BY RANDOM() LIMIT " + str(retreiveCount)) if retreiveCount is not None else ""))
        comments = cur.fetchall()
        return [Comment(*c) for c in comments]

    def getOtherComments(self, retreiveCount = 5, blog_ids = None):
        '''
            Get a list of comments not in this blog.

            Parameters
            ====================================

            retreiveCount   `int` - The topmost k comments, recommend a small integer smaller than 30
            blog_ids  `list[int]` - A pre-fetched blog_id list

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.
        '''
        cur = self.conn.cursor()
        if blog_ids is None:
            blog_ids = Blog.getIDs(self.conn)
        
        blog_ids = random.sample(blog_ids, retreiveCount+1)

        if (self.blog_id in blog_ids):
            del blog_ids[blog_ids.index(self.blog_id)]

        cur.execute("SELECT * FROM comments WHERE blog_id IN (" + ",".join([str(bi) for bi in blog_ids]) + ") ORDER BY RANDOM() LIMIT " + str(retreiveCount))
        comments = cur.fetchall()
        return [Comment(*c) for c in comments]
    
    def getSimilarBlogsByKeywords(self, retreiveCount = 5, logKeywords = False):
        '''
            Get a list of blogs with the same keywords as this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.
            logKeywords     `bool`      - Whether to log the keywords found in this blog.

            Returns
            ====================================

            `list(Blog)`  - a list of Blog objects.

        '''
        cur = self.conn.cursor()
        
        # Log TFIDF Keywords if needed.
        if (logKeywords):
            cur.execute("SELECT keywords FROM keywords WHERE blog_id = " + str(self.blog_id))
            print("Keywords:\n", [r[0] for r in cur.fetchall()])

        # Select a list of blogs.
        cur.execute("SELECT * FROM blogs "+
                    "WHERE blog_id IN "+
                        "(SELECT blog_id "+
                            # From a shuffled keywords table.
                            "FROM (SELECT * FROM keywords ORDER BY RANDOM()) " +
                            # The keywords should be in those keywords of this blog
                            "WHERE blog_id != " + str(self.blog_id) +" AND keywords IN (SELECT keywords FROM keywords WHERE blog_id = " + str(self.blog_id) + ") "+
                            "GROUP BY blog_id "+
                            "ORDER BY COUNT(keywords_id) DESC" + 
                            ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))) + 
                        ") ")
        return [Blog(*b, self.conn) for b in cur.fetchall()]
    
    def getSimilarBlogsByTags(self, retreiveCount = 5, logKeywords = False):
        '''
            Get a list of blogs with the same tags as this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.
            logKeywords     `bool`      - Whether to log the keywords found in this blog.

            Returns
            ====================================

            `list(Blog)`  - a list of Blog objects.

        '''
        cur = self.conn.cursor()
        
        # Log TFIDF Keywords if needed.
        if (logKeywords):
            cur.execute("SELECT tags FROM tags WHERE blog_id = " + str(self.blog_id))
            print("Tags:\n", [r[0] for r in cur.fetchall()])

        # Select a list of blogs.
        cur.execute("SELECT * FROM blogs "+
                    "WHERE blog_id IN "+
                        "(SELECT blog_id "+
                            # From a shuffled tags table.
                            "FROM (SELECT * FROM tags ORDER BY RANDOM()) " +
                            # The tags should be in those tags of this blog
                            "WHERE blog_id != " + str(self.blog_id) +" AND tags IN (SELECT tags FROM tags WHERE blog_id = " + str(self.blog_id) + ") "+
                            "GROUP BY blog_id "+
                            "ORDER BY COUNT(tag_id) DESC" + 
                            ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))) + 
                        ") ")
        return [Blog(*b, self.conn) for b in cur.fetchall()]
    
    def getCommentsFromSimilarKeywords(self, retreiveCount = 5):
        '''
            Get a list of comments with the same keywords as this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.

        '''
        cur = self.conn.cursor()

        # Select a list of comments.
        cur.execute("SELECT * FROM comments WHERE blog_id IN (" + ",".join([str(b.blog_id) for b in self.getSimilarBlogsByKeywords(retreiveCount)]) + ") ORDER BY RANDOM()" + ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))))
        return [Comment(*c) for c in cur.fetchall()]
    
    def getCommentsFromSimilarTags(self, retreiveCount = 5):
        '''
            Get a list of comments with the same tags as this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.

        '''
        cur = self.conn.cursor()

        # Select a list of comments.
        cur.execute("SELECT * FROM comments WHERE blog_id IN (" + ",".join([str(b.blog_id) for b in self.getSimilarBlogsByTags(retreiveCount)]) + ") ORDER BY RANDOM()" + ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))))
        return [Comment(*c) for c in cur.fetchall()]
    
    def getSimilarBlogsByTFIDF(self, conn2 = sqlite3.connect(DB_FILE2), topK = 10, retreiveCount = 5, logKeywords = False, orderedBy = "count"):
        '''
            Get a list of blogs with the similar TFIDF as this blog.

            Parameters
            ====================================

            conn2  `sqlite3.Connection` - A SQLite connection object for the word dictionary. Default as the a new connection to the global DB_FILE2 databse file.
            topK            `int`       - The top-K tf-idf words to be selected for comparisons.
            retreiveCount   `None|int`  - The topmost k blogs, all if None is given.
            logKeywords     `bool`      - Whether to log the keywords found in this blog.
            orderedBy       `str`       - "count": blogs sorted by tf-idf matched vocab count; "tfidf": blogs sorted by tf-idf similarity

            Returns
            ====================================

            `list(Blog)`  - a list of Blog objects.

        '''
        cur = self.conn.cursor()
        cur2 = conn2.cursor()

        
        cur2.execute("SELECT word FROM word_dict WHERE id IN (SELECT word_id "+
                        "FROM (SELECT word_id,tf_idf "+
                            "FROM blogs_tf_idf "+
                            "WHERE blog_id = " + str(self.blog_id) + " " +
                            "ORDER BY RANDOM()) "+
                        "ORDER BY tf_idf DESC "+
                        "LIMIT " + str(topK) + ")")
        tfidfKeywords = [r[0] for r in cur2.fetchall()]

        if (logKeywords):
            print("tf-idf Keywords:\n", tfidfKeywords)

        # Select a list of blogs_id.
        cur.execute("SELECT * FROM blogs "+
                    "WHERE blog_id IN "+
                        "(SELECT blog_id "+
                            # From a shuffled keywords table.
                            "FROM (SELECT * FROM keywords ORDER BY RANDOM()) " +
                            # The keywords should be in those keywords of this blog
                            "WHERE blog_id != " + str(self.blog_id) +" AND keywords IN ('" + "','".join(tfidfKeywords) + "') "+
                            "GROUP BY blog_id "+
                            "ORDER BY COUNT(keywords_id) DESC" + 
                            ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))) + 
                        ") ")
        blogList = [Blog(*b, self.conn) for b in cur.fetchall()]

        # Sort by tf-idf similarity
        if (orderedBy == "tfidf"):
            cur2.execute("SELECT word_id FROM blogs_title_tf_idf GROUP BY word_id")
            word_id_list = [r[0] for r in cur2.fetchall()]

            cur2.execute("SELECT blog_id, word_id, tf_idf FROM blogs_title_tf_idf WHERE blog_id IN (" + ",".join([*[str(b.blog_id) for b in blogList],str(self.blog_id)]) + ")")
            tf_idf_dict = {}
            for r in cur2.fetchall():
                if (r[0] not in tf_idf_dict):
                    tf_idf_dict[r[0]] = {word_id: 0 for word_id in word_id_list}
                tf_idf_dict[r[0]][r[1]] = r[2]
            blogList = sorted(blogList, key=lambda blog: cosine_similarity([tf_idf_dict[self.blog_id]], tf_idf_dict[int(blog.blog_id)])[0][0])[:retreiveCount]
        
        return blogList

    def getCommentsFromSimilarTFIDF(self, conn2 = sqlite3.connect(DB_FILE2), topK = 10, retreiveCount = 5):
        '''
            Get a list of comments with the same tfidf as this blog.

            Parameters
            ====================================

            conn2  `sqlite3.Connection` - A SQLite connection object for the word dictionary. Default as the a new connection to the global DB_FILE2 databse file.
            topK            `int`       - The top-K tf-idf words to be selected for comparisons.
            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.

        '''
        cur = self.conn.cursor()

        # Select a list of comments.
        cur.execute("SELECT * FROM comments WHERE blog_id IN (" + ",".join([str(b.blog_id) for b in self.getSimilarBlogsByTFIDF(conn2, topK, retreiveCount)]) + ") ORDER BY RANDOM()" + ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))))
        return [Comment(*c) for c in cur.fetchall()]
    
    def getSimilarBlogs(self, conn2 = sqlite3.connect(DB_FILE2), topK = 10, retreiveCount = 5, finalRetreiveCount = 1, logKeywords = False, cachedWordList = None, orderedBy = "random"):
        '''
            Get a list of blogs with the similar TFIDF as this blog.

            Parameters
            ====================================

            conn2  `sqlite3.Connection` - A SQLite connection object for the word dictionary. Default as the a new connection to the global DB_FILE2 databse file.
            topK            `int`       - The top-K tf-idf words to be selected for comparisons.
            retreiveCount   `None|int`  - The topmost k blogs in each algorithm, all if None is given.
            finalRetreiveCount  `int`   - The topmost k blogs to return.
            logKeywords     `bool`      - Whether to log the keywords found in this blog.
            orderedBy       `str`       - "random": blogs sorted by random order; "tfidf": blogs sorted by tf-idf similarity

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.

        '''
        blogsFromKeywords = self.getSimilarBlogsByKeywords(retreiveCount = retreiveCount , logKeywords=logKeywords)
        blogsFromTags = self.getSimilarBlogsByTags(retreiveCount=retreiveCount , logKeywords=logKeywords)
        blogsFromTIFID = self.getSimilarBlogsByTFIDF(retreiveCount=retreiveCount , topK=topK, logKeywords=logKeywords)

        blogList = [*blogsFromKeywords, *blogsFromTags, *blogsFromTIFID]

        cur2 = conn2.cursor()
        # Sort by tf-idf similarity
        if (orderedBy == "tfidf"):
            if (cachedWordList is not None):
                word_id_list = cachedWordList
            else:
                cur2.execute("SELECT word_id FROM blogs_tf_idf GROUP BY word_id")
                word_id_list = [r[0] for r in cur2.fetchall()]

            cur2.execute("SELECT blog_id, word_id, tf_idf FROM blogs_tf_idf WHERE blog_id IN (" + ",".join([*[str(b.blog_id) for b in blogList], str(self.blog_id)]) +  ")")
            tf_idf_dict = {}
            for r in cur2.fetchall():
                if (r[0] not in tf_idf_dict):
                    tf_idf_dict[r[0]] = [0 for word_id in word_id_list]
                tf_idf_dict[r[0]][word_id_list.index(r[1])] = r[2]
            
            if self.blog_id in tf_idf_dict:
                blogList = sorted(blogList, key=lambda blog: cosine_similarity([tf_idf_dict[self.blog_id]], [tf_idf_dict[blog.blog_id]])[0][0] if blog.blog_id in tf_idf_dict else 0)
        
        return blogList[:finalRetreiveCount]

    def getCommentsFromSimilarBlogs(self, conn2 = sqlite3.connect(DB_FILE2), topK = 10, retreiveCount = 5, orderedBy = "random", cachedWordList = None, logKeywords = False, printBlogTitles=False):
        '''
            Get a list of comments with the same tfidf as this blog.

            Parameters
            ====================================

            conn2  `sqlite3.Connection` - A SQLite connection object for the word dictionary. Default as the a new connection to the global DB_FILE2 databse file.
            topK            `int`       - The top-K tf-idf words to be selected for comparisons.
            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.

        '''
        cur = self.conn.cursor()

        # Select a list of comments.
        similarBlogs = self.getSimilarBlogs(conn2, topK, retreiveCount, orderedBy=orderedBy, logKeywords=logKeywords, cachedWordList=cachedWordList)

        if (printBlogTitles):
            print([str(b.title) for b in similarBlogs])
        cur.execute("SELECT * FROM comments WHERE blog_id IN (" + ",".join([str(b.blog_id) for b in similarBlogs]) + ") ORDER BY RANDOM()" + ("" if retreiveCount is None else (" LIMIT " + str(retreiveCount))))
        return [Comment(*c) for c in cur.fetchall()]
    
    @staticmethod
    def getFromDB(blog_id, conn = sqlite3.connect(DB_FILE)):
        '''
            Get a Blog object from the database.

            Parameters
            ====================================

            blog_id     `int` - The blog_id to retrieve.
            conn    `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.

            Returns
            ====================================

            `Blog`  - a Blog object.
        '''
        blogRow = getData(conn = conn, table = "blogs", id1 = blog_id)
        return Blog(blog_id, blogRow[1], blogRow[2], conn = conn)
    
    @staticmethod
    def getBlogsWithNoComments(conn = sqlite3.connect(DB_FILE)):
        '''
            Get a list of Blog objects from the database which has no comments.

            Parameters
            ====================================

            conn    `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.

            Returns
            ====================================

            `list(Blog)`  - a list of Blog objects.
        '''
        cur = conn.cursor()

        # Select a list of comment.
        cur.execute("SELECT DISTINCT blog_id FROM comments")
        hasCommentIDs = set([c[0] for c in cur.fetchall()])
        cur.execute("SELECT DISTINCT blog_id FROM blogs")
        allCommentIDs = set([c[0] for c in cur.fetchall()])

        # Get the blogs of 
        notInBlogs = allCommentIDs-hasCommentIDs
        if len(notInBlogs) > 0:
            cur.execute("SELECT * FROM blogs WHERE blog_id IN (" + ",".join([str(nb) for nb in notInBlogs]) + ")")
            return [Blog(*b) for b in cur.fetchall()]
        else:
            return []
    
    @staticmethod
    def getCount(conn = sqlite3.connect(DB_FILE)):
        '''
            Get the total number of blogs in the database.

            Parameters
            ====================================

            conn    `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.

            Returns
            ====================================

            `int`  - Total number of blogs.
        '''
        cur = conn.cursor()
        cur.execute("SELECT COUNT(blog_id) FROM blogs")
        return cur.fetchone()[0]
    
    @staticmethod
    def getIDs(conn = sqlite3.connect(DB_FILE), rowLimit = None):
        '''
            Get the list of blog_id in the database.

            Parameters
            ====================================

            conn    `sqlite3.Connection`    - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.
            rowLimit    `int`               - The limit row count of blogs to return.

            Returns
            ====================================

            `list[int]`  - A list of blog_id .
        '''
        cur = conn.cursor()
        cur.execute("SELECT blog_id FROM blogs" + ("" if rowLimit is None else (" LIMIT " + str(rowLimit))))
        return [bi[0] for bi in cur.fetchall()]

    @staticmethod
    def getMajorityTitleLengths(conn = sqlite3.connect(DB_FILE), majority = 0.99):
        cur = conn.cursor()
        cur.execute("SELECT lenstr FROM (SELECT blog_id,length(title) AS lenstr FROM blogs ORDER BY length(title) DESC LIMIT " + str(int(Blog.getCount() * (1-majority))) + ") ORDER BY lenstr ASC LIMIT 1")
        maxLen = cur.fetchone()[0]
        return maxLen

class Comment:
    '''
        Class representing a Comment item.
    '''
    def __init__(self, blog_id, comment_id, body, reply):
        '''
            Create a Comment object.

            Parameters
            ====================================

            blog_id     `int`   - The blog_id
            comment_id  `int`   - The comment_id
            body     `str`   - The body
            reply       `str`   - The reply of this comment
        '''
        self.blog_id = blog_id
        self.comment_id = comment_id
        self.body = body
        self.reply = reply
    
    @staticmethod
    def getFromDB(blog_id, comment_id=None, conn = sqlite3.connect(DB_FILE)):
        '''
            Get a Blog object from the database.

            Parameters
            ====================================

            blog_id     `int|list[int]` - The blog_id to retrieve.
            comment_id  `int` - The comment_id to retrieve.
            conn    `sqlite3.Connection` - A SQLite connection object. Default as the a new connection to the global DB_FILE databse file.

            Returns
            ====================================

            `Comment|list(Comment)`  - a Comment object; a list of comment is returned if no comment_id is given.
        '''
        cur = conn.cursor()
        blogClause = ("blog_id IN (" + ",".join(blog_id) + ")") if isinstance(blog_id, list) else ("blog_id = " + str(blog_id))
        if comment_id is None:
            cur.execute("SELECT * FROM comments WHERE " + blogClause)
            commentRows = cur.fetchall()
            return [Comment(*c) for c in commentRows]
        else:
            cur.execute("SELECT * FROM comments WHERE " + blogClause + " AND comment_id = " + str(comment_id))
            commentRow = cur.fetchone()
            return Comment(*commentRow)
    
    @staticmethod
    def getMajorityBodyLengths(conn = sqlite3.connect(DB_FILE), majority = 0.99):
        cur = conn.cursor()
        cur.execute("SELECT COUNT(comment_id) FROM comments")
        count = cur.fetchone()[0]
        print("Total Comments: ", count)
        cur.execute("SELECT lenstr FROM (SELECT length(comments) AS lenstr FROM comments ORDER BY length(comments) DESC LIMIT " + str(int(count * (1-majority))) + ") ORDER BY lenstr ASC LIMIT 1")
        maxLen = cur.fetchone()[0]
        cur.execute("SELECT COUNT(comment_id) FROM comments WHERE length(comments) < " + str(maxLen))
        print("Total Valid Comments with limited comment length: ", cur.fetchone()[0])
        cur.execute("SELECT COUNT(blog_id) FROM (SELECT MIN(length(comments)) AS minLen, blog_id FROM comments GROUP BY blog_id) WHERE minLen < " + str(maxLen))
        print("Total Valid Blogs with limited comment length: ", cur.fetchone()[0])
        return maxLen

class Keyword:
    '''
        Class representing a Keyword item.
    '''
    def __init__(self, blog_id, keyword_id, keyword):
        '''
            Create a Comment object.

            Parameters
            ====================================
            blog_id     `int`   - The blog_id
            keyword_id  `int`   - The keyword_id
            keyword     `str`   - The keyword
        '''
        self.blog_id = blog_id
        self.keyword_id = keyword_id
        self.keyword = keyword 

class Tag:
    '''
        Class representing a Keyword item.
    '''
    def __init__(self, tag_id, keyword_id, keyword):
        '''
            Create a Comment object.

            Parameters
            ====================================
            blog_id     `int`   - The blog_id
            tag_id      `int`   - The tag_id
            tag         `str`   - The tag
        '''
        self.blog_id = blog_id
        self.tag_id = tag_id
        self.tag = tag 

class WordDict():
    ''' Class representing a dictionary of words in the corpus.
    '''
    def __init__(self, conn = sqlite3.connect(DB_FILE), conn2 = sqlite3.connect(DB_FILE2), rowLimit = None, segType = 2):
        ''' Build the dictionary of all the Chinese words and English words.

            Parameters
            ====================================

            conn    `sqlite3.Connection`    - A SQLite connection object for the data source. Default as the a new connection to the global DB_FILE databse file.
            conn2    `sqlite3.Connection`   - A SQLite connection object for the word dictionary. Default as the a new connection to the global DB_FILE2 databse file.
            rowLimit    `int`               - The limit row count of blogs to return.
            segType     `int`               - 0: by characters; 1: by characters, but remove english words; 2: by jieba
        '''
        self.conn = conn
        self.conn2 = conn2
        self.segType = segType
        self.rowLimit = rowLimit
        self.corpusCount = None
        self.results = None
        self.wordMap = None
        self.idMap = None
    
    def initalCorpusCount(self, forceRefersh = False):
        ''' Initalize the value of how much observation was used to build the dictionary.

            Parameters
            ====================================

            forceRefersh    `bool`  - Force a refresh count from a connection to the db, instead of using the cached variable .corpusCount .
        '''
        if (self.corpusCount is None or forceRefersh):
            cur = self.conn.cursor()
            bids = Blog.getIDs(self.conn, self.rowLimit)
            count = len(bids) * 2
            cur.execute("SELECT COUNT(comment_id) FROM comments WHERE blog_id IN (" + ",".join([str(bid) for bid in bids]) + ")")
            count += cur.fetchone()[0]
            self.corpusCount = count
        
    def getRows(self, retrieveCount = None):
        ''' Select rows of dictionary.

            Parameters
            ====================================

            retrieveCount   `int`   - The no. of rows (based on descending order of word count) to retrieve

            Returns
            ====================================

            `tuple()`   - A data row of word dictionary.
        '''
        cur = self.conn2.cursor()
        cur.execute("SELECT * FROM word_dict ORDER BY count DESC" + ("" if retrieveCount is None else (" LIMIT " + str(retrieveCount))))
        return self.setCacheResults(cur.fetchall())

    def getRowsByFreq(self, retrieveCount = None, minFreq = 0.0005, maxFreq = 0.025):
        ''' Select rows of dictionary within a range of frequency (inclusive).

            Parameters
            ====================================

            retrieveCount   `int`   - The no. of rows (based on descending order of word count) to retrieve.
            minFreq         `float` - The minimum frequency to be appeared across the corpus. (inclusive)
            maxFreq         `float` - The maximum frequency to be appeared across the corpus. (inclusive)
            
            Returns
            ====================================

            `tuple()`   - A data row of word dictionary.
        '''
        cur = self.conn2.cursor()
        self.initalCorpusCount()
        cur.execute("SELECT * FROM word_dict WHERE freq >= " + str(minFreq) + " AND freq <= " + str(maxFreq) + " ORDER BY count DESC" + ("" if retrieveCount is None else (" LIMIT " + str(retrieveCount))))
        return self.setCacheResults(cur.fetchall())
    
    def keepChineseOnly(self):
        ''' Remove non-Chinese words in the existing results.
        '''
        return self.setCacheResults([w for w in self.results if re.search("[\u4E00-\u9FFF]", w[0]) is not None])
    
    def setCacheResults(self, results):
        self.results = results
        self.wordMap = {w[0]: w for w in self.results}
        self.idMap = {w[1]: w for w in self.results}
        return results

    def transformTextToIDs(self, text, log = False):
        ''' Transform text to dictionary ids.

            Parameters
            ====================================

            text    `str`   - The text to be transformed.
            log     `bool`  - Whether to print original text, segmented words and dictionary ids.

            Returns
            ====================================

            `list[id]`  - The list of transformed dictionary ids.
        '''
        words = WordDict.segment(text, segType = self.segType)

        if self.results is None:
            raise ValueError("Must cache results using setCacheResults before use.")
        
        ids = [self.wordMap[w] for w in words if w in self.wordMap]
        
        if log:
            print("Original: ", text)
            print("Segmented: ", words)
            print("Dict ID: ", ids)
        
        return ids
    
    def getVocabSize(self):
        ''' Get the size of the original dictionary.

            Returns
            ====================================

            `int`   - The number of records in the dictionary
        '''
        cur = self.conn2.cursor()
        cur.execute("SELECT count(id) FROM word_dict")
        return cur.fetchone()[0]
    
    def getScopedVocabSize(self):
        ''' Get the size of the currently scoped dictionary.

            Returns
            ====================================

            `int`   - The number of records in the dictionary
        '''
        return len(self.results)

    def buildTFIDF(self):
        # Create new tf-idf tables
        cur2 = self.conn2.cursor()
        print("DB Initiation - Creating tf-idf tables")
        cur2.execute('''DROP TABLE IF EXISTS blogs_tf_idf''')
        cur2.execute('''DROP TABLE IF EXISTS blogs_title_tf_idf''')
        cur2.execute('''DROP TABLE IF EXISTS comments_tf_idf''')
        self.conn2.commit()
        cur2.execute('''CREATE TABLE blogs_tf_idf
                    (blog_id    INTEGER, 
                    word_id     INTEGER,
                    count       INTEGER,
                    tf_idf      FLOAT,
                    PRIMARY KEY(blog_id,word_id),
                    FOREIGN KEY(word_id) REFERENCES word_dict(id))''')
        self.conn2.commit()
        cur2.execute('''CREATE TABLE blogs_title_tf_idf
                    (blog_id    INTEGER, 
                    word_id     INTEGER,
                    count       INTEGER,
                    tf_idf      FLOAT,
                    PRIMARY KEY(blog_id,word_id),
                    FOREIGN KEY(word_id) REFERENCES word_dict(id))''')
        self.conn2.commit()
        cur2.execute('''CREATE TABLE comments_tf_idf
                    (blog_id    INTEGER, 
                    comment_id  INTEGER,
                    word_id     INTEGER,
                    count       INTEGER,
                    tf_idf      FLOAT,
                    PRIMARY KEY(blog_id,comment_id,word_id),
                    FOREIGN KEY(word_id) REFERENCES word_dict(id))''')
        self.conn2.commit()
        

        print("DB TFIDF Initialization - Loop Entries")
        cur = self.conn.cursor()
        # Select the title and blog ids form all the blogs
        allEntries = cur.execute("SELECT blog_id,title,body FROM blogs" + ("" if self.rowLimit is None else (" LIMIT " + str(self.rowLimit))))
        blogsTFIDF = dict()
        blogsTitleTFIDF = dict()
        commentsTFIDF = dict()
        idx = 0

        # Loop all the blogs for tf-idf preparation
        blogCount = Blog.getCount(self.conn) if self.rowLimit is None else self.rowLimit
        for i in allEntries:
            # Segment the title and push into the counter
            allWordsTitle = self.transformTextToIDs(i[1])
            titleCounter = collections.Counter(allWordsTitle)
            eleLen = sum(titleCounter.values())
            # There may be cases with no valid words found
            if (eleLen > 0):
                blogsTitleTFIDF[i[0]] = {w[1]: (ctn, ctn/eleLen*w[4]) for w,ctn in titleCounter.items()}
           
            # Segment the body and push into the counter
            allWordsBody = self.transformTextToIDs(i[2])
            bodyCounter = collections.Counter(allWordsBody)
            eleLen = sum(bodyCounter.values())
            # There may be cases with no valid words found
            if (eleLen > 0):
                blogsTFIDF[i[0]] = {w[1]: (ctn, ctn/eleLen*w[4]) for w,ctn in bodyCounter.items()}

            # Get the comments and push all the words
            comments = Comment.getFromDB(i[0])
            commentsTFIDF[i[0]] = dict()
            for c in comments:
                allWordsComment = self.transformTextToIDs(c.body)
                commentCounter = collections.Counter(allWordsComment)
                eleLen = sum(commentCounter.values())
                # There may be cases with no valid words found
                if (eleLen > 0):
                    commentsTFIDF[i[0]][c.comment_id] = {w[1]: (ctn, ctn/eleLen*w[4]) for w,ctn in commentCounter.items()}

            # Log progresses
            idx+=1
            if (idx % 500 == 0):
                print("Processing... (", idx/blogCount*100, " %)")
        
        # Loop all the data and insert into the db
        titleTFIDFLen = len(blogsTitleTFIDF)
        idx = 0
        for blog_id,titleWords in blogsTitleTFIDF.items():
            for word_id,titleTfidf in titleWords.items():
                cur2.execute("INSERT INTO blogs_title_tf_idf VALUES(" + str(blog_id) + ", " + str(word_id) + ", " + str(titleTfidf[0]) + ", " + str(titleTfidf[1]) + ")")
            
            # Log progresses
            idx += 1
            if (idx % 500 == 0):
                print("Processing - Blog Titles ... (", idx/titleTFIDFLen*100, " %)")
        
        # Loop all the data and insert into the db
        blogTFIDFLen = len(blogsTFIDF)
        idx = 0
        for blog_id,blogWords in blogsTFIDF.items():
            for word_id,blogTfidf in blogWords.items():
                cur2.execute("INSERT INTO blogs_tf_idf VALUES(" + str(blog_id) + ", " + str(word_id) + ", " + str(blogTfidf[0]) + ", " + str(blogTfidf[1]) + ")")
            
            # Log progresses
            idx += 1
            if (idx % 500 == 0):
                print("Processing - Blogs ... (", idx/blogTFIDFLen*100, " %)")
        
        # Loop all the comments and insert into the db
        commentTFIDFLen = len(commentsTFIDF)
        idx = 0
        for blog_id,comments in commentsTFIDF.items():
            for comment_id,commentWords in comments.items():
                for word_id,commentTfidf in commentWords.items():
                    cur2.execute("INSERT INTO comments_tf_idf VALUES(" + str(blog_id) + ", " + str(comment_id) + ", " + str(word_id) + ", " + str(commentTfidf[0]) + ", " + str(commentTfidf[1]) + ")")
            # Log progresses
            idx += 1
            if (idx % 500 == 0):
                print("Processing - Comments ... (", idx/commentTFIDFLen*100, " %)")
        
        self.conn2.commit()

    @staticmethod
    def getWordList(conn2 = sqlite3.connect(DB_FILE2)):
        cur2 = conn2.cursor();
        cur2.execute("SELECT word_id FROM blogs_tf_idf GROUP BY word_id")
        return [r[0] for r in cur2.fetchall()]

    @staticmethod
    def removeInvalid(string):
        ''' Remove numbers, emails, URLs.

            Parameters
            ====================================

            text    `str`   - The text to be segmented.

            Returns
            ====================================

            `str`   - The string withought numbers, emails, URLs.
        '''
        return re.sub("\r|[0-9.]+|([-a-zA-Z0-9.`?{}]+@\w+\.\w+)|(((http|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)?)", "",string)

    @staticmethod
    def segment(text, segType = 2):
        ''' Segment the incoming text into Chinese and English words.

            Parameters
            ====================================

            text    `str`   - The text to be segmented
            segType `int`   - 0: by characters; 1: by characters, but remove english words; 2: by jieba

            Returns
            ====================================

            `list[str]`   - Segmented text in a list.
        '''
        # Decompose HTML if needed
        if (re.search("<.+>", text)):
            text = re.sub("\n+", "\n", "".join([s for s in BeautifulSoup(text, "html.parser").find_all(string=True) if (s.parent.name not in ["script", "style", "select", "option"] and not isinstance(s, Comment))]))
        
        # Skip English and Numbers if needed
        if (segType == 1):
            text = re.sub("[0-9a-zA-Z]","",text)

        # Split all the characters
        if (segType == 2):
            words = [*jieba.cut(text, cut_all=False)]

        # OR use jieba to perform word segmentation
        elif (segType == 0 or segType == 1):
            words = [*WordDict.removeInvalid(text)]

            # Replace the split english characters with a grouped word
            if (len(words) > 1):
                startEng = [*filter(lambda c: re.match("[a-zA-Z\']",c[1]) and (c[0]==0 or re.match("[a-zA-Z]",words[c[0]-1]) is None), enumerate(words))]
                endEng = [*filter(lambda c: re.match("[a-zA-Z\']",c[1]) and (c[0]==(len(words)-1) or re.match("[a-zA-Z]",words[c[0]+1]) is None), enumerate(words))]
            else:
                startEng = []
                endEng = []
            replaceEle = zip([s[0] for s in startEng], [e[0]+1 for e in endEng])
            accLen = 0
            for r in replaceEle:
                start = r[0] - accLen
                end = r[1] - accLen
                words[start] = "".join(words[start:end])
                words[start+1:] = words[end:]
                accLen += end-start-1
        
        return words

    @staticmethod
    def build(conn = sqlite3.connect(DB_FILE), conn2 = sqlite3.connect(DB_FILE2), rowLimit = None, segType = 2):
        ''' Build the dictionary of all the Chinese words and English words.

            Parameters
            ====================================

            conn    `sqlite3.Connection`    - A SQLite connection object for the data source. Default as the a new connection to the global DB_FILE databse file.
            conn2    `sqlite3.Connection`   - A SQLite connection object for the word dictionary. Default as the a new connection to the global DB_FILE2 databse file.
            rowLimit    `int`               - The limit row count of blogs to return.
            segType     `int`               - 0: by characters; 1: by characters, but remove english words; 2: by jieba

            Returns
            ====================================

            `WordDict - A dictionary object for the connection of currently building dictionary.
        '''

        cur = conn.cursor()

        # Count the number of blogs and collect all the blog ids
        if (rowLimit is None):
            cur.execute("SELECT COUNT(blog_id) FROM blogs" + ("" if rowLimit is None else (" LIMIT " + str(rowLimit))))
            blogCount = cur.fetchall()[0][0]
        else:
            blogCount = rowLimit

        # Create dictionary table in the new db
        cur2 = conn2.cursor()
        print("DB Initiation - Creating dictionary table")
        cur2.execute('''DROP TABLE IF EXISTS word_dict''')
        cur2.execute('''DROP TABLE IF EXISTS blogs_tf_idf''')
        cur2.execute('''DROP TABLE IF EXISTS blogs_title_tf_idf''')
        cur2.execute('''DROP TABLE IF EXISTS comments_tf_idf''')
        conn2.commit()
        cur2.execute('''CREATE TABLE word_dict
                    (word    TEXT, 
                    id       INTEGER,
                    count    INTEGER,
                    freq     FLOAT,
                    idf      FLOAT,
                    PRIMARY KEY(id))''')
        conn2.commit()

        wordDict = WordDict(conn, conn2, segType=segType, rowLimit=rowLimit);


        print("DB Initiation - Loop Entries")
        # Select the title and blog ids form all the blogs
        allEntries = cur.execute("SELECT blog_id,title,body FROM blogs" + ("" if rowLimit is None else (" LIMIT " + str(rowLimit))))
        wordCount = dict()
        idx = 0
        wordDict.initalCorpusCount()
        corpusCount = wordDict.corpusCount

        # Loop all the blogs for dictionary preparation
        for i in allEntries:
            # Segment the title and push into the counter
            allWordsTitle = WordDict.segment(i[1], segType = segType)
            wordsTitle = set(allWordsTitle)
            for w in wordsTitle:
                wordCount[w] = wordCount.setdefault(w, 0) + 1
            
            # Segment the body and push into the counter
            allWordsBody = WordDict.segment(i[2], segType = segType)
            wordsBody = set(allWordsBody)
            for w in wordsBody:
                wordCount[w] = wordCount.setdefault(w, 0) + 1

            # Get the comments and push all the words
            comments = Comment.getFromDB(i[0])
            for c in comments:
                allWordsComment = WordDict.segment(c.body, segType = segType)
                wordsComment = set(allWordsComment)
                for w in wordsComment:
                    wordCount[w] = wordCount.setdefault(w, 0) + 1

            # Log progresses
            idx+=1
            if (idx % 500 == 0):
                print("Processing... (", idx/blogCount*100, " %)")

        # Loop all the words and insert into the db
        wordCountLen = len(wordCount);
        for idx,w in enumerate(wordCount):
            line = "INSERT INTO word_dict VALUES('" + w.replace("'","''") + "', " + str(idx) + ", " + str(wordCount[w]) + ", " + str(wordCount[w]/corpusCount) + ", " + str(math.log(corpusCount/wordCount[w])) + ")"
            cur2.execute(line)
            if (idx % 500 == 0):
                print("Insertion... (", idx/wordCountLen*100, " %)")
        
        conn2.commit()

        return wordDict


''' 
    Initialize Database and Data
'''
#connection = initDB()
#connection = initData(connection, closeConnection=True)


'''
    Test of getting comments from a blog_id 2
'''
connection = sqlite3.connect(DB_FILE)
connection2 = sqlite3.connect(DB_FILE2)

wordList = WordDict.getWordList(connection2)

for i in range(0,10):
    # Get a blog from blog_id
    testBlog = Blog.getFromDB(int(random.random()*200000), conn = connection)
    print("Blog ID: ", testBlog.blog_id)
    print("Blog Title: ", testBlog.title)
    print("Keywords:\n", [k.keyword for k in testBlog.getThisKeywords()])
    
    # Get comments in this post
    nowComments = testBlog.getThisComments()
    print("\nComments in this post:\n", *[c.body for c in nowComments])

    # Get random comments in other posts
    #nowComments = testBlog.getOtherComments(retreiveCount=5)
    #print("Comments in other post:\n", [c.body for c in nowComments])

    # Get similar blogs
    #print("\nSimilar Blog (Keywords):\n", *[(b.blog_id, b.title) for b in testBlog.getSimilarBlogsByKeywords(logKeywords=True)])
    #print("\nSimilar Blog (Tags):\n", *[(b.blog_id, b.title) for b in testBlog.getSimilarBlogsByTags(logKeywords=True)])
    #print("\nSimilar Blogs (TFIDF):\n", *[(b.blog_id, b.title) for b in testBlog.getSimilarBlogsByTFIDF(connection2, logKeywords=True)])
    #print("\nSimilar Blog (Overall):\n", *[(b.blog_id, b.title) for b in testBlog.getSimilarBlogs(logKeywords=True, orderedBy="tfidf")])
    
    # Get similar comments
    #print("\nOriginal Comments (Keywords):\n", *[(c.blog_id, c.body) for c in testBlog.getThisComments()], "\nSimilar Comments:\n", [c.body for c in testBlog.getCommentsFromSimilarKeywords(retreiveCount=10)])
    #print("\nOriginal Comments (Tags):\n", *[(c.blog_id, c.body) for c in testBlog.getThisComments()], "\nSimilar Comments:\n", [c.body for c in testBlog.getCommentsFromSimilarTags(retreiveCount=10)])
    #print("\nOriginal Comments (TFIDF):\n", *[(c.blog_id, c.body) for c in testBlog.getThisComments()], "\nSimilar Comments:\n", [c.body for c in testBlog.getCommentsFromSimilarTFIDF(retreiveCount=10)])
    print("\nOriginal Comments (Overall):\n", *[(c.blog_id, c.body) for c in testBlog.getThisComments()], "\nSimilar Comments:\n", [c.body for c in testBlog.getCommentsFromSimilarBlogs(retreiveCount=10, orderedBy="tfidf", printBlogTitles = True, logKeywords = True, cachedWordList=wordList)])
    
    # Get the count of blogs with no comments
    # print(Blog.getBlogsWithNoComments(connection))
    print("\n\n")

'''
# Build word dict initially
wordDict = WordDict.build(rowLimit=100000)


# Initialize the settings
#wordDict = WordDict(rowLimit=100000)

# Initialize the counting
wordDict.initalCorpusCount()

# Get the rows by frequency (default: 0.001-0.02)
wordDict.getRowsByFreq()

# Keep Chinese dictionary only
wordDict.keepChineseOnly()

print("Scoped Vocab Count: ", wordDict.getScopedVocabSize())

wordDict.buildTFIDF()

# Sample the first 500 records to check it
#print("Sample of Rows: \n", wordDict.results[:500])
'''