import sqlite3
import os
import csv

# Data file processing
DATA_FOLDER = "./"
DB_FILE = "pixnet.db"

class Blog():
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
        cur.execute("SELECT * FROM comments WHERE blog_id = " + str(self.blog_id) + ((" AND comment_id <= " + str(retreiveCount)) if retreiveCount is not None else ""))
        comments = cur.fetchall()
        return [Comment(*c) for c in comments]

    def getOtherComments(self, retreiveCount = 5):
        '''
            Get a list of comments not in this blog.

            Parameters
            ====================================

            retreiveCount   `None|int` - The topmost k comments, all if None is given.

            Returns
            ====================================

            `list(Comment)`  - a list of Comment objects.
        '''
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM comments WHERE blog_id != " + str(self.blog_id) + ((" ORDER BY RANDOM() LIMIT " + str(retreiveCount)) if retreiveCount is not None else ""))
        comments = cur.fetchall()
        return [Comment(*c) for c in comments]
    
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


class Comment():
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




csv.field_size_limit(100000000)


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



''' 
    Initialize Database and Data
'''
#connection = initDB()
#connection = initData(connection, closeConnection=True)


'''
    Test of getting comments from a blog_id 2
connection = sqlite3.connect(DB_FILE)

# Get a blog from blog_id
testBlog = Blog.getFromDB(2, conn = connection)

# Get comments in this post
nowComments = testBlog.getThisComments()
print("Comments in this post:\n", [data.trim_illegal_char(c.body) for c in nowComments])

# Get random comments in other posts
nowComments = testBlog.getOtherComments(retreiveCount=5)
print("Comments in other post:\n", [data.trim_illegal_char(c.body) for c in nowComments])
'''
