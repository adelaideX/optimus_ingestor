"""
A service for generating geo location derived datasets from clickstream data
"""
import json
import urllib2
import warnings

import MySQLdb
from pymongo import MongoClient

import base_service
import config
import utils


class GeoLocation(base_service.BaseService):
    """
    Generates derived datasets for each persons location (IP) and each course
    """
    inst = None

    def __init__(self):
        GeoLocation.inst = self
        super(GeoLocation, self).__init__()

        # The pretty name of the service
        self.pretty_name = "Geo Location"
        self.enabled = True
        # Whether to run more than once
        self.loop = True
        # The amount of time to sleep in seconds
        self.sleep_time = 60

        self.gl_table = 'geo'
        self.gl_db = 'Geo_Location'

        self.sql_gl_conn = None
        self.sql_course_conn = None

        # Vars
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = ""
        self.mongo_collection = None
        self.mongo_collectionname = ""
        self.courses = {}

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.courses = self.get_all_courses()
        self.sql_gl_conn = self.connect_to_sql(self.sql_gl_conn, "Geo_Location", True)
        self.sql_course_conn = self.connect_to_sql(self.sql_course_conn, "", True)

        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("GeoLocation")
        last_timefinder = self.find_last_run_ingest("TimeFinder")
        last_iptocountry = self.find_last_run_ingest("IpToCountry")
        last_dbstate = self.find_last_run_ingest("DatabaseState")

        if self.finished_ingestion("TimeFinder") and \
                        last_run < last_timefinder and \
                self.finished_ingestion("IpToCountry") and \
                        last_run < last_iptocountry and \
                self.finished_ingestion("DatabaseState") and \
                        last_run < last_dbstate:
            # Clean 'pc_table'
            self.clean_gl_db()

            for course_id, course in self.courses.items():
                # Get chapters from course info
                json_file = course['dbname'].replace("_", "-") + '.json'
                courseinfo = self.loadcourseinfo(json_file)
                if courseinfo is None:
                    utils.log("Can not find course info for ." + str(course_id))
                    continue

                # Create 'pc_table'
                self.create_gl_table()

                # Dict of geo location data items, key is the user id
                gl_dict = {}
                user_id_list = []

                gl_course_id = course['mongoname']

                utils.log("LOADING COURSE {" + gl_course_id + "}")

                # find all user_id
                utils.log("{auth_user}")
                # Select the database
                self.sql_course_conn.select_db(course['dbname'])
                userid_cursor = self.sql_course_conn.cursor()
                query = "SELECT id FROM auth_user"
                userid_cursor.execute(query)
                result = userid_cursor.fetchall()
                for x in result:
                    user_id_list.append(x[0])

                # Tracking logs
                utils.log("{logs}")
                self.mongo_dbname = "logs"
                self.mongo_collectionname = "clickstream"
                self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)

                geo_user_events = self.mongo_collection.aggregate([
                    {"$match": {"context.course_id": gl_course_id, "context.user_id": {"$type": 16}}},
                    {"$group": {
                        "_id": {
                            "user_id": "$context.user_id",
                            "country": "$country",
                            "subdivision": "$subdivision",
                            "city": "$city",
                            "loc": "$loc",
                        },
                        "eventSum": {"$sum": 1},
                        "last_event": {"$last": "$time"}
                    }}
                ], allowDiskUse=True, batchSize=100, useCursor=True)  # ['result'] batchSize=100, , useCursor=False

                gl_cursor = self.sql_gl_conn.cursor()
                utils.log("save to {geolocation}")
                tablename = self.gl_table + "_" + course_id

                for item in geo_user_events:
                    user_id = item["_id"]["user_id"]
                    country = item["_id"].get("country")
                    if country != '' and country is not None and user_id in user_id_list:
                        self.save2db(gl_cursor, tablename,
                                     course_id=gl_course_id,
                                     user_id=user_id,
                                     country=country,
                                     subdivision=item["_id"].get("subdivision", ""),
                                     city=item["_id"].get("city", ""),
                                     loc=item["_id"].get("loc", {"x": "", "y": ""}),
                                     last_event=item.get("last_event"),
                                     nevents=item.get("eventSum", 0)
                                     )
                    else:
                        utils.log("context.user_id: %s does not exist in %s {auth_user} or no country identified." % (
                            user_id, gl_course_id))
                self.sql_gl_conn.commit()

            self.save_run_ingest()
            utils.log("Geo Location completed")

    def save2db(self, cursor, table, **kwargs):

        lon = kwargs['loc'].get("x", "")
        lat = kwargs['loc'].get("y", "")
        if kwargs['city']:
            city = kwargs['city'].replace("'", "\\\'")
        else:
            city = ""

        parameters = kwargs['course_id'], kwargs['user_id'], kwargs['country'], kwargs['subdivision'], city, lat, lon, \
                     kwargs['nevents'], kwargs['last_event']

        warnings.filterwarnings('ignore', category=MySQLdb.Warning)

        query = "INSERT INTO " + table + " (course_id, user_id, country, subdivision, city, lat, lon, nevents, last_event) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s')" % parameters
        cursor.execute(query)

        warnings.filterwarnings('always', category=MySQLdb.Warning)

    def create_gl_table(self):
        """
        Creates the geo location table
        """
        cursor = self.sql_gl_conn.cursor()
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "user_id", "col_type": "varchar(255)"},
            {"col_name": "country", "col_type": "varchar(255)"},
            {"col_name": "subdivision", "col_type": "varchar(255)"},
            {"col_name": "city", "col_type": "varchar(255)"},
            {"col_name": "lat", "col_type": "varchar(255)"},
            {"col_name": "lon", "col_type": "varchar(255)"},
            {"col_name": "nevents", "col_type": "int"},
            {"col_name": "last_event", "col_type": "date"}
        ]
        for course_id, course in self.courses.items():
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)
            gl_tablename = self.gl_table + "_" + course_id
            query = "CREATE TABLE IF NOT EXISTS " + gl_tablename
            query += " ("
            for column in columns:
                query += column["col_name"] + " " + column["col_type"] + ", "
            query += " KEY idx_course_uid(`course_id`, `user_id`, `country`)) DEFAULT CHARSET=utf8;"
            # print query
            cursor.execute(query)
            warnings.filterwarnings('always', category=MySQLdb.Warning)

    def loadcourseinfo(self, json_file):
        """
        Loads the course information from JSON course structure file
        :param json_file: the name of the course structure file
        :return the course information
        """
        print self
        courseurl = config.SERVER_URL + '/datasources/course_structure/' + json_file
        print "ATTEMPTING TO LOAD " + courseurl
        courseinfofile = urllib2.urlopen(courseurl)
        if courseinfofile:
            courseinfo = json.load(courseinfofile)
            return courseinfo
        return None

    def clean_gl_db(self):
        """
        Deletes the existing geo tables
        """
        gl_cursor = self.sql_gl_conn.cursor()
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        for course_id, course in self.courses.items():
            gl_tablename = self.gl_table + "_" + course_id
            query = "DROP TABLE IF EXISTS %s" % gl_tablename
            gl_cursor.execute(query)
        self.sql_gl_conn.commit()
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        utils.log(self.gl_db + " has been cleaned.")

    # Connects to a Mongo Database
    def connect_to_mongo(self, db_name="", collection_name=""):
        """
        Connects to a mongo database
        :param db_name:
        :param collection_name:
        :return:
        """
        db_name = safe_name(db_name)
        try:
            if self.mongo_client is None:
                self.mongo_client = MongoClient('localhost', 27017)
            if db_name != "":
                self.mongo_db = self.mongo_client[db_name]
                if self.mongo_db:
                    self.mongo_dbname = db_name
                    if collection_name != "":
                        self.mongo_collection = self.mongo_db[collection_name]
                        if self.mongo_collection:
                            self.mongo_collectionname = collection_name
            return True
        except Exception, e:
            utils.log("Could not connect to MongoDB: %s" % e)
        return False


def safe_name(filename):
    """
    Protection against bad database names
    :param filename: the filename
    :return: a safe fileneame
    """
    return str(filename).replace('.', '_')


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    print path
    required_files = []
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "GeoLocation"


def service():
    """
    Returns an instance of the service
    """
    return GeoLocation()
