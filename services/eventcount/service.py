"""
Service for importing the edX clickstream
"""
import warnings

import MySQLdb
import dateutil

import base_service
import utils
import time
import config
import urllib2
import json
from datetime import datetime


class Eventcount(base_service.BaseService):
    """
    Collects the student engagement data - chapter, sequential and video logs
    from courseware_studentmodule table by week of activity
    """
    inst = None

    def __init__(self):
        Eventcount.inst = self
        super(Eventcount, self).__init__()

        # The pretty name of the service
        self.pretty_name = "Eventcount"
        # Whether the service is enabled
        self.enabled = True
        # Whether to run more than once
        self.loop = True
        # The amount of time to sleep in seconds
        self.sleep_time = 60

        self.ec_table = "courseevent"
        self.ec_db = "Course_Event"

        self.sql_ec_conn = None
        self.sql_cm_conn = None

        self.cm_table = "courseware_studentmodule"

        self.courses = {}

        self.initialize()

        pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.courses = self.get_all_courses()
        self.sql_ec_conn = self.connect_to_sql(self.sql_ec_conn, self.ec_db, True)
        self.sql_cm_conn = self.connect_to_sql(self.sql_cm_conn, "", True)

        print "SETTING UP EVENT COUNT"
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """

        last_run = self.find_last_run_ingest("EventCount")
        last_timefinder = self.find_last_run_ingest("TimeFinder")
        last_dbstate = self.find_last_run_ingest("DatabaseState")

        if self.finished_ingestion("TimeFinder") \
                and last_run < last_timefinder \
                and self.finished_ingestion("DatabaseState") \
                and last_run < last_dbstate:
            print "RUNNING THE EVENT COUNT"
            utils.log("The Event Count starts at: " + str(datetime.now()))
            self.clean_ec_db()
            print "STARTING EVENT COUNT"
            for course_id, course in self.courses.items():
                print "EVENT COUNTING " + str(course_id)
                # print course_id

                # Get events from course info
                json_file = course['dbname'].replace("_", "-") + ".json"
                course_db_name = course['dbname']
                courseinfo = self.loadcourseinfo(json_file)
                if courseinfo is None:
                    utils.log("Can not find course info for ." + str(course_id))
                    continue

                # print courseinfo

                # Get events
                events = self.get_events(courseinfo)

                dates = self.get_dates(courseinfo)

                # Create courseevent table
                self.create_ec_table(course_id, events)

                events_date_counts = {}

                # get the date from courseware_studentmodule
                for event_id in events:
                    self.group_event_by_date(course_db_name, event_id, events_date_counts, dates)

                # Insert records into database
                self.insert_ec_table(course_id, events_date_counts)

                self.sql_ec_conn.commit()

            self.loop = False
            utils.log("The Event Count ends at: " + str(datetime.now()))
            self.save_run_ingest()

    def get_dates(self, courseinfo):
        """
        Get the course start and end dates from the courseinfo file
        :param courseinfo:
        :return:
        """
        course_launch_date = None
        course_close_date = None
        if 'start' in courseinfo:
            try:
                course_launch_time = dateutil.parser.parse(courseinfo['start'].replace('"', ""))
                course_launch_date = course_launch_time.date()
            except Exception:
                print "ERROR: BAD COURSE START DATE"
                pass
        else:
            utils.log("Course start date not found")
            pass
        if 'end' in courseinfo:
            try:
                course_close_time = dateutil.parser.parse(courseinfo['end'].replace('"', ""))
                course_close_date = course_close_time.date()
            except Exception:
                print "ERROR: BAD COURSE END DATE"
                pass
        else:
            utils.log("Course end date not found")

        return [course_launch_date, course_close_date]

    def get_chapters(self, obj, found=None):
        """
        Gets the chapter for the object (recursive)
        :param self:
        :param obj: the object being added
        :param found: an array of previously found elements
        :return the found object
        """
        if not found:
            found = []
        if obj['tag'] == 'chapter':
            found.append(obj)
        else:
            for child in obj['children']:
                found = self.get_chapters(child, found)
        return found

    def get_events(self, courseinfo):
        """
        Analyses chapters within a course
        :param courseinfo: The json object containing the course structure
        :param self:
        :param chapters: An array of chapter dictionaries
        :return: A dict containing the events in order
        """

        chapters = []
        chapters = self.get_chapters(courseinfo, chapters)

        events = []
        for chapter in chapters:
            # check for event id and not invisible
            if 'url_name' in chapter and 'visible_to_staff_only' not in chapter:
                events.append(chapter['url_name'])
                for sequential in chapter['children']:
                    if sequential['tag'] == 'sequential' and 'children' in sequential:
                        # check for event id and not invisible
                        if 'url_name' in sequential and 'visible_to_staff_only' not in sequential:
                            events.append(sequential['url_name'])
                            for vertical in sequential['children']:
                                if vertical['tag'] == 'vertical' and 'children' in vertical:
                                    for child in vertical['children']:
                                        # check for event id and not invisible
                                        if 'url_name' in child and 'visible_to_staff_only' not in child:
                                            if child['tag'] == 'video':
                                                events.append(child['url_name'])

        return events

    def create_ec_table(self, course_id, events):
        """
        Creates the event course tables
        """
        cursor = self.sql_ec_conn.cursor()
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "event_date", "col_type": "date"},
        ]

        # Add events to columns, add "u_" in front of event id to avoid possible column name syntax problems.
        for event_id in events:
            event_id = event_id.replace(".", "$")
            columns.append({"col_name": "u_" + event_id, "col_type": "int DEFAULT 0"})

        ec_tablename = self.ec_table + "_" + course_id
        query = "CREATE TABLE IF NOT EXISTS " + ec_tablename
        query += " ("
        for column in columns:
            query += column["col_name"] + " " + column["col_type"] + ", "
        query = query[:-2]
        query += " );"
        cursor.execute(query)
        self.sql_ec_conn.commit()

    def insert_ec_table(self, course_id, events_date_counts):
        """
        Inserts the counts for the related events into event count table
        :param course_id:
        :param events_date_counts:
        """
        cursor = self.sql_ec_conn.cursor()

        ec_tablename = self.ec_table + "_" + course_id

        for event_date, events_counts in events_date_counts.iteritems():
            if len(events_counts) == 0:
                continue

            columns_name = ""
            columns_value = ""
            for event_id, count in events_counts.iteritems():
                columns_name += "u_" + event_id.replace(".", "$") + ", "
                columns_value += str(count) + ", "

            columns_name = columns_name[:-2]
            columns_value = columns_value[:-2]

            query = "INSERT INTO " + ec_tablename + " (course_id, event_date, " + columns_name + ") VALUES ('" + course_id + "', '" + event_date.strftime(
                "%Y-%m-%d") + "', " + columns_value + ");"

            # print query

            cursor.execute(query)
        pass

    def loadcourseinfo(self, json_file):
        """
        Loads the course information from JSON course structure file
        :param json_file: the name of the course structure file
        :return the course information
        """
        print self
        courseurl = config.SERVER_URL + '/datasources/course_structure/' + json_file
        courseinfofile = urllib2.urlopen(courseurl)
        if courseinfofile:
            courseinfo = json.load(courseinfofile)
            return courseinfo
        return None

    def clean_ec_db(self):
        cursor = self.sql_ec_conn.cursor()
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)

        for course_id, course in self.courses.items():
            ec_tablename = self.ec_table + "_" + course_id
            query = "DROP TABLE IF EXISTS %s" % ec_tablename
            cursor.execute(query)

        self.sql_ec_conn.commit()
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        utils.log(self.ec_db + " has been cleaned.")

    def group_event_by_date(self, course_db_name, event_id, events_date_counts, dates):
        """
        Gets the dates for each students interaction with given event
        :param self:
        :param course_db_name:
        :param event_id:
        :param events_date_counts:
        """
        # print event_id
        # print course_db_name
        # check we have valid dates and assign them - else ignore them
        date_filter = "1 = 1"
        try:
            start_date = dates[0].strftime("%Y-%m-%d")
            end_date = dates[1].strftime("%Y-%m-%d")
            date_filter = "date(`created`) between  '" + start_date + "' AND '" + end_date + "' "
        except ValueError:
            pass

        self.sql_cm_conn = self.connect_to_sql(self.sql_cm_conn, course_db_name, True)
        query = "select date(`created`) time_date from `courseware_studentmodule` where `module_id` like '%" + event_id + "' and " + date_filter + " "
        cursor = self.sql_cm_conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        for item in results:
            if 'time_date' in item:
                item_date = item['time_date']
            else:
                continue

            if item_date not in events_date_counts:
                events_date_counts[item_date] = {}
                events_date_counts[item_date][event_id] = 0
            elif event_id not in events_date_counts[item_date]:
                events_date_counts[item_date][event_id] = 0

            events_date_counts[item_date][event_id] += 1
        pass


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    print path
    required_files = []
    return required_files


def filenametodate(filename):
    """
    Extracts the date from a clickstream filename
    :param filename: The filename to extract
    :return: The date
    """
    date = filename.replace(".log", "").replace(config.CLICKSTREAM_PREFIX, "")
    date = time.strptime(date, "%Y-%m-%d")
    return date


def name():
    """
    Returns the name of the service class
    """
    return "EventCount"


def service():
    """
    Returns an instance of the service
    """
    return Eventcount()
