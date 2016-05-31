"""
Service for importing the email extract from edx
"""
import csv
import json
import time
import urllib2

import config
import os

import MySQLdb
import warnings

import base_service
import utils


class EmailCRM(base_service.BaseService):
    """
    Generates the required tables to store the email extract for use with CRM
    """

    inst = None

    def __init__(self):
        EmailCRM.inst = self
        super(EmailCRM, self).__init__()

        # The pretty name of the service
        self.pretty_name = "Email Extract For CRM"
        # Whether the service is enabled
        self.enabled = True
        # Whether to run more than once
        self.loop = True
        # The amount of time to sleep in seconds
        self.sleep_time = 60

        self.sql_db = None
        self.sql_ec_conn = None

        self.mongo_db = None
        self.mongo_dbname = ""

        # Variables
        self.ec_db = 'Email_CRM'
        self.ec_table = 'emailcrm'
        self.cc_table = 'iso_3166_2_countries'
        self.basepath = os.path.dirname(__file__)
        self.courses = {}

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.sql_ec_conn = self.connect_to_sql(self.sql_ec_conn, self.ec_db, True)
        self.courses = self.get_all_courses()

        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("EmailCRM")
        last_personcourse = self.find_last_run_ingest("PersonCourse")
        if self.find_last_run_ingest("PersonCourse") and last_run < last_personcourse:

            # Create 'ec_table'
            self.create_ec_table()
            # manage country code table
            self.country_code_import()

            ingests = self.get_ingests()
            for ingest in ingests:
                if ingest['type'] == 'file':
                    print "we have an email file not started or completed! " + ingest['meta']

                    self.start_ingest(ingest['id'])
                    path = ingest['meta']

                    # purge the table
                    self.truncate_ec_table()

                    # Ingest the email file
                    self.ingest_csv_file(path, self.ec_table)

                    # export the file
                    self.datadump2csv()

            self.save_run_ingest()
            utils.log("EmailCRM completed")

        pass

    def country_code_import(self):
        """
        Checks to see if the table exists, if not then create the table and import data.
        :return:
        """
        # check to see if table exists if it does then ignore - if not then create and populate with data from file
        query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}' ".format(self.cc_table)
        cursor = self.sql_ec_conn.cursor()
        cursor.execute(query)
        if cursor.fetchone()[0] == 0:
            cursor.close()
            # create the table
            self.create_cc_table()
            iso_countries_file = self.basepath + '/resources/iso_3166_2_countries.csv'
            if os.path.isfile(iso_countries_file):
                self.ingest_csv_file(iso_countries_file, self.cc_table)
        pass

    def ingest_csv_file(self, ingest_file_path, tablename):
        """
        Ingests a csv file of the type defined, may not work for all separated text files
        :param ingest_file_path:
        :param tablename:
        :return:
        """
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "LOAD DATA LOCAL INFILE '" + ingest_file_path + "' INTO TABLE " + tablename + " " \
                "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\' LINES TERMINATED BY '\\n'  IGNORE 1 LINES"
        cursor = self.sql_ec_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        self.sql_ec_conn.commit()
        print "Ingested " + ingest_file_path + "into " + tablename
        pass

    def truncate_ec_table(self):
        """
        Truncate the email table
        """
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "TRUNCATE " + self.ec_table
        cursor = self.sql_ec_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        self.sql_ec_conn.commit()
        cursor.close()

        print "Truncating " + self.ec_table
        pass

    def create_ec_table(self):
        """
        Create the course profile table
        """
        columns = [
            {"col_name": "email", "col_type": "varchar(255)"},
            {"col_name": "full_name", "col_type": "varchar(255)"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "is_opted_in_for_email", "col_type": "varchar(255)"},
            {"col_name": "preference_set_datetime", "col_type": "date"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.ec_table
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " KEY idx_email (`email`, `course_id`)) DEFAULT CHARSET=utf8;"

        cursor = self.sql_ec_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        pass

    def create_cc_table(self):
        """
        Create the country code table
        """
        columns = [
            {"col_name": "Sort Order", "col_type": "int(11)"},
            {"col_name": "Common Name", "col_type": "varchar(255)"},
            {"col_name": "Formal Name", "col_type": "varchar(255)"},
            {"col_name": "Type", "col_type": "varchar(255)"},
            {"col_name": "Sub Type", "col_type": "varchar(255)"},
            {"col_name": "Sovereignty", "col_type": "varchar(255)"},
            {"col_name": "Capital", "col_type": "varchar(255)"},
            {"col_name": "ISO 4217 Currency Code", "col_type": "varchar(255)"},
            {"col_name": "ISO 4217 Currency Name", "col_type": "varchar(255)"},
            {"col_name": "ITU-T Telephone Code", "col_type": "varchar(255)"},
            {"col_name": "ISO 3166-1 2 Letter Code", "col_type": "varchar(255)"},
            {"col_name": "ISO 3166-1 3 Letter Code", "col_type": "varchar(255)"},
            {"col_name": "ISO 3166-1 Number", "col_type": "int(11)"},
            {"col_name": "IANA Country Code TLD", "col_type": "varchar(255)"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.cc_table
        query += "("
        for column in columns:
            query += "`" + column['col_name'] + "`" + " " + column['col_type'] + ', '
        query += " KEY `idx_2_letter_code` (`ISO 3166-1 2 Letter Code`)) DEFAULT CHARSET=utf8;"

        cursor = self.sql_ec_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        pass

    def get_ingests(self):
        """
        Retrieves the relevant ingests for the service
        """
        self.setup_ingest_api()
        cur = self.api_db.cursor()
        query = "SELECT * FROM ingestor" \
                " WHERE service_name = '" + str(self.__class__.__name__) + "' " \
                " AND completed = 0 ORDER BY created ASC;"
        cur.execute(query)
        ingests = []
        for row in cur.fetchall():
            ingest = {
                'id': row[0],
                'type': row[2],
                'meta': row[3]
            }
            ingests.append(ingest)
        cur.close()

        return ingests

    def start_ingest(self, ingest_id):
        """
        Starts an ingestion entry
        :param ingest_id: the ID of the ingestion entry
        """
        cur = self.api_db.cursor()
        current_date = time.strftime('%Y-%m-%d %H:%M:%S')
        query = "UPDATE ingestor " \
                " SET started=1, completed=1, started_date='" + current_date + "' WHERE id=" + str(ingest_id) + ";"
        cur.execute(query)
        self.api_db.commit()
        cur.close()

        pass

    def datadump2csv(self):
        """
        Generates a CSV file for CRM
        """
        e_tablename = self.ec_table

        print "Exporting CSV: " + e_tablename
        if self.sql_ec_conn is None:
            self.sql_ec_conn = self.connect_to_sql(self.sql_ec_conn, "Email_CRM", True)

        backup_path = config.EXPORT_PATH
        current_time = time.strftime('%m%d%Y-%H%M%S')

        # loop through courses -
        # write first file with headers then
        # each subsequent iteration append to file

        backup_prefix = e_tablename + "_" + current_time
        backup_file = os.path.join(backup_path, backup_prefix + ".csv")

        for i, course in enumerate(self.courses.items()):
            try:
                course_id = course[0]
                mongoname = course[1]['mongoname']
                dbname = course[1]['dbname']

                # Get nice course name from course info
                json_file = dbname.replace("_", "-") + '.json'
                courseinfo = self.loadcourseinfo(json_file)
                if courseinfo is None:
                    utils.log("Can not find course info for ." + str(course_id))
                    continue
                nice_name = courseinfo['display_name']

                query = "SELECT up.user_id AS user_id, au.is_staff AS is_staff, " \
                        "au.is_active AS is_active, au.last_login AS last_login, e.email AS email, " \
                        "substring_index(substring_index(e.full_name, ' ', 1), ' ', -( 1 )) AS first_name, " \
                        "trim(substr(e.full_name, locate(' ', e.full_name))) AS last_name, " \
                        "" + "'" + course_id + "'" + " AS course_id, " \
                        "" + "'" + nice_name + "'" + " AS course_name, " \
                        "e.is_opted_in_for_email AS is_opted_in_for_email, " \
                        "up.gender AS gender, up.year_of_birth AS year_of_birth, " \
                        "up.level_of_education AS level_of_education, " \
                        "( CASE up.level_of_education " \
                        "WHEN 'p' THEN 'Doctorate' " \
                        "WHEN 'a' THEN 'Associate degree' " \
                        "WHEN 'b' THEN 'Bachelors degree' " \
                        "WHEN 'm' THEN 'Masters or professional degree' " \
                        "WHEN 'hs' THEN 'Secondary/high school' " \
                        "WHEN 'jhs' THEN 'Junior secondary/junior high/middle school' " \
                        "WHEN 'el' THEN 'Elementary/primary school' " \
                        "WHEN 'none' THEN 'No Formal Education' " \
                        "WHEN 'other' THEN 'Other Education' " \
                        "WHEN '' THEN 'User did not specify level of education' " \
                        "WHEN 'p_se' THEN 'Doctorate in science or engineering (no longer used)' " \
                        "WHEN 'p_oth' THEN 'Doctorate in another field (no longer used)' " \
                        "ELSE 'User did not specify level of education' END ) AS levelofEd, " \
                        "up.country AS country, " \
                        "( CASE c.`Common Name` " \
                        "WHEN 'British Sovereign Base Areas' THEN '' " \
                        "ELSE c.`Common Name` END )AS country_name " \
                        "FROM {0}.auth_user au " \
                        "JOIN emailcrm e ON au.email = e.email " \
                        "JOIN {0}.auth_userprofile up ON au.id = up.user_id " \
                        "LEFT JOIN iso_3166_2_countries c ON up.country = c.`ISO 3166-1 2 Letter Code` " \
                        "AND c.Type = 'Independent State' " \
                        "WHERE e.course_id = '{1}' ".format(dbname.lower(), mongoname)

                ec_cursor = self.sql_ec_conn.cursor()
                ec_cursor.execute(query)
                result = ec_cursor.fetchall()
                ec_cursor.close()

                if i == 0:
                    with open(backup_file, "w") as csv_file:
                        csv_writer = csv.writer(csv_file, dialect='excel')
                        csv_writer.writerow([i[0] for i in ec_cursor.description])  # write headers
                        for record in result:
                            csv_writer.writerow(record)
                else:
                    with open(backup_file, "a") as csv_file:
                        csv_writer = csv.writer(csv_file, dialect='excel')
                        for record in result:
                            csv_writer.writerow(record)

            except self.sql_ec_conn.ProgrammingError:
                pass

        utils.log("The personcourse table: %s exported to csv file %s" % (e_tablename, backup_file))

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


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    print path
    required_files = []

    main_path = os.path.realpath(os.path.join(path, 'database_state', 'latest'))

    # patch main_path to use child directory as we can't use symlink
    if not config.SYMLINK_ENABLED:
        main_path = utils.get_subdir(main_path)

    for filename in os.listdir(main_path):
        if filename == config.DBSTATE_PREFIX.lower() + "email_opt_in-prod-analytics.csv":
            required_files.append(os.path.join(main_path, filename))
            break
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "EmailCRM"


def service():
    """
    Returns an instance of the service
    """
    return EmailCRM()