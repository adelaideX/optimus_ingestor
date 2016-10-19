# coding=utf-8
"""
Service for importing the email extract from edx
"""
import glob
import json
import os
import time
import urllib2
import warnings
from datetime import datetime

import MySQLdb
import unicodecsv as csv

import base_service
import config
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
        self.sql_ecrm_conn = None

        self.mongo_db = None
        self.mongo_dbname = ""

        # Variables
        self.ecrm_db = 'Email_CRM'
        self.ecrm_table = 'emailcrm'
        self.cn_table = 'countries_io'
        self.cn_url = 'http://country.io/names.json'

        self.le_table = 'lastexport'

        self.basepath = os.path.dirname(__file__)
        self.courses = {}

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.sql_ecrm_conn = self.connect_to_sql(self.sql_ecrm_conn, self.ecrm_db, True)
        self.courses = self.get_all_courses()

        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("EmailCRM")
        last_personcourse = self.find_last_run_ingest("PersonCourse")
        last_dbstate = self.find_last_run_ingest("DatabaseState")

        if self.finished_ingestion("PersonCourse") and last_run < last_personcourse and \
                self.finished_ingestion("DatabaseState") and \
                        last_run < last_dbstate:

            # Create country name table and import data (if required)
            self.create_load_cn_table()

            # Create 'last export table'
            self.create_le_table()

            ingests = self.get_ingests()
            for ingest in ingests:
                if ingest['type'] == 'file':
                    # print "ingesting " + ingest['meta']

                    self.start_ingest(ingest['id'])
                    path = ingest['meta']

                    # purge the table
                    self.truncate_ecrm_table()

                    # Ingest the email file
                    self.ingest_csv_file(path, self.ecrm_table)

                    # Load last export file so we can use it for delta
                    self.load_last_export()

                    # export the file
                    self.datadump2csv()

                    # update the ingest record
                    self.finish_ingest(ingest['id'])

                    self.save_run_ingest()
                    utils.log("EmailCRM completed")
        pass

    def load_last_export(self):
        """
        truncate the table then load the last export file.
        """
        backup_path = config.EXPORT_PATH

        file_list = glob.glob(os.path.join(backup_path, self.ecrm_table + "*.csv"))
        file_list.sort(reverse=True)
        last_file = file_list[0]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "SELECT 1 FROM %s WHERE extract_file = '%s' " % (self.le_table, last_file)
        cursor = self.sql_ecrm_conn.cursor()

        if not cursor.execute(query) and os.path.isfile(last_file):
            self.ingest_csv_file(last_file, self.le_table)
        cursor.close()

        self.sql_ecrm_conn.commit()

        # update the extract_date timestamp with now.
        query = "UPDATE %s SET extract_date = '%s', extract_file = '%s' WHERE extract_date is NULL" % (
            self.le_table, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), last_file)
        cursor = self.sql_ecrm_conn.cursor()
        cursor.execute(query)
        cursor.close()
        self.sql_ecrm_conn.commit()
        warnings.filterwarnings('always', category=MySQLdb.Warning)

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
                                                                                              "CHARACTER SET UTF8 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\' LINES TERMINATED BY '\\r\\n'  IGNORE 1 LINES"

        cursor = self.sql_ecrm_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        self.sql_ecrm_conn.commit()
        print "Ingested " + ingest_file_path + " into " + tablename
        pass

    def truncate_ecrm_table(self):
        """
        Truncate the email table
        """
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "TRUNCATE " + self.ecrm_table
        cursor = self.sql_ecrm_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        self.sql_ecrm_conn.commit()
        cursor.close()

        print "Truncating " + self.ecrm_table
        pass

    def create_ecrm_table(self):
        """
        Create the emailcrm table
        """
        columns = [
            {"col_name": "email", "col_type": "varchar(255)"},
            {"col_name": "full_name", "col_type": "varchar(255)"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "is_opted_in_for_email", "col_type": "varchar(255)"},
            {"col_name": "preference_set_datetime", "col_type": "date"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.ecrm_table
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " KEY idx_email_course (`email`, `course_id`)) DEFAULT CHARSET=utf8;"
        try:
            cursor = self.sql_ecrm_conn.cursor()
            cursor.execute(query)
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError), e:
            utils.log("Connection FAILED: %s" % (repr(e)))
            self.sql_ecrm_conn = self.connect_to_sql(self.sql_ecrm_conn, self.ecrm_db, True)
            cursor = self.sql_ecrm_conn.cursor()
            cursor.execute(query)
            utils.log("Reset connection and executed query")
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        pass

    def create_le_table(self):
        """
        Create the lastexport table
        """

        columns = [
            {"col_name": "user_id", "col_type": "int(11)"},
            {"col_name": "is_staff", "col_type": "varchar(255)"},
            {"col_name": "is_active", "col_type": "int(11)"},
            {"col_name": "email", "col_type": "varchar(255)"},
            {"col_name": "viewed", "col_type": "int(11)"},
            {"col_name": "explored", "col_type": "int(11)"},
            {"col_name": "certified", "col_type": "int(11)"},
            {"col_name": "mode", "col_type": "varchar(255)"},
            {"col_name": "first_name", "col_type": "varchar(255)"},
            {"col_name": "last_name", "col_type": "varchar(255)"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "course_name", "col_type": "varchar(255)"},
            {"col_name": "course_start_date", "col_type": "varchar(255)"},
            {"col_name": "enrolled_date", "col_type": "varchar(255)"},
            {"col_name": "is_opted_in_for_email", "col_type": "varchar(255)"},
            {"col_name": "gender", "col_type": "varchar(255)"},
            {"col_name": "year_of_birth", "col_type": "varchar(255)"},
            {"col_name": "level_of_education", "col_type": "varchar(255)"},
            {"col_name": "levelofEd", "col_type": "varchar(255)"},
            {"col_name": "country_name", "col_type": "varchar(255)"},
            {"col_name": "extract_date", "col_type": "datetime"},
            {"col_name": "extract_file", "col_type": "varchar(255)"},

        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.le_table
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " KEY `idx_le` (`user_id`,`viewed`,`explored`,`certified`,`is_opted_in_for_email`)) DEFAULT CHARSET=utf8;"
        try:
            cursor = self.sql_ecrm_conn.cursor()
            cursor.execute(query)
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError), e:
            utils.log("Connection FAILED: %s" % (repr(e)))
            self.sql_ecrm_conn = self.connect_to_sql(self.sql_ecrm_conn, self.ecrm_db, True)
            cursor = self.sql_ecrm_conn.cursor()
            cursor.execute(query)
            utils.log("Reset connection and executed query")
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        pass

    def create_load_cn_table(self):
        """
        Create the country name table
        """
        columns = [
            {"col_name": "country_code", "col_type": "varchar(255)"},
            {"col_name": "country_name", "col_type": "varchar(255)"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.cn_table
        query += "("
        for column in columns:
            query += "`" + column['col_name'] + "`" + " " + column['col_type'] + ', '
        query += " KEY `idx_2_letter_code` (`country_code`)) CHARSET=utf8;"

        cursor = self.sql_ecrm_conn.cursor()
        cursor.execute(query)
        cursor.close()
        query = "SELECT 1 FROM " + self.cn_table
        cursor = self.sql_ecrm_conn.cursor()
        if not cursor.execute(query):
            # import data
            try:
                countrydatafile = urllib2.urlopen(self.cn_url)
                if countrydatafile:
                    countrydata = json.load(countrydatafile)

                    sql = '''INSERT INTO ''' + self.cn_table + ''' VALUES(%s, %s)'''

                    sql_values_list = list()
                    for key, value in countrydata.iteritems():
                        sql_values_list.append((key, value))

                    cursor = self.sql_ecrm_conn.cursor()
                    cursor.executemany(sql, sql_values_list)
                    cursor.close()
            except Exception, e:
                print repr(e)
                utils.log("Country import failed: %s" % (repr(e)))
        cursor.close()
        warnings.filterwarnings('always', category=MySQLdb.Warning)
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

    def datadump2csv(self):
        """
        Generates a CSV file for CRM
        """
        e_tablename = self.ecrm_table

        print "Exporting CSV: " + e_tablename

        backup_path = config.EXPORT_PATH
        current_time = time.strftime('%m%d%Y-%H%M%S')

        # loop through courses -
        # write first file with headers then
        # each subsequent iteration append to file

        backup_prefix = e_tablename + "_" + current_time
        backup_file = os.path.join(backup_path, backup_prefix + ".csv")

        for idx, course in enumerate(self.courses.items()):
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
                start = courseinfo['start'].split('T')
                start_date = datetime.strptime(start[0].replace('"', ''), "%Y-%m-%d")
                start_date = start_date.strftime("%d/%m/%Y")

                # au.last_login,

                query = "SELECT up.user_id, " \
                        "CASE au.is_staff " \
                        "WHEN 1 THEN 'Yes' ELSE 'No' END AS is_staff, " \
                        "au.is_active, TRIM(TRAILING '.' FROM e.email ) AS email, " \
                        "pc.viewed, pc.explored, pc.certified, pc.mode, " \
                        "TRIM(TRAILING '\\\\' FROM REPLACE(REPLACE(substring_index(e.full_name, ' ', 1),'�', ''), ',', '')) AS first_name, " \
                        "TRIM(TRAILING '\\\\' FROM SUBSTR(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(SUBSTR(e.full_name, LOCATE(' ', e.full_name)), '�', ''), ',', ''), '|', ''), CONVERT(CHAR(127) USING utf8), '')), 1, 30)) AS last_name, " \
                        "'{2}' AS course_id, " \
                        "'{3}' AS course_name, " \
                        "'{5}' AS course_start_date, " \
                        "DATE_FORMAT(pc.start_time,'%d/%m/%Y') as enrolled, " \
                        "CASE WHEN DATE_FORMAT(NOW(), '%Y') - up.year_of_birth < 15 THEN 'No' ELSE CASE WHEN e.is_opted_in_for_email = 'True' THEN 'Yes' ELSE 'No' END END AS is_opted_in_for_email, " \
                        "CASE up.gender WHEN 'm' THEN 'Male'  WHEN 'f' THEN 'Female' WHEN 'o' THEN 'Other' ELSE NULL END as gender, " \
                        "CASE WHEN up.year_of_birth <= 1900 THEN NULL " \
                        "ELSE up.year_of_birth END AS year_of_birth ," \
                        "up.level_of_education, " \
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
                        "c.country_name " \
                        "FROM {0}.auth_user au " \
                        "JOIN {4}.emailcrm e ON au.email = e.email " \
                        "JOIN Person_Course.personcourse_{2} pc ON au.id = pc.user_id " \
                        "JOIN {0}.auth_userprofile up ON au.id = up.user_id " \
                        "LEFT JOIN {4}.countries_io c ON up.country = c.country_code " \
                        "LEFT JOIN email_crm.lastexport le " \
                        "ON le.user_id = up.user_id " \
                        "AND le.viewed = pc.viewed  " \
                        "AND le.explored = pc.explored " \
                        "AND le.certified = pc.certified " \
                        "AND le.course_id = '{2}' " \
                        "WHERE e.course_id = '{1}' " \
                        "AND le.user_id is null " \
                        "AND le.viewed is null " \
                        "AND le.explored is null " \
                        "AND le.certified is null " \
                        "AND le.course_id is null ".format(dbname, mongoname, course_id, nice_name, self.ecrm_db,
                                                           start_date)

                ec_cursor = self.sql_ecrm_conn.cursor()
                ec_cursor.execute(query)
                result = ec_cursor.fetchall()
                ec_cursor.close()

                if idx == 0:
                    with open(backup_file, "wb") as csv_file:
                        csv_writer = csv.writer(csv_file, dialect='excel', encoding='utf-8')
                        csv_writer.writerow([i[0] for i in ec_cursor.description])  # write headers
                        for row in result:
                            csv_writer.writerow(row)
                else:
                    with open(backup_file, "ab") as csv_file:
                        csv_writer = csv.writer(csv_file, dialect='excel', encoding='utf-8')
                        for row in result:
                            csv_writer.writerow(row)
                utils.log("EmailCRM select written to file: %s" % course_id)
            except Exception, e:
                print repr(e)
                utils.log("EmailCRM FAILED: %s" % (repr(e)))
                break

        utils.log("The EmailCRM data: %s exported to csv file %s" % (e_tablename, backup_file))

    def loadcourseinfo(self, json_file):
        """
        Loads the course information from JSON course structure file
        :param json_file: the name of the course structure file
        :return the course information
        """
        # print self
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
            break  # only one email file, once found exit the search
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
