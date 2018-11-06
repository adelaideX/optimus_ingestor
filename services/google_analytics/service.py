# coding=utf-8
"""
Service for importing the GA utm data from edx
"""
import os
import warnings
import datetime

import MySQLdb

import base_service
import config
import utils


class GoogleAnalytics(base_service.BaseService):
    """
    Generates the required tables to store the campaign and conversion data emailed from edX
    """

    inst = None

    def __init__(self):
        GoogleAnalytics.inst = self
        super(GoogleAnalytics, self).__init__()

        # The pretty name of the service
        self.pretty_name = "Campaigns and Conversions from Google Analytics"
        # Whether the service is enabled
        self.enabled = True
        # Whether to run more than once
        self.loop = True
        # The amount of time to sleep in seconds
        self.sleep_time = 60

        self.sql_ga_conn = None

        # Variables
        self.ga_db = 'Google_Analytics'
        self.map_table = 'coursemap'
        self.conv_table = 'conversions'
        self.camp_table = 'campaigns'

        self.basepath = os.path.dirname(__file__)

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.sql_ga_conn = self.connect_to_sql(self.sql_ga_conn, self.ga_db, True)

        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        ingests = self.get_ingests()
        # check every 60 secs but only execute code if there are files to ingest.
        if ingests:
            # Create coursemap table
            self.create_map_table()

            # Create conversions table
            self.create_conv_table()

            # Create campaigns table
            self.create_camp_table()

            for ingest in ingests:
                if ingest['type'] == 'file':
                    # print "ingesting " + ingest['meta']
                    self.start_ingest(ingest['id'])
                    path = ingest['meta']

                    if 'Campaign' in ingest['meta']:
                        # Ingest the campaigns file
                        self.ingest_csv_file(path, self.camp_table)
                    elif 'Conversions' in ingest['meta']:
                        # Ingest the conversions file
                        self.ingest_csv_file(path, self.conv_table)
                    else:
                        utils.log("GoogleAnalytics - Campaign or Conversions not found in file path")
                    # update the ingest record
                    self.finish_ingest(ingest['id'])

                # identify any new campaigns and add the key to the coursemap table
                print("GoogleAnalytics - updating map table")
                self.update_map_table()
                # save_run to ingest api
                # self.save_run_ingest()
            utils.log("GoogleAnalytics completed")
        pass

    def update_map_table(self):
        """
        Updates the course map table with any ad_content data that is in each table but not in the coursemap table
        :return:
        """
        # get the values to be inserted in map table
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        cursor = self.sql_ga_conn.cursor()
        query = "SELECT DISTINCT sub.ad_content FROM (SELECT ad_content FROM " + self.camp_table + " UNION SELECT ad_content FROM " + self.conv_table + " ) sub WHERE  sub.ad_content NOT IN (SELECT cm.ad_content FROM " + self.map_table + " cm)"
        insert = "INSERT INTO " + self.map_table + " (ad_content) " + query
        # check if there are any records to update
        rows_count = cursor.execute(query)
        if rows_count > 0:
            cur = self.sql_ga_conn.cursor()
            # print insert
            # update the map table
            cur.execute(insert)
            self.sql_ga_conn.commit()
            cur.close()
        cursor.close()
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
        conv_fields = '(date,campaign,source,ad_content,enrollment_serverside,transactions,revenue)'
        camp_fields = '(date,campaign,source,ad_content,sessions,new_users,new_sessions,bounce_rate,pages_per_session,avg_session_duration)'
        fields = ''
        if tablename == 'campaigns':
            fields = camp_fields
        elif tablename == 'conversions':
            fields = conv_fields

        query = "LOAD DATA LOCAL INFILE '" + ingest_file_path + "' INTO TABLE " + tablename + " CHARACTER SET UTF8 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 7 LINES " + fields

        cursor = self.sql_ga_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()
        self.sql_ga_conn.commit()
        print "Ingested " + ingest_file_path + " into " + tablename

        pass

    def create_map_table(self):
        """
        Create the coursemap table
        """
        columns = [
            {"col_name": "ad_content", "col_type": "varchar(255)"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.map_table
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " PRIMARY KEY (`ad_content`), KEY idx_ad_course (course_id)) DEFAULT CHARSET=utf8;"
        try:
            cursor = self.sql_ga_conn.cursor()
            cursor.execute(query)
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError), e:
            utils.log("Connection FAILED: %s" % (repr(e)))
            self.sql_ga_conn = self.connect_to_sql(self.sql_ga_conn, self.ga_db, True)
            cursor = self.sql_ga_conn.cursor()
            cursor.execute(query)
            utils.log("Reset connection and executed query")
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()

        pass


    def create_conv_table(self):
        """
        Create the conversions table
        """
        columns = [
            {"col_name": "id", "col_type": "int(11) unsigned NOT NULL AUTO_INCREMENT"},
            {"col_name": "date", "col_type": "date"},
            {"col_name": "campaign", "col_type": "varchar(255)"},
            {"col_name": "source", "col_type": "varchar(255)"},
            {"col_name": "ad_content", "col_type": "varchar(255)"},
            {"col_name": "enrollment_serverside", "col_type": "varchar(255)"},
            {"col_name": "transactions", "col_type": "varchar(255)"},
            {"col_name": "revenue", "col_type": "varchar(255)"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.conv_table
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " PRIMARY KEY (`id`), KEY idx_ad (ad_content)) DEFAULT CHARSET=utf8;"
        try:
            cursor = self.sql_ga_conn.cursor()
            cursor.execute(query)
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError), e:
            utils.log("Connection FAILED: %s" % (repr(e)))
            self.sql_ga_conn = self.connect_to_sql(self.sql_ga_conn, self.ga_db, True)
            cursor = self.sql_ga_conn.cursor()
            cursor.execute(query)
            utils.log("Reset connection and executed query")
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()

        pass


    def create_camp_table(self):
        """
        Create the campaign table
        """

        columns = [
            {"col_name": "id", "col_type": "int(11) unsigned NOT NULL AUTO_INCREMENT"},
            {"col_name": "date", "col_type": "date"},
            {"col_name": "campaign", "col_type": "varchar(255)"},
            {"col_name": "source", "col_type": "varchar(255)"},
            {"col_name": "ad_content", "col_type": "varchar(255)"},
            {"col_name": "sessions", "col_type": "int(11)"},
            {"col_name": "new_users", "col_type": "int(11)"},
            {"col_name": "new_sessions", "col_type": "varchar(255)"},
            {"col_name": "bounce_rate", "col_type": "varchar(255)"},
            {"col_name": "pages_per_session", "col_type": "double"},
            {"col_name": "avg_session_duration", "col_type": "varchar(255)"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + self.camp_table
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " PRIMARY KEY (`id`), KEY idx_ad (ad_content)) DEFAULT CHARSET=utf8;"
        try:
            cursor = self.sql_ga_conn.cursor()
            cursor.execute(query)
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError), e:
            utils.log("Connection FAILED: %s" % (repr(e)))
            self.sql_ga_conn = self.connect_to_sql(self.sql_ga_conn, self.ga_db, True)
            cursor = self.sql_ga_conn.cursor()
            cursor.execute(query)
            utils.log("Reset connection and executed query")
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        cursor.close()

        pass


    def find_last_run_ingest_type(self, service_name):
        """
        Finds the date of the last time the service ran
        :param service_name: The name of the service to find
        :return: The date of the last run
        """
        self.setup_ingest_api()
        cur = self.api_db.cursor()
        query = "SELECT * FROM ingestor WHERE service_name = '" + service_name + "' AND type = 'save_run' " \
                                                                                 "AND started = 1 AND completed = 1 ORDER BY created DESC LIMIT 1;"
        cur.execute(query)
        date = datetime.datetime.fromtimestamp(0)
        for row in cur.fetchall():
            date = row[6]
        cur.close()
        return date


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    # print path
    required_files = []

    main_path = os.path.realpath(os.path.join(path, 'ga'))

    # patch main_path to use child directory as we can't use symlink
    if not config.SYMLINK_ENABLED:
        main_path = utils.get_subdir(main_path)

    for filename in os.listdir(main_path):
        if '.csv' in filename:
            required_files.append(os.path.join(main_path, filename))
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "GoogleAnalytics"


def service():
    """
    Returns an instance of the service
    """
    return GoogleAnalytics()
