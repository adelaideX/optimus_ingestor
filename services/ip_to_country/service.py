"""
Service for attaching countries to mongo entries with IP addresses
"""
import base_service
import utils
import os
import geoip2
from geoip2 import database
from geoip2.errors import *
from bson import ObjectId
from pymongo import *

basepath = os.path.dirname(__file__)


class IPToCountry(base_service.BaseService):
    """
    Finds entries in Mongo which have an IP and adds a country to them
    """

    inst = None

    def __init__(self):
        IPToCountry.inst = self
        super(IPToCountry, self).__init__()

        # The pretty name of the service
        self.pretty_name = "IP To Country"
        # Whether the service is enabled
        self.enabled = True
        # Whether to run more than once
        self.loop = True
        # The amount of time to sleep in seconds
        self.sleep_time = 60

        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = 'logs'
        self.ipfield = 'ip'

        self.geo_reader = None
        self.city_reader = None
        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        # self.geo_reader = geoip2.database.Reader(basepath+'/lib/GeoIP2-Country.mmdb')
        self.geo_reader = geoip2.database.Reader(basepath + '/lib/GeoLite2-Country.mmdb')
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("IpToCountry")
        last_clickstream = self.find_last_run_ingest("Clickstream")
        if self.finished_ingestion("Clickstream") and last_run < last_clickstream:

            if self.mongo_client is None:
                self.mongo_client = MongoClient('localhost', 27017)
                if self.mongo_db is None:
                    self.mongo_db = self.mongo_client[self.mongo_dbname]

            if self.mongo_db:
                for collection in self.mongo_db.collection_names():
                    mongo_collection = self.mongo_db[collection]
                    if mongo_collection.name == 'clickstream':
                        # utils.log("CHECKING COUNTRY")
                        # to speed up - add an geo_attempt flag so we don't check errors or bad ips again
                        toupdates = mongo_collection.find(
                            {self.ipfield: {'$exists': True}, 'country': {'$exists': False},
                             'geo_attempt': {'$exists': False}})
                        utils.log("CHECKING COUNTRY")
                        i = 0
                        total = toupdates.count()
                        # for toupdate in toupdates:
                        #    total += 1
                        for toupdate in toupdates:
                            if toupdate[self.ipfield] != '::1' and toupdate[self.ipfield] != '':
                                try:
                                    country = self.geo_reader.country(toupdate[self.ipfield])
                                    isocountry = country.country.iso_code
                                    isosubdiv = None
                                    if isocountry == 'AU':
                                        if self.city_reader is None:
                                            self.city_reader = geoip2.database.Reader(
                                                basepath + '/lib/GeoLite2-City.mmdb')
                                        city = self.city_reader.city(toupdate[self.ipfield])
                                        isosubdiv = city.subdivisions.most_specific.iso_code
                                        cityname = city.city.name
                                    if isosubdiv is not None:
                                        mongo_collection.update({"_id": toupdate['_id']}, {
                                            "$set": {"country": isocountry, "subdivision": isosubdiv,
                                                     "city": cityname}})
                                    else:
                                        mongo_collection.update({"_id": toupdate['_id']},
                                                                {"$set": {"country": isocountry}})
                                    print "*** ADDING ADDRESS " + str(i) + " / " + str(total)
                                except AddressNotFoundError:
                                    # utils.log("Could not find address for " + str(toupdate))
                                    mongo_collection.update({"_id": toupdate['_id']},
                                                            {"$set": {"geo_attempt": 1}})
                                    pass
                            else:
                                mongo_collection.update({"_id": ObjectId(toupdate['_id'])},
                                                        {"$set": {"country": "", "geo_attempt": 1}})
                            i += 1
                        utils.log("FINISHED COUNTRY")
                        self.save_run_ingest()

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


def name():
    """
    Returns the name of the service class
    """
    return "IPToCountry"


def service():
    """
    Returns an instance of the service
    """
    return IPToCountry()
