# -*- coding: utf-8 -*-
"""
Python wrapper for the Criteo API


"""
from suds.client import Client as soapclient
import xml.etree.ElementTree as etree

import sys
import time
import unicodecsv as csv

if sys.version_info[0] == 3:
    from urllib.request import urlopen
else:
    # Not Python 3 - today, it is most likely to be Python 2
    # But note that this might need an update when Python 4
    # might be around one day
    from urllib import urlopen


def _assign(selector, entity):
    for key, value in selector.items():
        attr = getattr(entity, key)
        if isinstance(value, dict):
            _assign(value, attr)
        else:
            if hasattr(attr, 'int'):
                attr.int = value
            else:
                setattr(entity, key, value)
    return entity


class Client(object):
    """
    SUPPORTED METHODS
    ~~~~~~~~~~~~~~~~~
        . clientLogin
        . getAccount
        . getBudgets
        . getCampaigns
        . getCatalogsNames
        . getCategories
        . getJobStatus
        . getReportDownloadUrl
        . getStatisticsLastUpdate
        . partnerLogin
        . scheduleReportJob
    UNSUPPORTED METHODS
    ~~~~~~~~~~~~~~~~~
        . mutateCampaigns(ArrayOfCampaignMutate listOfCampaignMutates, )
        . mutateCategories(ArrayOfCategoryMutate listofCategoryMutates, )
    """

    def __init__(self, username, password, token, client_version=None,
                 loglevel='INFO'):
        """
        Args:
            username:
            password:
            token:
        KwArgs:
            client_version:
            loglevel:
        """
        self.__client = soapclient(
        'https://advertising.criteo.com/API/v201305/AdvertiserService.asmx?WSDL',
        )
        self.username = username
        headers = self._make_type('apiHeader')
        headers.authToken = self.__client.service.clientLogin(
            username, password, client_version
        )
        headers.appToken = token
        headers.clientVersion = client_version
        self.__client.set_options(soapheaders=headers)
        self.logging(loglevel)

    def clientLogin(self, username, password, source):
        """
        SOAP Method:
            clientLogin(xs:string username,
                        xs:string password,
                        xs:string source)
        """
        return self.__client.service.clientLogin(
            username, password, source
        )

    def getAccount(self):
        """
        SOAP Method:
            getAccount()
        """
        return self.__client.service.getAccount()

    def getBudgets(self, budgetSelector):
        """
        SOAP Method:
            getBudgets(BudgetSelectors budgetSelector)
        Args:
            budgetSelector: {
                budgetIDs = (ArrayOfInt)
            }
        """
        if not isinstance(budgetSelector, dict):
            raise TypeError('budgetSelector must be a dictionary')
        selector = self._make_type('BudgetSelectors')
        return self.__client.service.getBudgets(
            _assign(budgetSelector, selector)
        )

    def getCampaigns(self, campaignSelector):
        """
        SOAP Method:
            getCampaigns(CampaignSelectors campaignSelector, )
        Args:
            campaignSelector: {
                campaignStatus: (ArrayOfCampaignStatus),
                biddingStrategy: (ArrayOfBiddingStrategy),
                budgetIDs: (ArrayOfInt),
                campaignIDs: (ArrayOfInt)
            }
        """
        if not isinstance(campaignSelector, dict):
            raise TypeError('campaignSelector must be a dictionary')
        selector = self._make_type('CampaignSelectors')
        return self.__client.service.getCampaigns(
            _assign(campaignSelector, selector)
        )

    def getCatalogsNames(self):
        """
        SOAP Method:
            getCatalogsNames()
        """
        return self.__client.service.getCatalogsNames()

    def getCategories(self, categorySelector):
        """
        SOAP Method:
            getCategories(CategorySelectors categorySelector, )
        Args:
            categorySelector: {
                categoryIDs = (ArrayOfInt),
                selected = None
            }
        """
        if not isinstance(categorySelector, dict):
            raise TypeError('categorySelector must be a dictionary')
        selector = self._make_type('CategorySelectors')
        return self.__client.service.getCategories(
            _assign(categorySelector, selector)
        )

    def getJobStatus(self, job_id, wait=0.5):
        """
        SOAP Method:
            getJobStatus(xs:long jobID, )
        Args:
            job_id: jobID
        KwArgs:
            wait: time to wait for the response, in seconds
        """
        time.sleep(wait)
        return self.__client.service.getJobStatus(job_id)

    def getReportDownloadUrl(self, jobID):
        """
        SOAP Method:
            getReportDownloadUrl(xs:long jobID, )
        """
        return self.__client.service.getReportDownloadUrl(jobID)

    def getStatisticsLastUpdate(self):
        """
        SOAP Method:
            getStatisticsLastUpdate()
        """
        return self.__client.service.getAccount()

    def mutateCampaigns(self, CampaignMutates):
        """
        SOAP Method:
            mutateCampaigns(ArrayOfCampaignMutate CampaignMutates)
        """
        raise NotImplementedError('API service not supported at the moment')

    def mutateCategories(self):
        """
        SOAP Method:
            mutateCategories(ArrayOfCategoryMutate listofCategoryMutates, )
        """
        raise NotImplementedError('API service not supported at the moment')

    def partnerLogin(self, username, password, source):
        """
        SOAP Method:
            partnerLogin(xs:long appToken,
                         xs:string username,
                         xs:string password,
                         xs:string source)
        """
        return self.__client.service.partnerLogin(
            username, password, source
        )

    def scheduleReportJob(self, reportJob):
        """
        SOAP Method:
            scheduleReportJob(ReportJob reportJob, )
        Args:
            reportJob: {
                selectedColumns = (ArrayOfReportColumn)
                reportSelector = (ReportSelector){
                    CategoryIDs = (ArrayOfInt)
                    CampaignIDs = (ArrayOfInt)
                    BannerIDs = (ArrayOfInt)
                }
                reportType = (ReportType)
                aggregationType = (AggregationType)
                startDate = None
                endDate = None
                isResultGzipped = None
            }
        """
        if not isinstance(reportJob, dict):
            raise TypeError('reportJob must be a dictionary')
        report = self._make_type('ReportJob')
        return self.__client.service.scheduleReportJob(
            _assign(reportJob, report)
        )

    def downloadReport(self, jobID, path):
        """
        Utility method for downloading a report in csv format.
        Args:
            jobID: jobID
            path: path to destination csv file
        """

        rows = self.get_report(jobID)

        with open(path, 'wb') as rep:
            wr = csv.DictWriter(rep, set([f for r in rows for f in r.keys()]))
            wr.writeheader()

            for row in rows:
                wr.writerow(row.attrib)

    def get_report(self, jobID):
        """
        Utility method for downloading a report in list of dict's.
        Args:
            jobID: jobID
        """
        while True:
            if not self.__client.service.getJobStatus(jobID) == 'Pending':
                break

        table = etree.parse(
            urlopen(self.getReportDownloadUrl(jobID))
        ).getroot().getchildren()[0]

        return [i for i in table if i.tag == 'rows'][0]

    def _make_type(self, object_name):
        return self.__client.factory.create(object_name)

    def logging(self, log_level='DEBUG'):
        """
        Set the logging level of the client instance
        KwArgs:
            log_level: logging level for Python logging module
        """
        import logging
        logging.basicConfig(level=logging.INFO)
        logging.getLogger('suds.client').setLevel(getattr(logging, log_level))
