from dhis2 import Api
import json
from os.path import exists

SERVER = ''
USERNAME = ''
PASSWORD = ''

ORG_UNIT_FIELDS = 'id,name,shortName,featureType,coordinates,parent'
PAGE_SIZE = 50

DATA_ELEMENTS = {"rmqxJ1TtUEA": "B50-196 PF",
                 "moq6FPoeBDm": "B50-197 PF with cerebral complications",
                 "ITGJpcL9wra": "B50-198 Other severe and complicated PF",
                 "z1mnnlpCKew": "B50-199 PF unspecified",
                 "SnKf3dD3W5T": "B50-200 PV",
                 "ALQjhqZF6LA": "B50-201 PV with other complications",
                 "xb597ZmVbCq": "B50-202 PV without complication",
                 "WvMqItWTzvZ": "B50-203 Mixed (Other parasitologically confirmed malaria)",
                 "GYZmBo0O4ui": "B50-204 Unspecified malaria"}


class DHIS2:
    def __init__(self):
        self.api = Api(SERVER, USERNAME, PASSWORD)
        self.__dataset_id = 'LNLZYbrGEh6'
        self.org_units = {}
        self.__org_unit_file_name = 'metadata/OrgUnits.json'

    def __download_org_units(self):
        print('Downloading org units...')
        for index, page in enumerate(
                self.api.get_paged('organisationUnits',
                                   params={'fields': ORG_UNIT_FIELDS},
                                   page_size=PAGE_SIZE)):
            if index == 0:
                self.org_units = page
            else:
                # merge the new org units with the previously downloaded org units
                self.org_units['organisationUnits'] += page['organisationUnits']
            print("Page {} of {}".format(index + 1, page['pager']['pageCount']))

        # delete pager
        if 'pager' in self.org_units:
            del self.org_units['pager']

    def __download_data(self):
        print('Downloading org units...')
        for index, org_unit in enumerate(self.org_units['organisationUnits']):
            r = self.api.get('dataValueSets', params={
                'dataSet': [self.__dataset_id],
                'orgUnit': ['FZN1YXK7fWW'],
                'startDate': '2010-01-01',
                'endDate': '2013-09-01'
            })

            if index == 30:
                exit()
            print(r.json())
            exit()

    def __save_org_units(self):
        print('Saving org units to {} ... '.format(self.__org_unit_file_name), end=" ")
        with open(self.__org_unit_file_name, 'w') as f:
            json.dump(self.org_units, f)
        print('Done')

    def __load_json_file(self):
        print('Loading {} ... '.format(self.__org_unit_file_name), end=" ")
        with open(self.__org_unit_file_name, "r") as json_file:
            self.org_units = json.load(json_file)
        print('Done')

    def run(self):
        if exists(self.__org_unit_file_name):
            self.__load_json_file()
        else:
            self.__download_org_units()
            self.__save_org_units()
        self.__download_data()


if __name__ == "__main__":
    dhis2 = DHIS2()
    dhis2.run()
