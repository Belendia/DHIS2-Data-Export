import os
from dhis2 import Api
import json
from os.path import exists
from csv import writer
import concurrent.futures

# please specify the server address, username and password here.
SERVER = ''
USERNAME = ''
PASSWORD = ''

ORG_UNIT_FIELDS = 'id,name,shortName,featureType,coordinates,parent'
DATA_ELEMENTS_GROUP_FIELDS = 'id,name,dataElements'
PAGE_SIZE = 50
ORG_UNIT_START_POS = 0
DATA_SET_ID = ''  # please specify the dataset ID here

YEARS = ['2010', '2011', '2012', '2013']
MONTHS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
TEMP_DATA_FILE_PATH = 'data/temp_files/'


class DHIS2:
    def __init__(self):
        self.api = Api(SERVER, USERNAME, PASSWORD)

        self.org_units = {}
        self.category_option_combos = {}
        self.data_elements = {}
        self.data_element_groups = {}
        self.__period = []
        self.data = []

        self.__org_unit_file_name = 'metadata/OrgUnits.json'
        self.__category_option_combos_file_name = 'metadata/CategoryOptionCombos.json'
        self.__data_elements_file_name = 'metadata/DataElements.json'
        self.__data_element_groups_file_name = 'metadata/DataElementGroups.json'

        # for year in YEARS:
        #     one_year_period = []
        #     for month in MONTHS:
        #         one_year_period.append("{}{}".format(year, month))
        #     self.__period.append(','.join(one_year_period))

        for year in YEARS:
            for month in MONTHS:
                self.__period.append("{}{}".format(year, month))

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

    def __download_category_option_combos(self):
        print('Downloading category option combos ...')
        for index, page in enumerate(
                self.api.get_paged('categoryOptionCombos', page_size=PAGE_SIZE)):
            for c in page['categoryOptionCombos']:
                self.category_option_combos[c['id']] = c['displayName']
            print("Page {} of {}".format(index + 1, page['pager']['pageCount']))

    def __download_data_element_groups(self):
        print('Downloading data element groups ...')
        for index, page in enumerate(
                self.api.get_paged('dataElementGroups', params={'fields': DATA_ELEMENTS_GROUP_FIELDS},
                                   page_size=PAGE_SIZE)):
            for deg in page['dataElementGroups']:
                des = []
                # data elements belonging to this data element group
                for de in deg['dataElements']:
                    des.append(de['id'])
                # {dataElementGroupId:{name: DataElementName, dataElements: [dataElementId, dataElementId]}}
                self.data_element_groups[deg['id']] = {"name": deg['name'], 'dataElements': des}
            print("Page {} of {}".format(index + 1, page['pager']['pageCount']))

    def __get_data_element_group_name(self, id):
        for deg_id, data in self.data_element_groups.items():
            if id in data['dataElements']:
                return data['name']
        return ''

    def __download_data_elements(self):
        print('Downloading data elements ...')
        for index, page in enumerate(
                self.api.get_paged('dataElements', page_size=PAGE_SIZE)):
            for de in page['dataElements']:
                self.data_elements[de['id']] = {'name': de['displayName'],
                                                'groupName': self.__get_data_element_group_name(de['id'])}
            print("Page {} of {}".format(index + 1, page['pager']['pageCount']))

    def __download_org_unit_data(self, org_unit):
        print("Downloading data for org unit ... {}".format(org_unit['name']))

        r = self.api.get('dataValueSets', params={
            'dataSet': [DATA_SET_ID],
            'orgUnit': [org_unit['id']],
            'period': ','.join(self.__period)  # p
        })

        data_values = r.json()
        if data_values:
            # save the header
            if exists('{}x_{}.csv'.format(TEMP_DATA_FILE_PATH, org_unit['id'])):
                os.remove('{}x_{}.csv'.format(TEMP_DATA_FILE_PATH, org_unit['id']))

            self.__save_data([['OrgUnitId', 'OrgUnitName', 'DataElement', 'DataElementGroup', 'Period',
                               'CategoryOption', 'AttributeOption', 'Value', 'StoredBy', 'Created', 'LastUpdated',
                               'Comment', 'FollowUp']], org_unit['id'])

            data = []
            for d in data_values['dataValues']:
                data.append([org_unit['id'], org_unit['name'], self.data_elements[d['dataElement']]['name'],
                             self.data_elements[d['dataElement']]['groupName'], DHIS2.get_data(d, 'period'),
                             self.category_option_combos[d['categoryOptionCombo']],
                             self.category_option_combos[d['attributeOptionCombo']], DHIS2.get_data(d, 'value'),
                             DHIS2.get_data(d, 'storedBy'), DHIS2.get_data(d, 'created'),
                             DHIS2.get_data(d, 'lastUpdated'), DHIS2.get_data(d, 'comment'),
                             DHIS2.get_data(d, 'followup')])

            self.__save_data(data, org_unit['id'])

    def __download_data(self):
        # download 20 health facility data at a time
        print('Downloading org units...')
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(self.__download_org_unit_data, self.org_units['organisationUnits'][ORG_UNIT_START_POS:])

        print('Done')

    @staticmethod
    def get_data(var, key):
        if key in var:
            return var[key]
        return ''

    def __save_org_units(self):
        print('Saving org units to {} ... '.format(self.__org_unit_file_name), end=" ")
        with open(self.__org_unit_file_name, 'w') as f:
            json.dump(self.org_units, f)
        print('Done')

    def __save_category_option_combos(self):
        print('Saving category option combos to {} ... '.format(self.__category_option_combos_file_name), end=" ")
        with open(self.__category_option_combos_file_name, 'w') as f:
            json.dump(self.category_option_combos, f)
        print('Done')

    def __save_data_element_groups(self):
        print('Saving data element groups to {} ... '.format(self.__data_element_groups_file_name), end=" ")
        with open(self.__data_element_groups_file_name, 'w') as f:
            json.dump(self.data_element_groups, f)
        print('Done')

    def __save_data_elements(self):
        print('Saving data elements to {} ... '.format(self.__data_elements_file_name), end=" ")
        with open(self.__data_elements_file_name, 'w') as f:
            json.dump(self.data_elements, f)
        print('Done')

    @staticmethod
    def __save_data(data, org_unit_id):
        with open('{}x_{}.csv'.format(TEMP_DATA_FILE_PATH, org_unit_id), 'a+',
                  newline='\n') as write_obj:
            csv_writer = writer(write_obj)
            for d in data:
                csv_writer.writerow(d)

    def __org_unit_load_json_file(self):
        print('Loading {} ... '.format(self.__org_unit_file_name), end=" ")
        with open(self.__org_unit_file_name, "r") as json_file:
            self.org_units = json.load(json_file)
        print('Done')

    def __category_option_combos_load_json_file(self):
        print('Loading {} ... '.format(self.__category_option_combos_file_name), end=" ")
        with open(self.__category_option_combos_file_name, "r") as json_file:
            self.category_option_combos = json.load(json_file)
        print('Done')

    def __data_element_groups_load_json_file(self):
        print('Loading {} ... '.format(self.__data_element_groups_file_name), end=" ")
        with open(self.__data_element_groups_file_name, "r") as json_file:
            self.data_element_groups = json.load(json_file)
        print('Done')

    def __data_elements_load_json_file(self):
        print('Loading {} ... '.format(self.__data_elements_file_name), end=" ")
        with open(self.__data_elements_file_name, "r") as json_file:
            self.data_elements = json.load(json_file)
        print('Done')

    def config_org_unit(self):
        if exists(self.__org_unit_file_name):
            self.__org_unit_load_json_file()
        else:
            self.__download_org_units()
            self.__save_org_units()

    def config_category_option_combo(self):
        if exists(self.__category_option_combos_file_name):
            self.__category_option_combos_load_json_file()
        else:
            self.__download_category_option_combos()
            self.__save_category_option_combos()

    def config_data_element_group(self):
        if exists(self.__data_element_groups_file_name):
            self.__data_element_groups_load_json_file()
        else:
            self.__download_data_element_groups()
            self.__save_data_element_groups()

    def config_data_element(self):
        if exists(self.__data_elements_file_name):
            self.__data_elements_load_json_file()
        else:
            self.__download_data_elements()
            self.__save_data_elements()

    def run(self):
        self.config_org_unit()
        self.config_category_option_combo()
        self.config_data_element_group()
        self.config_data_element()
        self.__download_data()


if __name__ == "__main__":
    dhis2 = DHIS2()
    dhis2.run()
