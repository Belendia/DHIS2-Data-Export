from dhis2 import Api
import json

SERVER = 'https://'
USERNAME = ''
PASSWORD = ''

ORG_UNIT_FIELDS = 'id,name,shortName,featureType,coordinates,parent'
PAGE_SIZE = 50


class DHIS2:
    def __init__(self):
        self.api = Api(SERVER, USERNAME, PASSWORD)
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

    def __save_org_units(self):
        print('Saving org units to {} ... '.format(self.__org_unit_file_name), end=" ")
        with open(self.__org_unit_file_name, 'w') as f:
            json.dump(self.org_units, f)
        print('Done')

    def run(self):
        self.__download_org_units()
        self.__save_org_units()


if __name__ == "__main__":
    dhis2 = DHIS2()
    dhis2.run()
