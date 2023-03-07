import requests
import json
from bs4 import BeautifulSoup


class ElectionHelper:

    __URL = "https://voterlist.election.gov.np/bbvrs1/index_process_1.php"
    __VoterListUrl = "https://voterlist.election.gov.np/bbvrs1/view_ward_1.php"
    __PingUrl = "https://election.gov.np"
    __DistrictDict = {}
    __MunicipalityDict = {}
    __WardDict = {}
    __VotingCenterDict = {}
    __VoterListDict = {}

    def __init__(self):
        try:
            response = requests.head(self.__PingUrl)
            if response.status_code != 200:
                raise Exception("Error: Unable to connect to remote host.")
        except Exception as e:
            exit(str(e))

    def RequestServer(self, payload):
        records = {}
        URL = self.__VoterListUrl if payload.get("reg_centre") else self.__URL
        try:
            i =1
            response = requests.post(URL, payload)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser') if payload.get("reg_centre") else BeautifulSoup(str(json.loads(response.text)),"lxml") 
                if payload.get("reg_centre"):
                    table = soup.find(class_='div_bbvrs_data')
                    rows = table.find_all('tr')[1:]
                    for row in rows:
                        columns = row.find_all('td')
                        records[columns[1].text.strip()] = {
                            "name": columns[2].text.strip(),
                            "age": columns[3].text.strip(),
                            "gender": columns[4].text.strip(),
                            "parents": columns[5].text.strip(),
                            "spouse": columns[6].text.strip()
                        }
                else:
                    options = soup.find_all('option')[1:]
                    for option in options:
                        records[int(option['value'])] = option.text
            return records if records else None
        except Exception as e:
            exit(f"Error: {str(e)}")
    
    def get_district(self):
        payload = {'list_type': 'district'}
        self.__DistrictDict.update({state: self.RequestServer({**payload, 'state': state}) for state in range(1, 8)})
        return self.__DistrictDict

    def get_municipality(self, district):
        payload = {'district': district, 'list_type': 'vdc'}
        self.__MunicipalityDict = {}
        self.__MunicipalityDict[district] = self.RequestServer(payload)
        return self.__MunicipalityDict
    
    def get_ward(self, vdc):
        payload = {'vdc': vdc, 'list_type': 'ward'}
        self.__WardDict = {}
        self.__WardDict[vdc] = self.RequestServer(payload)
        return self.__WardDict
    
    def get_voting_center(self,vdc,ward):
        payload = {"vdc":vdc,"ward":ward,"list_type":"reg_centre"}
        self.__VotingCenterDict = {}
        self.__VotingCenterDict[vdc] = {ward : self.RequestServer(payload)}
        return self.__VotingCenterDict
    
    def get_voter_list(self,state,district,vdc,ward,votingcenter):
        payload = {"state":state,"district":district,"vdc_mun":vdc,"ward":ward,"reg_centre":votingcenter}
        self.__VoterListDict = {}
        self.__VoterListDict[votingcenter]=self.RequestServer(payload)
        return self.__VoterListDict