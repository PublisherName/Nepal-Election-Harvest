import pandas as pd
import os
import hashlib

class ElectionDataGroup():

    def __init__(self):
        self.state_id = [1,2,3,4,5,6,7]
        self.RES_DIR = 'res'
        self.MUN_DIR = 'res/mun'
        self.WARD_DIR = 'res/ward'
        self.VOTING_CENTER_DIR = 'res/voting_center'
        self.VOTER_DIR = 'res/voter'
        self.LOC_ENC_FILENAME = 'res/processed/location_encoding.csv'
        self.PROCESSED_DIR = 'res/processed/by_district'
        self.DISTRICT_FILE = os.path.join(self.RES_DIR, 'districts.csv')
        os.makedirs(self.PROCESSED_DIR,exist_ok=True)
        self.district_voter_data = pd.DataFrame()
        if not os.path.isdir(self.RES_DIR):
            exit("Resources Not found.")
    
    def read_district_of_state(self,state):
        if state > 7 or state < 1:
            exit("Error : Invalid State Id")
        if os.path.exists(self.DISTRICT_FILE):
            df = pd.read_csv(self.DISTRICT_FILE)
            district_df = df[df['district_state'] == state]
            district_dict = district_df[['district_name', 'district_id']].set_index('district_id')['district_name'].to_dict()
            return district_dict if bool(district_dict) else None
        return None
    
    def read_municipality_of_district(self,district):
        MUN_FILE = os.path.join(self.MUN_DIR, f"{district}.csv")
        if os.path.exists(MUN_FILE):
            mun_df = pd.read_csv(MUN_FILE)
            municipality_dict = mun_df[['municipality_name', 'municipality_id']].set_index('municipality_id')['municipality_name'].to_dict()
            return municipality_dict if bool(municipality_dict) else None
        return None

    def read_ward_of_municipality(self,municipality):
        WARD_FILE = os.path.join(self.WARD_DIR, f"{municipality}.csv")
        if os.path.exists(WARD_FILE):
            ward_df = pd.read_csv(WARD_FILE)
            ward_dict = ward_df[['ward_name', 'ward_id']].set_index('ward_id')['ward_name'].to_dict()
            return ward_dict if bool(ward_dict) else None
        return None
    
    def read_voting_center_of_municipality_ward(self,municipality,ward):
        VOTING_CENTER_FILE = os.path.join(self.VOTING_CENTER_DIR, f"{municipality}", f"{ward}.csv")
        if os.path.exists(VOTING_CENTER_FILE):
            voting_center_df = pd.read_csv(VOTING_CENTER_FILE)
            voting_center_dict = voting_center_df[['voting_center_name', 'voting_center_id']].set_index('voting_center_id')['voting_center_name'].to_dict()
            return voting_center_dict if bool(voting_center_dict) else None
        return None
    
    def read_voter_of_voting_center(self,voting_center):
        VOTER_FILE = os.path.join(self.VOTER_DIR, f"{voting_center}.csv")
        if os.path.exists(VOTER_FILE):
            voter_df = pd.read_csv(VOTER_FILE)
            return voter_df if not voter_df.empty else None
        return None
    
    def voter_concate(self,other_df,refs):
        other_df['state'],other_df['district'],other_df['municipality'],other_df['ward'],other_df['voting_center'] = refs
        other_df['voter_name'] = other_df['voter_name'].str.replace('  ', ' ')
        other_df['voter_gender'] = other_df['voter_gender'].replace({'पुरुष': 1, 'महिला': 0})
        self.district_voter_data = pd.concat([self.district_voter_data,other_df])
    
    def voter_save(self,filename):
        self.encode_location()
        self.district_voter_data.to_csv(os.path.join(self.PROCESSED_DIR, f'{filename}.csv'), index=False)
        print(f"Info: Total voter in {filename} is : {self.district_voter_data.shape[0]}")
        self.district_voter_data = self.district_voter_data.iloc[0:0]
    
    def encode_location(self):
        try:
            df_location = pd.read_csv(self.LOC_ENC_FILENAME)
            identifiers = dict(zip(df_location['location'], df_location['count']))
        except FileNotFoundError:
            identifiers = {}

        for idx, row in self.district_voter_data.iterrows():
            identifier = f"{row['state']}-{row['district']}-{row['municipality']}-{row['ward']}-{row['voting_center']}"
            if identifier not in identifiers:
                identifiers[identifier] = len(identifiers) + 1

        df_location = pd.DataFrame(list(identifiers.items()), columns=['location', 'count'])
        df_location.to_csv(self.LOC_ENC_FILENAME, index=False)

        self.district_voter_data['location_id'] = self.district_voter_data.apply(lambda row: identifiers[f"{row['state']}-{row['district']}-{row['municipality']}-{row['ward']}-{row['voting_center']}"], axis=1)
        self.district_voter_data = self.district_voter_data.drop(['state', 'district', 'municipality', 'ward', 'voting_center'], axis=1)
    
    def is_processed(self,district_name):
        checkfile = district_name + ".csv"
        for filename in os.listdir(self.PROCESSED_DIR):
            if filename == checkfile:
                return True
                break
        else:
            return False
        

DataGroup = ElectionDataGroup()
for state_id in DataGroup.state_id:
    district_data = DataGroup.read_district_of_state(state_id)
    for district_id,district_name in district_data.items():
        if not DataGroup.is_processed(district_name):
            municipality_data = DataGroup.read_municipality_of_district(district_id)
            for municipality_id,municipality_name in municipality_data.items():
                ward_data = DataGroup.read_ward_of_municipality(municipality_id)
                for ward_name,ward_id in ward_data.items():
                    voting_center_data = DataGroup.read_voting_center_of_municipality_ward(municipality_id,ward_id)
                    for voting_center_id, voting_center_name in voting_center_data.items():
                        voter_data = DataGroup.read_voter_of_voting_center(voting_center_id)
                        if not voter_data.empty:
                            refs = [state_id,district_name,municipality_name,ward_id,voting_center_name]
                            DataGroup.voter_concate(voter_data,refs)
            DataGroup.voter_save(district_name)