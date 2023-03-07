from ElectionHelper import *
import pandas as pd
from tqdm import tqdm
import asyncio

import os

class DbHelper(ElectionHelper):
    
    def __init__(self):
        self.RES_DIR = 'res'
        self.DISTRICT_FILE = os.path.join(self.RES_DIR, 'districts.csv')
        self.MUN_DIR = os.path.join(self.RES_DIR, 'mun')
        self.WARD_DIR = os.path.join(self.RES_DIR, 'ward')
        self.VOTING_CENTER = os.path.join(self.RES_DIR, 'voting_center')
        self.VOTER = os.path.join(self.RES_DIR, 'voter')
        os.makedirs(self.VOTER, exist_ok=True)
        os.makedirs(self.RES_DIR, exist_ok=True)
        os.makedirs(self.MUN_DIR, exist_ok=True)
        os.makedirs(self.WARD_DIR, exist_ok=True)
        os.makedirs(self.VOTING_CENTER, exist_ok=True)

    def get_district_df(self):
        if not os.path.exists(self.DISTRICT_FILE):
            data = self.get_district()
            df = pd.DataFrame({'district_state': [k for k, v in data.items() for d, n in v.items()],
                               'district_id': [d for k, v in data.items() for d, n in v.items()],
                               'district_name': [n for k, v in data.items() for d, n in v.items()]})
            df = df.groupby(['district_state', 'district_id'], as_index=False).agg({'district_name': 'first'})
            df.to_csv(self.DISTRICT_FILE, index=False)
        else:
            df = pd.read_csv(self.DISTRICT_FILE)
        return df if not df.empty else None
    
    def get_municipality_df(self,district_id):
        tmp_mun_file = os.path.join(self.MUN_DIR ,f"{district_id}.csv")
        if not os.path.exists(tmp_mun_file):
            data = self.get_municipality(district_id)
            df = pd.DataFrame({'municipality_district': [k for k, v in data.items() for d, n in v.items()],
                               'municipality_id': [d for k, v in data.items() for d, n in v.items()],
                               'municipality_name': [n for k, v in data.items() for d, n in v.items()]})
            df = df.groupby(['municipality_district', 'municipality_id'], as_index=False).agg({'municipality_name': 'first'})
            df.to_csv(tmp_mun_file, index=False)
        else:
            df = pd.read_csv(tmp_mun_file)
        return df if not df.empty else None
    
    def get_ward_df(self,vdc_id):
        tmp_ward_file = os.path.join(self.WARD_DIR, f"{vdc_id}.csv")
        if not os.path.exists(tmp_ward_file):
            data = self.get_ward(vdc_id)
            df = pd.DataFrame({'municipality_id': [k for k, v in data.items() for d, n in v.items()],
                               'ward_id': [d for k, v in data.items() for d, n in v.items()],
                               'ward_name': [n for k, v in data.items() for d, n in v.items()]})
            df = df.groupby(['municipality_id', 'ward_id'], as_index=False).agg({'ward_name': 'first'})
            df.to_csv(tmp_ward_file, index=False)
        else:
            df = pd.read_csv(tmp_ward_file)
        return df if not df.empty else None

    def get_voting_center_df(self,vdc_id,ward_id):
        tmp_voting_center_dir = os.path.join(self.VOTING_CENTER + f"/{vdc_id}")
        os.makedirs(tmp_voting_center_dir, exist_ok=True)
        tmp_voting_center_file = os.path.join(tmp_voting_center_dir, f"{ward_id}.csv")
        if not os.path.exists(tmp_voting_center_file):
            data = self.get_voting_center(vdc_id,ward_id)
            df = pd.DataFrame(columns=["municipality_id", "ward_id", "voting_center_id", "voting_center_name"])
            for municipality_id, values in data.items():
                for ward_id, voting_center in values.items():
                    for voting_center_id, voting_center_name in voting_center.items():
                        df_new = pd.DataFrame({"municipality_id": [municipality_id], "ward_id": [ward_id], "voting_center_id": [voting_center_id], "voting_center_name": [voting_center_name]})
                        df = pd.concat([df, df_new])
            df.to_csv(tmp_voting_center_file, index=False)
        else:
            df = pd.read_csv(tmp_voting_center_file)
        return df if not df.empty else None
    
    async def get_voter_df(self,state_id,district_id,vdc_id,ward_id,voting_center_id):
        tmp_voter_file = os.path.join(self.VOTER, f"{voting_center_id}.csv")
        print(f"Harvesting Data of state: {state_id} , District : {district_id}, vdc : {vdc_id} , ward No: {ward_id}, voting center: {voting_center_id}")
        if not os.path.exists(tmp_voter_file):
            data = self.get_voter_list(state_id,district_id,vdc_id,ward_id,voting_center_id)
            df = pd.DataFrame(columns=["voter_id", "voter_name", "voter_age", "voter_gender", "voter_parents","voter_spouse"])
            for _,values in data.items():
                for voter_id,details in values.items():
                    df_new = pd.DataFrame({"voter_id": [voter_id], "voter_name": [details['name']], "voter_age": [details['age']],"voter_gender" : [details['gender']],"voter_parents" : [details['spouse']],"voter_spouse" : [details['parents']]})
                    df = pd.concat([df, df_new])
            df.to_csv(tmp_voter_file, index=False)
        else:
            df = pd.read_csv(tmp_voter_file)
        return df if not df.empty else None

helper = DbHelper()

async def main():
    df_district = helper.get_district_df()
    for _, district in df_district.iterrows():
        df_municipality = helper.get_municipality_df(district.district_id)
        for _, municipality in df_municipality.iterrows():
            df_ward = helper.get_ward_df(municipality.municipality_id)
            tasks = []
            for _, ward in df_ward.iterrows():
                df_voting_center = helper.get_voting_center_df(municipality.municipality_id,ward.ward_id)
                for _, voting_center in tqdm(df_voting_center.iterrows(), desc=f"Queuing Data of {district.district_name} , {municipality.municipality_name} , ward No: {ward.ward_id}"):
                    task = asyncio.create_task(helper.get_voter_df(district.district_state,district.district_id,municipality.municipality_id,ward.ward_id,voting_center.voting_center_id))
                    tasks.append(task)
            await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())