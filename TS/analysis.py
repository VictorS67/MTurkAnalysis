"""
This Script is used for analyze the exported data from TS survey in MTurk Project.
It can output:
    - A ROUGH version of mean rewards of each arm from MOOClet database
"""
import requests
import numpy as np
import pandas as pd
from TS.secure import *


def load_variables(payload: dict, key_name: str=None):
    try:
        variables = {}
        for index, (key, value) in enumerate(payload.items()):
            if type(value) is int:
                if key_name:
                    url = mooclet_url + f'{key_name}/{value}'
                else:
                    url = mooclet_url + f'{mooclet_url_parameters[key]}/{value}'
                objects = requests.get(url, headers={'Authorization': mooclet_auth_token})
                if objects.status_code != 200:
                    print("unable to request endpoint: ", str(reward_id))
                    return None
                objects = objects.json()
                if key_name:
                    if variables.get(key_name):
                        variables[key_name].append(objects)
                    else:
                        variables[key_name] = [objects]
                else:
                    variables[key] = objects
            else:
                variables = {**variables, **load_variables(value, key)}
        return variables
    except:
        print("load_variables: Cannot call APIs")
        return None


def load_data(payload: dict) -> pd.DataFrame:
    try:
        variables = load_variables(payload)
        print(variables)

        columns = ["Arm", "Reward"]

        # get version value dataframe
        versions = []
        print()
        for version in variables['version']:
            version_id = version['id']
            version_url = mooclet_url + f'value?variable__name=version&version={version_id}'
            objects = requests.get(version_url, headers={'Authorization': mooclet_auth_token})
            if objects.status_code != 200:
                print("unable to request endpoint: ", str(reward_id))
                return None
            versions += objects.json()['results']
        version_df = pd.DataFrame.from_dict(versions)
        version_df.to_csv('version_df.csv')

        version_df = version_df.filter(items=["learner", "mooclet", "version", "policy", "timestamp"])
        version_df = version_df.rename(columns={'timestamp': 'timestamp_version'})
        print(version_df.shape)

        # get reward value dataframe
        reward = variables['reward']
        reward_url = mooclet_url + f'value?variable__name={reward["name"]}'
        objects = requests.get(reward_url, headers={'Authorization': mooclet_auth_token})
        if objects.status_code != 200:
            print("unable to request endpoint: ", str(reward_id))
            return None
        rewards = objects.json()['results']
        reward_df = pd.DataFrame.from_dict(rewards)
        reward_df.to_csv('reward_df.csv')
        reward_df = reward_df.filter(items=["learner", "mooclet", "version", "policy", "value", "timestamp"])
        reward_df = reward_df.rename(columns={'timestamp': 'timestamp_reward'})
        print(reward_df.shape)

        print()
        merged_df = pd.merge(version_df, reward_df, how="inner",
                             on=["learner", "mooclet", "version", "policy"])

        print(merged_df.shape)
        merged_df.to_csv('merged_df.csv')
        return None
    except:
        print("load_data: Cannot call APIs")
        return None


def clean_data(df: pd.DataFrame, reward_name: str, arm_name: str) -> pd.DataFrame:
    pass


def compute_mean_reward(df: pd.DataFrame) -> pd.DataFrame:
    pass


if __name__ == "__main__":
    mooclet_id = 20
    reward_id = 33
    context_ids = {}
    version_ids = {"arm_1": 47, "arm_2": 48}
    policyparameters_ids = {"thompson_sampling_contextual": 38, "uniform_random": 37}

    payload = {
        "mooclet": mooclet_id,
        "reward": reward_id,
        "context": context_ids,
        "version": version_ids,
        "policyparameters": policyparameters_ids
    }

    df = load_data(payload)

