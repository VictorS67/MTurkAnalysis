"""
This Script is used for analyze the exported data from UR survey in MTurk Project.
"""
import numpy as np
import pandas as pd


def load_data(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except:
        print("Cannot read csv file")
        return None


def clean_data(df: pd.DataFrame, reward_name: str, arm_name: str) -> pd.DataFrame:
    try:
        # Filter out relevant columns
        df = df.filter(items=["StartDate", "EndDate", "Finished",
                              "RecordedDate", "ResponseId", "ID", reward_name, arm_name])
        # Drop first two rows
        df = df.iloc[2:]
        # Change column types
        df = pd.DataFrame(
            {
                "StartDate": pd.Series(df["StartDate"], dtype=np.dtype('datetime64')),
                "EndDate": pd.Series(df["EndDate"], dtype=np.dtype('datetime64')),
                "RecordedDate": pd.Series(df["RecordedDate"], dtype=np.dtype('datetime64')),
                "ResponseId": pd.Series(df["ResponseId"], dtype=np.dtype('O')),
                "ID": pd.Series(df["ID"], dtype=np.dtype('O')),
                "Finished": pd.Series(df["Finished"], dtype=np.dtype('int32')),
                "Reward": pd.Series(df[reward_name], dtype=np.dtype('float64')),
                "Arm": pd.Series(df[arm_name], dtype=np.dtype('int32')),
            })
        # Drop NAs
        df = df.dropna()
        # Drop test responses
        df = df[(df['StartDate'].dt.month != 8)]
        # Drop unfinished responses
        df = df.drop(df[df.Finished != 1].index)
        return df
    except:
        print("Cannot filter some columns")
        return None


def compute_mean_reward(df: pd.DataFrame) -> pd.DataFrame:
    try:
        return df.groupby(by=["Arm"]).agg({'Reward': ['mean', 'count', 'max', 'min']})
    except:
        print("Cannot compute mean reward")
        return None


if __name__ == "__main__":
    df = load_data('./UR_Sep_17.csv')
    df = clean_data(df, 'Rating_1', 'arm')
    print(df)
    df_reward_mean = compute_mean_reward(df)
    print(df_reward_mean)
