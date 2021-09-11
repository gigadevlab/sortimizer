import pandas as pd


def increase_csv(csv_filepath):
    col_list = ['IP', 'Time', 'URL', 'Status']
    df = pd.DataFrame(pd.read_csv(csv_filepath, usecols=col_list))
    df.to_csv(csv_filepath, mode='a', header=False)


if __name__ == '__main__':
    increase_csv('weblog.csv')
