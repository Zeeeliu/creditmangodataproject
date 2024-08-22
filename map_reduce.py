import pandas as pd
import os

# allocate for the memory space


def process_file(filename):
    m_list = [[] for _ in range(1000)]

    df_report = pd.read_csv(filename)
    ssn_list = df_report['SSN'].tolist()
    print(f'...reading source file {filename} and converting it to list...')

    for ssn in ssn_list:
        index = hash(ssn) % 1000
        m_list[index].append(ssn)
    # Write each list in m_list to a CSV file based on its index
    for idx, ssn_group in enumerate(m_list):
        if ssn_group:  # Only write if there's data
            df = pd.DataFrame(ssn_group, columns=['SSN'])

            #sort the df
            df_sorted = df.sort_values(by='SSN')

            # Create directory dynamically if it doesn't exist
            directory_path = f"/Users/ziyeliu/Documents/index_V2/index_{idx}"
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)

            # write to csv
            print(df_sorted.shape)
            df_sorted.to_csv(f"/Users/ziyeliu/Documents/index_V2/index_{idx}/{hash(filename)}.csv", index=False)
            print(f'index is {idx} and filename is {filename}')

# List of your CSV files
files = [
    './Resources/credit_report0.csv',
    './Resources/credit_report1.csv',
    './Resources/credit_report2.csv',
    './Resources/credit_report3.csv',
    './Resources/credit_report4.csv',
    './Resources/credit_report5.csv',
    './Resources/credit_report6.csv',
    './Resources/credit_report7.csv',
    './Resources/credit_report8.csv',
    './Resources/credit_report9.csv',
]
for file in files:
    process_file(file)

