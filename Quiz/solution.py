import pandas as pd
import os
import requests
import sqlite3 as sql
import time

def main():
    """This is the main function which call every function"""
    try:
        start_time = time.time()
        all_df = read_files()
        if all_df:
            df = pd.concat(all_df, ignore_index=True)
            df['ConvertedAmount'] = 0
        for idx, row in df.iterrows():
            C_amount = conversion(row)
            df.iat[idx,3] = C_amount
        table = save_in_db(df)

        if table:
            print('Total time taken in operation:', time.time() - start_time)
            return True
    except Exception as e:
        print('Error in read_files')
        return e

def read_files():
    """This function reads the files and converts them to list of Fataframes"""
    try:
        df_list = []
        folder_list = [file for file in os.listdir() if  not file.endswith('.py')]
        for folder in folder_list:
            if folder.endswith('comma'):
                df_comma_list = def_list(folder, ', ')
                df_list.extend(df_comma_list)
            elif folder.endswith('camel'):
                df_camel_list = def_list(folder, ', ')
                df_list.extend(df_camel_list)
            elif folder.endswith('pipe'):
                df_pipe_list = def_list(folder, '|')
                df_list.extend(df_pipe_list)

        return df_list
    except Exception as e:
        print('Error in read_files ')
        return e

def def_list(folder, seperator):
    """
    This function returns df list of individual folder
    Input:
        folder: folder Name
        seperator: delimiter in csv files
    Output:
        df_list: list of dataframes
    """
    try:
        if (seperator == ', ' and folder.endswith('comma')) or (seperator == '|'):
            index_col = 'ID'
        else:
            index_col = 'TrxId'
        current_dir = os.getcwd()
        folder_path = os.path.join(current_dir, folder)
        df_list = []
        file_list = [file for file in os.listdir(folder_path)]
        for file in file_list:
            df = pd.read_csv(os.path.join(folder_path,file), delimiter=seperator, engine='python')
            df.set_index(index_col, inplace=True)
            df.columns = ['SourceCurrency', 'DestinationCurrency', 'SourceAmount']
            df_list.append(df)
        return df_list
    except Exception as e:
        print('Error in def_list ')
        return e

def conversion(df_row):
    """
    This function returns converted Currency amount
    Input:
        df_row : Represnts individual row in dataframe
    Output:
        result: Converted Amount 
    """
    try:
        url = f"https://api.apilayer.com/fixer/convert?to={df_row.DestinationCurrency}&from={df_row.SourceCurrency}&amount={df_row.SourceAmount}"
        headers= {
        "apikey": "QVOGBucdTbxFH8OOlYE1lgMP4pysP0Fv"
        }

        response = requests.get(url, headers=headers)
        http_status = response.status_code
        if http_status:
            result = response.json()
            return result['result']
    except Exception as e:
        print('Error in conversion')
        return e

def save_in_db(df):
    """
    This function Saves df to table in memory db
    Input:
        df: Transformed Dataframe
    Output:
        True if successful
    """
    try:
        conn = sql.connect(':memory:')
        if conn:
            c = conn.cursor()
            c.execute('CREATE TABLE IF NOT EXISTS fx_rates (SourceCurrency text, DestinationCurrency text, SourceAmount number)')
            conn.commit()
            df.to_sql('fx_rates', conn, if_exists='replace', index = False)

            c.execute('''  
            SELECT * FROM fx_rates
                    ''')

            for row in c.fetchall():
                # print (row)
                pass

            conn.close()
            return True
    except Exception as e:
        print('Error in save_in_db')
        return e
    

if __name__ == '__main__':
    """
    Note: 30 API call are left in Free Tier
    **********************************************************************
    **********************************************************************
    CLI run command: python solution,py
    **********************************************************************
    **********************************************************************
    """
    op = main()
    if op:
        print('Conversion Completed')