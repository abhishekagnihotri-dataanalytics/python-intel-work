__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = "This script loads data for the GSM_ATS_Operations tabular model by staging the data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Daily at 11:05 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
import numpy as np
import re
from datetime import datetime
from time import time
import shutil
from Project_params import params
from Helper_Functions import loadExcelFile, uploadDFtoSQL, map_columns
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    start_time = time()
    # print(start_time)

    successfully_loaded_files = list()
    renamed_files = list()
    project_name = 'ATS Operations Dashboard'

    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\gsmssbi\AT\ATS Operations"
    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            if 'ERM Master Schedule' in excel_file:
                excel_sheet_name = 'ERM'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)

                if len(df.index) == 0:  # DataFrame is empty
                    log(False, project_name=project_name, data_area='ERM Schedule', row_count=0, error_msg="Unable to read Excel on SharePoint. Perhaps sheet named changed in ERM Master file.")
                    continue
                # print(df.columns)

                df['Upload_Date'] = datetime.today()

                keep_columns = ['Supplier', 'Tier', 'ATS Group', 'Segmentation', 'Status', 'Year', 'WW', 'Alt WW',
                                'Key Purpose ', 'Format', 'Time Zone', 'Admin (ONLY) Notes', 'Upload_Date']
                try:
                    df = df.drop(df.columns.difference(keep_columns), axis=1)  # remove other columns (Power Query) python equivalent
                except KeyError:
                    log(False, project_name=project_name, data_area='ERM Schedule', error_msg="Column missing/changed in ERM Master file.")
                    continue
                finally:
                    df = df[keep_columns]  # manually change column order

                # Correct values for WorkWeek and Alternate WorkWeek that are not decimals in Excel file
                for col in ['WW', 'Alt WW']:
                    try:
                        df[col].replace(r'^\s*$', np.nan, regex=True, inplace=True)  # replace field that's entirely spaces (or empty) with NaN
                        df[col] = df[col].apply(lambda x: re.sub("[^0-9.]", '', x.split(',')[0]) if isinstance(x, str) else x)  # removes all non-numerical characters from string
                        df[col] = df[col].apply(lambda x: float(x[:4]) if isinstance(x, str) else x)  # splice entries longer than 4 characters to maximum length (i.e. multiple dates entered, choose first)
                    except ValueError:
                        log(False, project_name=project_name, data_area='ERM Schedule', error_msg="Unable to parse column {} in ERM Master file".format(col))
                        continue

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_ERM'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='ERM Schedule', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_ERM']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Cost Ops' in excel_file:
                excel_sheet_name = "Project List"
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)

                if len(df.index) == 0:  # DataFrame is empty
                    log(False, project_name=project_name, data_area='Cost Ops Affordability Tracker', error_msg="Unable to read Excel on SharePoint. Perhaps sheet named changed in the Cost Ops Tracker file")
                    continue

                try:
                    # Change Yes/No column to True/False
                    df['VALIDATED BY FINANCE'] = df['VALIDATED BY FINANCE'].apply(lambda x: True if type(x) == str and x.lower() == 'yes' else False)
                except KeyError:
                    log(False, project_name=project_name, data_area='Cost Ops Affordability Tracker', error_msg="Column missing/changed in the Cost Ops Tracker file")
                    continue

                # Remove blank columns from end of DataFrame
                blank_columns = list()
                for column_name in df.columns:
                    if df[column_name].isnull().all():
                        blank_columns.append(column_name)
                df.drop(blank_columns, axis=1, inplace=True)  # remove all columns containing only blank values
                df.drop(columns=['Unnamed: 28', 'Unnamed: 29'], inplace=True, errors='ignore')  # ignore error if column does not exist
                # print(df.columns)

                # Remove rows where Project Name is blank
                df = df[df['Name'].notna()]

                # Unpivot the Roadmap Savings
                roadmap_savings_columns = [x for x in df.columns if x.startswith('20')]
                id_columns = [x for x in df.columns if x not in roadmap_savings_columns]
                # print(roadmap_savings_columns)

                df = df.melt(id_vars=id_columns, value_vars=roadmap_savings_columns, var_name='Year', value_name='Savings')  # unpivot data
                df['Year'] = df['Year'].apply(lambda x: x.split(' ')[0] if ' ' in x else x)  # Remove "Roadmap Savings" from value

                df['Savings'] = pd.to_numeric(df['Savings'], errors='coerce').astype(float)  # force Savings column to be numeric
                df = df[(df['Savings'] != 0) & (df['Savings'].notna())]  # filter out savings of $0 or NULL

                df['Upload_Date'] = datetime.today()  # add modified date as column at the end

                # map_columns(table=params['Table_ATS_Cost_Ops'], df=df)

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_Cost_Ops'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Cost Ops Affordability Tracker', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_Cost_Ops']))

                    excel_sheet_name = 'Goals'
                    df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                    df.drop(df.columns.difference(['Year', 'Area', 'Goal', 'Cost type']), axis=1, inplace=True)  # remove other columns (Power Query) python equivalent
                    df = df[df['Area'].notna()]  # remove rows where Area is blank
                    df['Upload_Date'] = datetime.today()

                    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_Cost_Ops_Goals'], data=df, truncate=True, driver="{SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='Cost Ops Goals', row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_Cost_Ops_Goals']))
                        successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                        renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            # elif 'Headcount' in excel_file:
            #     excel_sheet_name = 'HC Data'
            #     df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
            #     df['Upload_Date'] = datetime.today()
            #
            #     insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_HC'], data=df, truncate=True, driver="{SQL Server}")
            #     log(insert_succeeded, project_name=project_name, data_area='Headcount', row_count=df.shape[0], error_msg=error_msg)
            #
            #     if insert_succeeded:
            #         print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_HC']))
            #         successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
            #         renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Risk and Controls' in excel_file:
                excel_sheet_name = 'Sheet1'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                df['Upload_Date'] = datetime.today()

                sql_columns = ['ATSRisk', 'Year', 'Quarter', 'Events', 'ModifiedDateTime']
                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_RC'], data=df, columns=sql_columns, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Risk and Controls', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_RC']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif excel_file == 'ATS Wellnomics Data.xlsx':
                excel_sheet_name = 'Sheet1'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                df['Upload_Date'] = datetime.today()

                # Keep only the following columns
                keep_columns = ['Year', 'Work Week', 'Medium Risk Employees', 'High Risk Employees', 'Repeat High Risk Employees', 'High Percent', 'Upload_Date']
                try:
                    df = df.drop(df.columns.difference(keep_columns), axis=1)  # remove other columns (Power Query) python equivalent
                except KeyError:
                    log(False, project_name=project_name, data_area='Wellnomics', row_count=0, error_msg="Column missing/changed in the Wellnomics file.")
                    continue

                # sql_columns = ['Year', 'WorkWeek', 'MediumRiskEmployees', 'HighRiskEmployees', 'RepeatHighRiskEmployees', 'ModifiedDateTime']
                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_Wellnomics'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Wellnomics', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_Wellnomics']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif excel_file == 'ATS Wellnomics Data by Dept.xlsx':
                excel_sheet_name = 'Sheet1'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                df['Upload_Date'] = datetime.today()

                # Keep only the following columns
                keep_columns = ['Year', 'WorkWeek', 'Department', 'High Risk', 'Repeat High Risk', 'Upload_Date']
                try:
                    df = df.drop(df.columns.difference(keep_columns), axis=1)  # remove other columns (Power Query) python equivalent
                except KeyError:
                    log(False, project_name=project_name, data_area='Wellnomics by Area', row_count=0, error_msg="Column missing/changed in the Wellnomics file.")
                    continue
                finally:
                    df = df[keep_columns]  # reorder columns to match database table

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_Wellnomics_Dept'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Wellnomics by Area', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_Wellnomics_Dept']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Substrates Ramp Readiness' in excel_file:
                excel_sheet_name = 'Ramp Readiness'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                df['Upload_Date'] = datetime.today()

                # Keep only the following columns
                keep_columns = ['Product Readiness', 'Year', 'Quarter', 'Dashboard Comment', 'Notes', 'Risk', 'Color', 'Upload_Date']
                try:
                    df = df.drop(df.columns.difference(keep_columns), axis=1)  # remove other columns (Power Query) python equivalent
                except KeyError:
                    log(False, project_name=project_name, data_area='Substrates Ramp Readiness', row_count=0, error_msg="Column missing/changed in the Substrates Ramp Readiness file.")
                    continue

                #print(df.columns)
                sql_columns = ['ProductReadiness', 'Year', 'Quarter', 'DashboardComment', 'Notes', 'Risk', 'Color', 'ModifiedDateTime']

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_RR_Sub'], data=df, columns=sql_columns, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Substrates Ramp Readiness', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_RR_Sub']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Substrates Operations' in excel_file:
                sheets = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), header_row=0)
                insert_succeeded = False
                for excel_sheet_name in sheets:
                    df = sheets[excel_sheet_name]
                    if excel_sheet_name == 'Upside-Availability Support':
                        table = params['Table_ATS_UA_Sub']
                        df['Upload_Date'] = datetime.today()
                    elif excel_sheet_name == 'Supplier Transparency PIYL':
                        table = params['Table_ATS_Supl_Trans_Sub']
                        df = df.melt(id_vars=['Year', 'Month'], var_name='Suppliers')
                        df['Month'].replace({'Goal': None}, inplace=True)
                    elif excel_sheet_name == 'Supplier Internal Excursions':
                        table = params['Table_ATS_Excursions_Sub']
                        df = df.melt(id_vars=['Year', 'Quarter'], var_name='Suppliers')
                        df['Quarter'].replace({'Goal': None}, inplace=True)
                    else:
                        continue
                    # print(df)
                    insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True, driver="{SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='Substrates Operations', row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], table))
                if insert_succeeded:
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'ATS Ramp Readiness' in excel_file:
                df_list = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file))

                df = df_list['Summary']
                df['Upload_Date'] = datetime.today()
                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_RR_Summary'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Ramp Readiness', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_RR_Summary']))

                df = df_list['ATM Details']
                df['Upload_Date'] = datetime.today()
                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_RR_ATM'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='ATM Details', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_RR_ATM']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Substrates POR Spending' in excel_file:
                excel_sheets = ['Actuals', 'Forecast']
                insert_succeeded = False
                for excel_sheet_name in excel_sheets:
                    df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                    df['Upload_Date'] = datetime.today()
                    insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_PP_' + excel_sheet_name], data=df, truncate=True, driver="{SQL Server}")
                    log(insert_succeeded, project_name=project_name, data_area='Piece Parts Spends ' + excel_sheet_name, row_count=df.shape[0], error_msg=error_msg)
                    if insert_succeeded:
                        print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_PP_' + excel_sheet_name]))
                if insert_succeeded:
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            ### Old Process commented out below ###
            # elif 'Piece Parts' in excel_file:
            #     year = 2020
            #     qtr = excel_file.split(' ')[2][:2]  # parse quarter from Excel file name
            #     qtr_map = {'Q1': {'0': 'DPOR', '1': 1, '2': 2, '3': 3},
            #                'Q2': {'0': 'MPOR', '1': 4, '2': 5, '3': 6},
            #                'Q3': {'0': 'JPOR', '1': 7, '2': 8, '3': 9},
            #                'Q4': {'0': 'SPOR', '1': 10, '2': 11, '3': 12}
            #                }
            #
            #     # Delete the previous data
            #     delete_statement = """DELETE FROM {0} WHERE [Month] IN ('{1}') AND [Year] = {2}""".format(params['Table_ATS_PP'], "','".join([str(x) for x in qtr_map[qtr].values()]), str(year))
            #     delete_success, error_msg = executeSQL(delete_statement)
            #     if not delete_success:
            #         log(delete_success, project_name=project_name, data_area='POR Piece Parts Spends', row_count=0, error_msg=error_msg)
            #     else:  # delete was successful
            #         excel_sheet_name = 'Detail'
            #         df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=4)
            #
            #         # Create new column key by concatenating multiple text columns
            #         cols = ['POR Name', 'Actual Name', 'Prod Category', 'Part Number']
            #         df['POR_key'] = df[cols].apply(lambda row: '::'.join(row.values.astype(str)), axis=1)
            #
            #         # Keep only the following columns
            #        keep_columns = ['POR_key', 'Assembly In', 'Unit \nCost', 'Spending', 'Assembly In.1',
            #                         'Unit \nCost.1', 'Spending.1', 'Assembly In.2', 'Unit \nCost.2',
            #                         'Spending.2', 'Assembly In.3', 'Unit \nCost.3', 'Spending.3']
            #         try:
            #             df = df.drop(df.columns.difference(keep_columns), axis=1)  # remove other columns (Power Query) python equivalent
            #         except KeyError:
            #             log(False, project_name=project_name, data_area='POR Piece Parts Spends', row_count=0, error_msg="Column missing/changed in the Piece Parts POR file.")
            #             continue
            #
            #         # Unpivot data table on POR Name, Actual Name, and Part Number
            #         df = df.melt(id_vars=['POR_key'], var_name='Attribute')  # melt is the unpivot method in python
            #
            #         # Add custom column to dataframe
            #         month_number = list()
            #         for _, row in df.iterrows():
            #             temp = getattr(row, 'Attribute')  # get the Attribute column value
            #             if '.' in temp:
            #                 temp = temp.split('.')[-1]
            #                 month_number.append(qtr_map[qtr][temp])
            #             else:
            #                 month_number.append(qtr_map[qtr]['0'])
            #         df['Month Number'] = month_number
            #
            #         # Replace values in Attribute column
            #         df.replace({'Unit \nCost': 'Unit Cost',
            #                     'Unit \nCost.1': 'Unit Cost',
            #                     'Unit \nCost.2': 'Unit Cost',
            #                     'Unit \nCost.3': 'Unit Cost',
            #                     'Assembly In.1': 'Assembly In',
            #                     'Assembly In.2': 'Assembly In',
            #                     'Assembly In.3': 'Assembly In',
            #                     'Spending.1': 'Spending',
            #                     'Spending.2': 'Spending',
            #                     'Spending.3': 'Spending',
            #                     }, inplace=True)
            #
            #         # Pivot again but this time by combine Attributes into a single column
            #         df_pivot = pd.pivot_table(df, values='value', index=['POR_key', 'Month Number'], columns=['Attribute'], aggfunc=np.sum).reset_index()
            #         df_pivot.index.name = df_pivot.columns.name = None
            #         df['Attribute'] = df['Attribute'].replace(0, np.nan)  # Fill empty cells correctly
            #         # print(df_pivot.columns)
            #         # print(df_pivot.head(10))
            #         # df_pivot['Spending'] = df_pivot['Spending'] * 1000  # multiply spending by 1000
            #
            #         df_pivot['Year'] = [str(year)] * len(df_pivot.index)  # create list of duplicate dates the same size as df
            #         df_pivot['Upload_Date'] =  [datetime.today()] * len(df_pivot.index)
            #
            #         # Undo concatenation
            #         df_pivot[['POR Name', 'Actual Name', 'Prod Category', 'Part Number']] = df_pivot.POR_key.apply(lambda x: pd.Series(str(x).split("::")))
            #         df_pivot = df_pivot[df_pivot['POR Name'] != 'nan']  # remove rows with empty POR Name
            #         df_pivot['Actual Name'] = df_pivot['Actual Name'].replace('0', np.nan)  # correct Actual Name column values
            #
            #         keep_columns = ['POR Name', 'Actual Name', 'Prod Category', 'Part Number', 'Year', 'Month Number',
            #                         'Assembly In', 'Spending', 'Unit Cost', 'Upload_Date']
            #         df_pivot = df_pivot[keep_columns]  # manually change column order to match database table
            #
            #         insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_PP'], data=df_pivot, truncate=False, driver="{SQL Server}")
            #         log(insert_succeeded, project_name=project_name, data_area='POR Piece Parts Spends', row_count=df.shape[0], error_msg=error_msg)
            #         if insert_succeeded:
            #             print('Successfully inserted {0} records into {1}'.format(df_pivot.shape[0], params['Table_ATS_PP']))
            #             successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
            #             renamed_files.append(excel_file)

            elif 'Low Cost GEO' in excel_file:
                excel_sheet_name = 'Sheet1'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                df['Upload_Date'] = datetime.today()

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_LCG'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Low Cost GEO Mix', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_LCG']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Upside Availability' in excel_file:
                excel_sheet_name = 'Sheet1'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)

                keep_columns = ['Year', 'Quarter', 'Support Type', 'Risk', 'Comment']
                try:
                    df = df.drop(df.columns.difference(keep_columns), axis=1)  # remove other columns (Power Query) python equivalent
                except KeyError:
                    log(False, project_name=project_name, data_area='Upside Availability', row_count=0, error_msg="Column missing/changed in the Upside Availability file.")
                    continue

                df['Upload_Date'] = datetime.today()
                print(df.columns)

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_UA'], data=df, truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Upside Availability', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_UA']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            if 'SRC - Due Dates' in excel_file:
                excel_sheet_name = 'Sheet1'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_SRC_Deadlines'], data=df, truncate=True, driver="{SQL Server}")
                if not insert_succeeded:
                    log(insert_succeeded, project_name=project_name, data_area='SRC Deadlines', row_count=0, error_msg=error_msg)
                else:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_SRC_Deadlines']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file)

    if successfully_loaded_files:  # load was successfully for at least one file
        for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
            try:
                shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
            except PermissionError:
                print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))

    successfully_loaded_files = []  # clear list variables before reading from second shared drive
    renamed_files = []
    shared_drive_folder_path = r"\\VMSOAPGSMSSBI06.amr.corp.intel.com\ATS_SCCI_Data"
    (_, _, file_list) = next(os.walk(shared_drive_folder_path))  # List all files (excluding folders) in directory
    for excel_file in file_list:
        if not excel_file.startswith('~'):  # ignore open files
            if 'Matrix' in excel_file:
                excel_sheet_name = 'FHR - Sector Sort'
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)

                try:
                    df.drop(columns=['Segmentation', 'Current FHR', 'Simulated FHR'], inplace=True)  # remove unnecessary columns
                    df.drop(df.tail(3).index, inplace=True)  # drop last three rows (comment in excel file)

                    pivoted_columns = list()
                    id_columns = list()
                    for col in df.columns:
                        if col.startswith('20'):
                            pivoted_columns.append(col)
                        else:
                            id_columns.append(col)
                    df = df.melt(id_vars=id_columns, value_vars=pivoted_columns, var_name='Year_Quarter', value_name='FHR')
                    # print(df.columns)

                    df.drop(df.columns.difference(['Unnamed: 0', 'Company', 'Intel/Subsidiary Name', 'Global Supplier ID',
                                                   'Ticker', 'Sector Name', 'Qtr', 'An', 'Ticker Exchange',
                                                   'Rating Period End*', 'Period End Date', 'PD (%)',
                                                   'Year_Quarter', 'FHR'
                                                   ]), axis=1, inplace=True)  # remove other columns (Power Query) python equivalent

                except KeyError:
                    log(False, project_name=project_name, data_area='Rapid Ratings AT FHR Matrix', row_count=0, error_msg="Column missing/changed in ATS Matrix.xlsx file.")
                    continue

                df['Upload_Date'] = datetime.today()

                sql_columns = ['FHRSupplierKey', 'FHRSupplierName', 'IntelSupplierName', 'GlobalSupplierESDID',
                               'SupplierStockTicker_Short', 'FHRSupplierSector', 'FHRDeltabyLastQuarter',
                               'FHRDeltabyLastYear', 'SupplierStockTicker_Long', 'FHRLastPeriodDescription',
                               'FHRLastPeriodEndDate', 'FHRDefaultProbability', 'Year_Quarter', 'FHR',
                               'ModifiedDateTime']

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_AT_FHR_Matrix'], data=df, columns=sql_columns, categorical=['Global Supplier ID'], truncate=True, driver="{SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area='Rapid Ratings AT FHR Matrix', row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_AT_FHR_Matrix']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            ### Old Process commented out below ###
            # elif 'Workforce Tracker' in excel_file:
            #     excel_sheet_name = 'Sheet1'
            #     df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
            #     df['Upload_Date'] = datetime.today()
            #
            #     insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_ATS_Workforce'], data=df, truncate=True, driver="{SQL Server}")
            #     log(insert_succeeded, project_name=project_name, data_area='Test Workforce Tracker', row_count=df.shape[0], error_msg=error_msg)
            #     if insert_succeeded:
            #         print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_ATS_Workforce']))
            #         successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
            #         renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

            elif 'Ratio Report' in excel_file:
                # initialize variables
                data_area = 'Rapid Ratings Ratio Report'
                file_path = 'https://intel.sharepoint.com/:x:/r/sites/supplierhealth/GSC1%20Matrix/ATS%20Ratio%20Report.xlsx?d=w5aa9ebbbbe91488898fc49a08f43fa26&csf=1&web=1&e=GfgQo9'
                excel_sheet_name = 'Ratio Extraction'

                # df = loadExcelFile(file_path, excel_sheet_name, header_row=0)
                df = loadExcelFile(os.path.join(shared_drive_folder_path, excel_file), excel_sheet_name, header_row=0)
                df.drop(columns=['Segmentation'], inplace=True)  # remove unnecessary column

                df.replace({'n/a': None}, inplace=True)  # replace "n/a" text values with None for upload to SQL

                for col in ['Current Period', 'Prior1 Period', 'Prior2 Period']:  # date columns
                    df[col] = pd.to_datetime(df[col], format='%m/%d/%y', errors='coerce')  # convert date fields to datetime objects

                df['Upload_Date'] = datetime.today()

                insert_succeeded, error_msg = uploadDFtoSQL(table=params['Table_AT_Ratio_Report'], data=df, categorical=['Global Supplier ID'], truncate=True, driver="{ODBC Driver 17 for SQL Server}")
                log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
                if insert_succeeded:
                    print('Successfully inserted {0} records into {1}'.format(df.shape[0], params['Table_AT_Ratio_Report']))
                    successfully_loaded_files.append(excel_file)  # add files to list of correctly loaded files
                    renamed_files.append(excel_file.split('.')[0] + '_' + datetime.today().strftime('%Y%m%d') + '.xlsx')

    if successfully_loaded_files:  # load was successfully for at least one file
        for i in range(len(successfully_loaded_files)):  # for all files that were successfully loaded into the database
            try:
                shutil.move(os.path.join(shared_drive_folder_path, successfully_loaded_files[i]), os.path.join(shared_drive_folder_path, 'Archive', renamed_files[i]))  # Move Excel file to Archive folder after it has been loaded successfully
            except PermissionError:
                print("{} cannot be moved to Archive because it is currently being used by another process.".format(os.path.join(shared_drive_folder_path, successfully_loaded_files[i])))

    print("--- %s seconds ---" % (time() - start_time))