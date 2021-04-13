#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 18:36:13 2021

@author: Gustavo
"""

import psycopg2
import pandas as pd
import numpy as np
import ujson


class DataBaseConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                host='junglescout.cusvdvuwccev.eu-west-3.rds.amazonaws.com',
                port=5432,
                user='gusvila',
                password='Panther1.',
                database='PostGresJSS2'
            )
            self.cursor = self.connection.cursor()
            print('Successfully connected to the database!')
        except:
            print('Couldnt connect to the database')

    def create_keyword_table(self):
        try:
            create_command = """CREATE TABLE keyword_table(
            keywordName TEXT NOT NULL PRIMARY KEY,
            score FLOAT8,
            matches FLOAT8,
            keywordId TEXT NOT NULL,
            exactSuggestedBidMedian FLOAT8,
            avgGiveaway FLOAT8,
            exactAvgCpc FLOAT8,
            exactSearchVolume FLOAT8,
            estimatedBroadSearchVolume FLOAT8,
            keywordCountry VARCHAR(10),
            quarterlyTrend FLOAT8,
            estimatedAvgGiveaway FLOAT8,
            easeOfRankingScore FLOAT8,
            broadSearchVolume FLOAT8,
            broadSuggestedBidMedian FLOAT8,
            keywordCategory TEXT,
            monthlyTrend FLOAT8,
            broadAvgCpc FLOAT8,
            estimatedExactSearchVolume FLOAT8,
            keyword_url TEXT,
            hasUpdatedSearchVolume BOOLEAN,
            hasUpdatedCpc BOOLEAN,
            organicProductCount FLOAT8,
            sponsoredProductCount FLOAT8 );"""

            self.cursor.execute(create_command)
            self.connection.commit()
            print('Table created succesfully!')
        except Exception as error:
            print('Error trying to create table: {}'.format(error))

        # self.connection.close()

        # self.cursor.close()

    #   cur.executemany(
    # """INSERT INTO "%s" (data) VALUES (%%s)""" % (args.tableName),rows)

    def create_suppliers_database_table(self):
        try:
            create_command = """CREATE TABLE suppliers_database_table(
                 supplierName TEXT NOT NULL, 
                 experience FLOAT8,
                 supplierUrl TEXT,
                 imageUrl TEXT, 
                 factoryTag TEXT,
                 nTransaction FLOAT8,
                 price FLOAT8,
                 minimumOrderQuantity FLOAT8,
                 nEmployees INT8RANGE,
                 plantArea FLOAT8,
                 supportProofing TEXT,
                 certifQualification TEXT,
                 certifQuality TEXT,
                 phoneNumber FLOAT8,  
                 parentKeyword TEXT NOT NULL,
                 totalNOPforKeyword INT NOT NULL,
                 FOREIGN KEY (parentKeyword) REFERENCES keyword_table (keywordName));"""
            # number of employees: INT8RANGE working, look with the constraints on it ?

            self.cursor.execute(create_command)
            self.connection.commit()
            print('Table created succesfully!')
        except Exception as error:
            print('Error trying to create table: {}'.format(error))

    def insert_rec(self):
        try:
            command_line = '''INSERT INTO keyword_table(name, age) VALUES ('bana', 10.8);'''
            self.cursor.execute(command_line)
            self.connection.commit()
        except Exception as error:
            print('Error occured: {}'.format(error))

    def insert_new_record_from_csv(self, table_name, path_csv):
        try:
            # with open(path_csv, 'r') as csv_file:
            #    self.cursor.copy_from(csv_file, table_name, sep='\t', null='')
            with open(path_csv, 'r') as csv_file:
                command_line = '''COPY {} FROM STDIN WITH (FORMAT CSV, HEADER false, DELIMITER '\t', NULL '', ENCODING 'utf-8'); '''.format(
                    table_name)
                self.cursor.copy_expert(command_line, csv_file)
                self.connection.commit()
            print('Values inserted in {}'.format(table_name))
        except Exception as error:
            print('Error trying to insert values: {}'.format(error))

    def join_js_tables(self):
        command_line = '''SELECT * FROM suppliers_database_table JOIN keyword_table ON suppliers_database_table.parentkeyword = keyword_table.keywordname;'''
        self.cursor.execute(command_line)
        return self.cursor.fetchall()

    def query_data(self, table_name):
        # command_line = '''SELECT * FROM {} WHERE keywordName = 'face masks skincare';'''.format(table_name)
        command_line = '''SELECT * FROM {};'''.format(table_name)
        self.cursor.execute(command_line)
        return self.cursor.fetchall()

    def delete_table(self, table_name):
        try:
            command_line = '''DROP TABLE {}'''.format(table_name)
            self.cursor.execute(command_line)
            self.connection.commit()
            print('Table {} deleted'.format(table_name))
        except:
            pass

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        self.connection.close()


FILTERS = {
    'experience': lambda df: (df['experience'] > 3),
    'factoryTag': lambda df: (df['factoryTag'] == 'factory'),
    'nTransaction': lambda df: (df['nTransaction'] > 20),
    'Price': lambda df: (df['price'] >= 15) & (df['price'] <= 200),
    'minimumOrderQuantity': lambda df: (df['minimumOrderQuantity'] > 50),
    'nEmployees': lambda df: (df['nEmployees'] >= 50) & (df['nEmployees'] <= 500),
    'plantArea': lambda df: (df['plantArea'] >= 200),
    'supportProofing': lambda df: (df['supportProofing'] == 'yes'),
    'certifQualification': lambda df: (df['certifQualification'] != '')
    'certifQuality': lambda df: (df['certifQuality'] != '')

}
# "yes" = "是" and "factory" = "工厂" in chinese, do we need to put chinese instead of english ?


def filter_dataframe(dataframe, dicFilter):
    df = dataframe[dicFilter['experience'](dataframe) & dicFilter['factoryTag'](dataframe) & dicFilter['nTransaction'](dataframe)
                    & dicFilter['Price'](dataframe) & dicFilter['minimumOrderQuantity'](dataframe) & dicFilter['nEmployees'](dataframe)
                    & dicFilter['plantArea'](dataframe) & dicFilter['supportProofing'](dataframe) & dicFilter['certifQualification'](dataframe)
                    & dicFilter['certifQuality'](dataframe)]
    return df


if __name__ == "__main__":
    database = DataBaseConnection()
    # database.create_keyword_table()
    # database.create_suppliers_database_table()

    # database.create_keyword_table()
    # database.create_suppliers_database_table()
    # database.insert_rec()
    # database.insert_new_record_from_csv('keyword_table','/Users/Gustavo/.spyder-py3/keywords_0_to_160.csv')
    # database.insert_new_record_from_csv('suppliers_database_table',
    #                                    '/Users/Gustavo/.spyder-py3/products_106500_to_120500.csv')
    # x = database.query_data('keyword_table')
    # sql = '''SELECT * FROM suppliers_database_table JOIN keyword_table ON suppliers_database_table.parentkeyword = keyword_table.keywordname;'''
    # x = pd.read_sql_query(sql, database.connection)
    # f = filter_dataframe(x, FILTERS)
    # y = database.query_data('suppliers_database_table')
    database.close()

