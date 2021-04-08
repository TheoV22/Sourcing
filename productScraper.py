#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 14:24:45 2021
@author: Gustavo
"""

import requests
import time
import re
import ast
import numpy as np
import pandas as pd
import asyncio
import concurrent.futures
import nest_asyncio
import os.path
import random
from collections import defaultdict
import ujson
import functools
import csv

SIGN_IN_URL = 'https://api.junglescout.com/users/sign_in'
KEYWORD_URL = 'https://api.junglescout.com/api/keyword_engine/get_related_keywords'
SUPPLIERS_URL = 'https://api.junglescout.com/api/suppliers'
PRODUCT_DATABASE_URL = 'https://api.junglescout.com/api/products/get_products'

SIGN_IN_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36',
    'content-type': 'application/json'
}

SIGN_IN_PAYLOADS = {
    "email": "charlie.hung@rocket-internet.com.au",
    "password": "Panther1.",
    "mixpanelId": "177813702f35ed-0726405ac4c2a8-163d6558-fa000-177813702f4663"
}

headers = {
    'authority': 'api.junglescout.com',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'accept': '*',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTg0Nzk2NTUsImlhdCI6MTYxNzg3NDg1NSwiaXNzIjoianVuZ2xlc2NvdXRfYXBpIiwiYXVkIjoiY2xpZW50IiwiYXV0aF90b2tlbiI6IjIxNmQ1NjE4NTYzYjA5MDEyOGFiMTI4NWM0OGRmYWUwIn0.uGCsLJZ9ZpEE53AfB6zGvRI-iqB14drDMuf_T429OwQ',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
    'content-type': 'application/json',
    'origin': 'https://members.junglescout.com',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://members.junglescout.com/',
    'accept-language': 'en-US,en;q=0.9',
    'if-none-match': 'W/"a7c3f3a0930435f5c07e8ee0ca59b11c"',
}

params = {
    'keyword':
        {
            'data[shingles][type]': 'shingles',
            'data[shingles][column]': 'name',
            'data[shingles][searchTerm]': 'makeup',
            'data[category][type]': 'terms',
            'data[category][valuesArray][]': ['Appliances', 'Arts, Crafts & Sewing', 'Automotive', 'Baby',
                                              'Beauty & Personal Care', 'Camera & Photo', 'Cell Phones & Accessories',
                                              'Clothing, Shoes & Jewelry', 'Computers & Accessories', 'Electronics',
                                              'Grocery & Gourmet Food', 'Health & Household', 'Home & Kitchen',
                                              'Industrial & Scientific', 'Kitchen & Dining', 'Musical Instruments',
                                              'Office Products', 'Patio, Lawn & Garden', 'Pet Supplies', 'Software',
                                              'Sports & Outdoors', 'Tools & Home Improvement', 'Toys & Games',
                                              'Video Games'],
            'data[country][type]': 'terms',
            'data[country][valuesArray][]': 'us',
            'data[wordCount][type]': 'range',
            'data[wordCount][min]': '0',
            'data[wordCount][max]': '999',
            'data[isComplete][type]': 'terms',
            'data[isComplete][valuesArray][]': 'true',
            'data[state][type]': 'terms',
            'data[state][valuesArray][]': 'active',
            'data[estimatedExactSearchVolume][type]': 'range',
            'data[estimatedExactSearchVolume][min]': '0',
            'data[estimatedExactSearchVolume][max]': '9999999',
            'data[estimatedBroadSearchVolume][type]': 'range',
            'data[estimatedBroadSearchVolume][min]': '1',
            'data[estimatedBroadSearchVolume][max]': '9999999',
            'data[organicProductCount][type]': 'range',
            'data[organicProductCount][min]': '1',
            'data[organicProductCount][max]': '9999999',
            'data[sort][type]': 'sort',
            'data[sort][column]': 'estimatedExactSearchVolume',
            'data[sort][direction]': 'desc',
            'data[paginate][type]': 'paginate',
            'data[paginate][pageSize]': '8000',
            'skipCounter': 'false',
            'excludeTopBrands': 'false',
        },
    'supplier':
        {
            'data[paginate][type]': 'paginate',
            'data[paginate][pageSize]': '10',
            'data[productDesc][type]': 'match',
            'data[productDesc][valuesArray][]': 'makeup organizer',
            'data[productDesc][minShouldMatch]': '1',
            'skipCounter': 'false',
            'excludeTopBrands': 'false'
        },
    'product database':
        {
            'data[listedAt][type]': 'multiFieldsRange',
            'data[listedAt][primaryField]': 'listedAt',
            'data[listedAt][secondaryField]': 'estimatedListedAt',
            'data[query][type]': 'query',
            'data[query][searchTerm]': 'tennis knee sleeve',
            'data[query][queryFields][]': ['name', 'brand', 'asin'],
            # 'data[calculatedCategory][type]': 'terms',
            # 'data[calculatedCategory][valuesArray][]': ['Appliances', 'Arts, Crafts & Sewing', 'Automotive', 'Baby', 'Beauty & Personal Care', 'Camera & Photo', 'Cell Phones & Accessories', 'Clothing, Shoes & Jewelry', 'Computers & Accessories', 'Electronics', 'Grocery & Gourmet Food', 'Health & Household', 'Home & Kitchen', 'Industrial & Scientific', 'Kitchen & Dining', 'Musical Instruments', 'Office Products', 'Patio, Lawn & Garden', 'Pet Supplies', 'Software', 'Sports & Outdoors', 'Tools & Home Improvement', 'Toys & Games'],
            'data[country][type]': 'terms',
            'data[country][valuesArray][]': 'us',
            'data[sort][type]': 'sort',
            'data[sort][column]': 'name',
            'data[sort][direction]': 'asc',
            'data[paginate][type]': 'paginate',
            'data[paginate][pageSize]': '100',
            'data[paginate][from]': '0',
            'data[isUnavailable][type]': 'terms',
            'data[isUnavailable][valuesArray][]': 'false',
            # 'data[price][type]': 'range',
            # 'data[price][min]': '0',
            # #'data[price][max]': '9999',
            # 'data[net][type]': 'range',
            # 'data[net][min]': '0',
            # #'data[net][max]': '9999',
            # 'data[rank][type]': 'range',
            # 'data[rank][min]': '0',
            # #'data[rank][max]': '9999',
            # 'data[estimatedSales][type]': 'range',
            # 'data[estimatedSales][min]': '0',
            # #'data[estimatedSales][max]': '9999',
            # 'data[estRevenue][type]': 'range',
            # 'data[estRevenue][min]': '0',
            # #'data[estRevenue][max]': '9999',
            # 'data[nReviews][type]': 'range',
            # 'data[nReviews][min]': '0',
            # #'data[nReviews][max]': '9999',
            # 'data[rating][type]': 'range',
            # 'data[rating][min]': '0',
            # #'data[rating][max]': '9999',
            # 'data[weight][type]': 'range',
            # 'data[weight][min]': '0',
            # #'data[weight][max]': '9999',
            # 'data[nSellers][type]': 'range',
            # 'data[nSellers][min]': '0',
            # #'data[nSellers][max]': '9999',
            # 'data[listingQualityScore][type]': 'range',
            # 'data[listingQualityScore][min]': '0',
            # #'data[listingQualityScore][max]': '9999',
            'data[isComplete][type]': 'terms',
            'data[isComplete][valuesArray][]': 'true',
            'data[state][type]': 'terms',
            'data[state][valuesArray][]': 'active',
            'skipCounter': 'true',
            'excludeTopBrands': 'false',
            'collapseParent': 'true',
        }
}

list_keys = {
    'keyword': {
        'name': [],
        'score': [],
        'matches': [],
        'id': [],
        'exactSuggestedBidMedian': [],
        'avgGiveaway': [],
        'exactAvgCpc': [],
        'exactSearchVolume': [],
        'estimatedBroadSearchVolume': [],
        'country': [],
        'quarterlyTrend': [],
        'estimatedAvgGiveaway': [],
        'easeOfRankingScore': [],
        'broadSearchVolume': [],
        'broadSuggestedBidMedian': [],
        'category': [],
        'monthlyTrend': [],
        'broadAvgCpc': [],
        'estimatedExactSearchVolume': [],
        'keyword_url': [],
        'hasUpdatedSearchVolume': [],
        'hasUpdatedCpc': [],
        'organicProductCount': [],
        'sponsoredProductCount': []
    },
    'supplier': {
        'name': [],
        'supplier_id': [],
        'location': [],
        'customers': [],
        'score': [],
        'latest_shipment': [],
        'products_manufactured': [],
        'total_shipments': []
    },
    'product database': {
        'name': [],
        'id': [],
        # 'keyword': [],
        # 'Exact Search Volume': [],
        'nReviews': [],
        'estimatedSales': [],
        'country': [],
        'weight': [],
        'weightUnit': [],
        'state': [],
        'apiUpdatedAt': [],
        'imageUrl': [],
        'fees': [],
        'subCategory': [],
        'subCategories': [],
        'width': [],
        'dimensions': [],
        'dimensionUnit': [],
        'categoryNullifiedAt': [],
        'estRevenue': [],
        'scrapedAt': [],
        'rating': [],
        'tier': [],
        'hasVariants': [],
        'rawCategory': [],
        'sellerName': [],
        'nSellers': [],
        'dimensionValuesDisplayData': [],
        'category': [],
        'isUnavailable': [],
        'listingQualityScore': [],
        'sellerType': [],
        'listedAt': [],
        'estimatedListedAt': [],
        'length': [],
        'noParentCategory': [],
        'isSharedBSR': [],
        'color': [],
        'calculatedCategory': [],
        'asin': [],
        'brand': [],
        'scrapedParentAsin': [],
        'multipleSellers': [],
        'rank': [],
        'pageAsin': [],
        'height': [],
        'price': [],
        'apiCategory': [],
        'net': [],
        'feeBreakdown': [],
        'variantAsinsCount': [],
        'sampleVariants': [],
        'product_url': [],
        'bsr_product': [],
        'hasRankFromApi': [],
        'currency_code': [],
        'Parent Keyword': [],
        'Total NOP for the keyword': []
    }
}


def file_exists(file_name):
    return os.path.isfile(file_name)


def get_bearer_token():
    # g = get_proxies()
    # indice = random.randint(0, len(g)-1)
    # print(indice)
    # proxy = g[2]
    # response = requests.post(SIGN_IN_URL, headers=SIGN_IN_HEADERS, params=SIGN_IN_PAYLOADS, proxies={'http': proxy, 'https': proxy}, timeout=60)
    response = requests.post(SIGN_IN_URL, headers=SIGN_IN_HEADERS, params=SIGN_IN_PAYLOADS, timeout=30)
    # print(response.text)
    dic_str = response.text
    token = ' '.join(['Bearer', dic_str.split('token')[1].split('"')[2]])
    # print(token)
    return token


def get_params(keyword, key, subkey, startFrom=None):
    if isinstance(keyword, str):
        params[key][subkey] = keyword
        if startFrom is not None:
            params[key]['data[paginate][from]'] = startFrom
        return params[key]
    else:
        raise ValueError('the keyword needs to be a string!')


def js_request(keyword, url=KEYWORD_URL, key='keyword', subkey='data[shingles][searchTerm]', startFrom=None):
    params = get_params(keyword, key, subkey, startFrom)
    # g = get_proxies()
    # random.seed(1)
    # indice = random.randint(0, len(g)-1)
    # print(indice)
    # proxy = g[2]
    for i in range(9):
        try:
            # page = requests.get(url, headers=headers, proxies={'http': proxy, 'https': proxy}, params=params, timeout=60)
            page = requests.get(url, headers=headers, params=params, timeout=20)
            page.raise_for_status()
        except requests.exceptions.Timeout as etmout:
            if i == 4:
                pass  # raise SystemExit(etmout)
            print('Request timed out. Trying to request page again. Try #{} of 4'.format(i + 1))
            time.sleep(1)
        except requests.HTTPError as ehttp:
            if ehttp.response.status_code == 429 and i != 4:
                print('Reached request limit. Wait for 1 h.')
                time.sleep(3600)
                print('Requesting for page again. Try #{} of 4'.format(i + 1))
            elif ehttp.response.status_code == 401:
                print('The Bearer token is invalid. Getting a new one...')
                raise SystemExit(ehttp)
                # try:
                #    headers['authorization'] = get_bearer_token()
                # print(headers['authorization'])
                # except:
                #   pass
            else:
                print(ehttp)  # pass#raise SystemExit(ehttp)
        except requests.exceptions.RequestException as err:
            print(err)  # pass#raise SystemExit(err)
            break
        else:
            # self.requestQueue.append(datetime.now())
            return page.text


def transform_list_of_dicts(text, data_type='keyword', appendKeyword=None):
    # print(type(text))
    # print(text)
    if not isinstance(text, str):
        print('Page text returned belongs to {}'.format(type(text)))
        return []
    text = re.sub(r'(?<=:)null', r'-99999', text)
    if data_type.lower() == 'keyword':
        text = re.sub(r'(?<=:)true', r'True', text)
        text = re.sub(r'(?<=:)false', r'False', text)
        new_text = re.split(r',(?={"score")', text)
        new_text[0] = re.split(r'\[(?={"score")', new_text[0])[1]
        new_text[-1] = new_text[-1].split(']')[0]
    elif data_type.lower() == 'supplier':
        text = re.sub(r'"total_shipments":\d+\}(?=\]|,)', '\g<0> !split!', text)
        new_text = text.split('!split!,')
        new_text[-1] = new_text[-1].split('!split!]')[0]
        new_text[0] = new_text[0].split('"data":[')[1]
    elif data_type.lower() == 'product database':
        text = re.sub(r'(?<=:)true', r'True', text)
        text = re.sub(r'(?<=:)false', r'False', text)
        total, new_text = text.split('"products":[')
        new_text = new_text.split('],"categories"')[0]
        new_text = re.split(r',(?={"id")', new_text)
        total_count = int(re.search(r'(?<="total_count":)\d+', total).group(0))
    list_of_dicts = []
    for rep in new_text:
        try:
            data_to_append = ast.literal_eval(rep)
            if appendKeyword is not None:
                data_to_append['Parent Keyword'] = appendKeyword
                data_to_append['Total NOP for the keyword'] = total_count
            list_of_dicts.append(data_to_append)
        except:
            pass
    return list_of_dicts


def build_list_of_dicts(keyword, url=KEYWORD_URL, key='keyword', subkey='data[shingles][searchTerm]'):
    if isinstance(keyword, list):
        if not keyword:
            raise ValueError('Keyword list must not be empty')
        else:
            ans_list = []
            list_dics = map(build_list_of_dicts, key)
            for element in list_dics:
                ans_list.extend(element)
            return ans_list
    print('requesting for {}'.format(keyword))
    if key in ['keyword', 'supplier']:
        try:
            data = js_request(keyword, url, key, subkey)
            return transform_list_of_dicts(data, data_type=key)
        except:
            return []
    elif key == 'product database':
        data = []
        for start in range(0, 100, 100):
            try:
                data.append(js_request(keyword, url, key, subkey, startFrom=start))
            except:
                pass
        ans = []
        if data:
            for rep in data:
                ans.extend(transform_list_of_dicts(rep, data_type=key, appendKeyword=keyword))
        return ans


nest_asyncio.apply()


async def concurrent_builder_dicts(taxonomy_words, key='keyword', m_workers=10):
    list_of_dicts = []
    # m = multiprocessing.Manager()
    # lock = m.Lock()
    with concurrent.futures.ProcessPoolExecutor(max_workers=m_workers) as executor:
        try:
            loop = asyncio.get_event_loop()
        except:
            pass
        try:
            if key == 'keyword':
                futures = [
                    loop.run_in_executor(executor, build_list_of_dicts, word)
                    for word in taxonomy_words
                ]
            elif key == 'product database':
                futures = [
                    loop.run_in_executor(executor,
                                         functools.partial(build_list_of_dicts, word, PRODUCT_DATABASE_URL, key,
                                                           'data[query][searchTerm]'))
                    for word in taxonomy_words
                ]
        except:
            pass
        for response in await asyncio.gather(*futures):
            try:
                # response.to_pickle("data_dictionary_of_keyword_{}.pkl".format(response[0]['id']))
                for element in response:
                    # if element not in list_of_dicts:
                    list_of_dicts.append(element)
                    # print(len(list_of_dicts))
            except:
                pass

    return list_of_dicts


def paralel_builder(taxonomy_words, key='keyword', m_workers=10):
    list_dict_keywords = []
    try:
        loop = asyncio.get_event_loop()
        list_dict_keywords = loop.run_until_complete(
            concurrent_builder_dicts(taxonomy_words, key=key, m_workers=m_workers))
    # if file_exists():
    #     with open("JSS_List_of_keywords.pkl", 'rb') as pickle_file:
    #         list_dict_keywords = pickle.load(pickle_file)
    #     return list_dict_keywords
    # else: return -1
    except:
        pass
    return list_dict_keywords


def remove_duplicates_from_list(lst_keywords, key='id'):
    d = defaultdict(list)
    final_lst = []
    for i in lst_keywords:
        d[i[key]].append(i)
    for i in d.values():
        final_lst.append(i[0])
    return final_lst


def save_file(data, file_name, method='json'):
    # func = {'ujson': ujson, 'json': json, 'pkl': pkl}
    way = 'w'
    if method == 'ujson':
        extension = 'json'
    else:
        extension = method
        if method in ['pkl', 'pickle']:
            way = 'wb'
    file_name = '.'.join([file_name, extension])
    with open(file_name, way) as file:
        ujson.dump(data, file)


def has_to_be_named2(taxonomy_word):
    N = 5
    temp_names = taxonomy_word
    list_of_keywords = []
    while len(list_of_keywords) < 10 ** 4:
        if (isinstance(temp_names, list) or isinstance(temp_names, np.ndarray)) and len(temp_names):
            try:
                iter_list = paralel_builder(temp_names)
                if not iter_list:
                    print('Error in getting new keywords! Trying again...')
                else:
                    list_of_keywords.extend(iter_list)
                    if len(iter_list) <= N:
                        indices = list(range(len(iter_list)))
                    else:
                        # random.seed(1)
                        indices = random.sample(range(0, len(iter_list) - 1), N)
                    iter_list = [iter_list[ind] for ind in indices]
                    temp_names = [d['name'] for d in iter_list]
            except:
                pass
        else:
            temp = build_list_of_dicts(temp_names)
            list_of_keywords.extend(temp)
            temp_names = [d['name'] for d in temp]
            # print(len(list_of_keywords))
    print('------------- Over ---------------')
    # file_name = "JSS_List_of_keywords_duplicates_{}.json".format(taxonomy_word)
    # with open(file_name, 'wb') as pickle_file:
    #    pickle.dump(list_of_keywords, pickle_file)  
    ans = remove_duplicates_from_list(list_of_keywords)
    file_name = ''.join(["Keywords/", "JSS_List_of_keywords_{}".format(taxonomy_word)])
    save_file(ans, file_name)
    return ans


def has_to_be_named_yet(keyword_list):
    N = 5
    # temp_names = taxonomy_word
    list_of_products = []
    if isinstance(keyword_list, str):
        list_of_products = paralel_builder(keyword_list, key='product database')
    elif isinstance(keyword_list, list) and keyword_list:
        for i in range(0, len(keyword_list), N):  # python handles all the problems this expression could have
            keyword_names = keyword_list[i:i + N]  # same
            try:
                iter_list = paralel_builder(keyword_names, key='product database')  # , proxy = list_proxies[i%10])
                if not iter_list:
                    print('Error in getting products containing the keywords {}!'.format(keyword_names))
                else:
                    list_of_products.extend(iter_list)
            except:
                pass
            # print(len(list_of_keywords))
    else:
        raise ValueError('The keywords provided need to be a string or a list of strings!')
    print('------------- Over ---------------')
    try:
        ans = remove_duplicates_from_list(list_of_products)
        # ans = remove_duplicates_from_list(ans, key = 'name')
    except:
        pass
    # file_name = "JSS_List_of_products_from_50520_to_57000"#.format(len(keyword_list))
    # save_file(ans, file_name)
    return ans


def transform_keyword_to_pd(list_of_data, sort_by='exactSearchVolume'):
    if not isinstance(list_of_data, list) or not list_of_data:
        raise ValueError('The data given must be a non-empty list')
    for element in list_of_data:
        list_to_null = np.setdiff1d(list(list_keys['keyword'].keys()), list(element.keys()))
        if len(list_to_null):
            for null_key in list_to_null:
                list_keys['keyword'][null_key].append(np.nan)
        for non_null_key, value in element.items():
            value_to_add = np.nan if isinstance(value, int) and value == -99999 else value
            list_keys['keyword'][non_null_key].append(value_to_add)
    temp_pd = pd.DataFrame(columns=list_keys['keyword'].keys())
    for key, value in list_keys['keyword'].items():
        if key in ['exactSearchVolume', 'broadSearchVolume']:
            temp_pd[key] = np.array(value).astype(np.int32)
        else:
            temp_pd[key] = np.asarray(value)

    return temp_pd.sort_values(by=sort_by, ascending=False)  # .set_index('name')


def transform_supplier_to_pd(list_of_data, sort_by='total_shipments'):
    if not isinstance(list_of_data, list) or not list_of_data:
        raise ValueError('The data given must be a non-empty list')
    for element in list_of_data:
        for non_null_key, value in element.items():
            list_keys['supplier'][non_null_key].append(value)
    temp_pd = pd.DataFrame(columns=list_keys['supplier'].keys())
    for key, value in list_keys['supplier'].items():
        if key == 'latest_shipment':
            temp_pd[key] = np.array(value).astype(np.int32)
        else:
            temp_pd[key] = np.asarray(value)

    return temp_pd.sort_values(by=sort_by, ascending=False).set_index('name')


def transform_product_database_to_pd(list_of_data, sort_by='net'):
    if not isinstance(list_of_data, list) or not list_of_data:
        raise ValueError('The data given must be a non-empty list')
    for element in list_of_data:
        list_to_null = np.setdiff1d(list(list_keys['product database'].keys()), list(element.keys()))
        if len(list_to_null):
            for null_key in list_to_null:
                list_keys['product database'][null_key].append(np.nan)
        for non_null_key, value in element.items():
            value_to_add = np.nan if isinstance(value, int) and value == -99999 else value
            list_keys['product database'][non_null_key].append(value_to_add)
    temp_pd = pd.DataFrame(columns=list_keys['product database'].keys())
    for key, value in list_keys['product database'].items():
        temp_pd[key] = np.asarray(value)

    return temp_pd.sort_values(by=sort_by, ascending=False)  # .set_index('name')


def get_proxies():
    list_proxies = []
    with open('proxies.csv', 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            list_proxies.append(row[0])
    return list_proxies


def main(petit_list):
    prods_list = has_to_be_named_yet(petit_list)
    return prods_list


if __name__ == "__main__":
    print('comecou')
    with open('Keywords/JSS_List_of_keywords_from_0_to_160.json', 'r') as json_file:
        keywords_list = ujson.load(json_file)
    print('keyword list loaded')
    for i in range(440500, 448500, 2000):
        petit_list = keywords_list[i:i + 2000]
        petit_list = [k['name'] for k in petit_list]
        print('begin of main')
        res = main(petit_list)
        file_name = "Products/JSS_List_of_products_from_{}_to_{}".format(i, i + 2000)
        save_file(res, file_name)
        print('Sleeping for 1000 s')
        time.sleep(1000)
    # res = js_request('baby')
#  g = get_proxies()
#  #p = get_params('baby', 'keyword', 'data[shingles][searchTerm]')
#  res = has_to_be_named2(['baby', 'disco ball', 'skin care', 'dog', 'toy'])
#  #res = paralel_builder(['baby', 'disco ball', 'skin care', 'dog', 'toy'], key = 'keyword', m_workers=10)
# #prods_list = main()
# if __name__ == "__main__":
#     print('comecou')
#     # with open('JSS_List_of_products_from_0_to_50000.json', 'r') as file:
#     #     products_list = ujson.load(file)
#     # print(len(products_list))
#     # with open('JSS_List_of_products_from_50000_to_50500.json', 'r') as file:
#     #     products_list.extend(ujson.load(file))
#     # # products_list = []
#     # print(len(products_list)) 
#     N = 196500
#     B = 192500
#     for i in range(B, N, 8000):
#         products_list = []
#         for j in range(i, min(i+4*2000,N), 2000):
#             file_name = "JSS_List_of_products_from_{}_to_{}.json".format(j, j+2000)
#             with open(file_name, 'r') as json_file: 
#                 products_list.extend(ujson.load(json_file))
#             print((j-i)/2000)
#         print('carregou')
#     #try:
#     #    products_list = remove_duplicates_from_list(products_list) 
#         #products_list = remove_duplicates_from_list(products_list, key = 'name')
#     #except:

#         #print('duplicates still on')
#         df_prod1 = transform_product_database_to_pd(products_list)
#         print('transformou')
#         try:
#             df_prod1 = df_prod1.drop_duplicates(subset=['id'])
#             print('ok')
#         except:
#             print('oops')
#         df_prod1.to_csv('products_{}_to_{}.csv'.format(i,j+2000), index=False, sep = '\t')
#         print((i-B)/8000)