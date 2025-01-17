from json import JSONDecodeError

import requests
import csv
import traceback
import os
from time import time, sleep
import json


def send_request(appId, params={'json': 1}):
    url = 'https://store.steampowered.com/appreviews/'
    response = requests.get(url=url + appId, params=params, headers={'User-Agent': 'Mozilla/5.0'})
    raw_text = response.text
    try:
        text_without_bom = raw_text.encode().decode('utf-8-sig')
        if text_without_bom == "":
            temp2 = json.loads("{}")
            temp2["success"] = 0
            return temp2
        return json.loads(text_without_bom)

    except JSONDecodeError:
        temp = json.loads("{}")
        temp["success"] = 0
        return temp


def get_all_reviews(appId):
    try:
        reviews = []
        cursor = "*"
        params = {
            'json': 1,
            'filter': 'recent',
            'language': 'all',
            'day_range': 9223372036854775807,
            'review_type': 'all',
            'purchase_type': 'all',
            'filter_offtopic_activity': '0'
        }
        thousand_counts = 0
        count = 0
        while True:
            sleeptimer = 0
            fail_count = 0
            params['cursor'] = cursor.encode()
            params['num_per_page'] = 100
            response = send_request(str(appId), params)

            while 'success' in response and response['success'] != 1:
                fail_count += 1
                sleeptimer += 1
                sleep(sleeptimer)
                response = send_request(str(appId), params)
                if fail_count > 10:
                    break

            if fail_count > 10:
                log_file.write(f'fail-count > 10 for {appId} at {count}:\n')
                break
            if 'success' not in response:
                log_file.write(f'success not in response for {appId} at {count}:\n')
                break
            count += response['query_summary']['num_reviews']
            cursor = response['cursor']
            reviews += response['reviews']
            sleep(1)
            if count >= thousand_counts + 1000:
                thousand_counts = count
                log_file.write(f'got {count} reviews\n')
            log_file.flush()
            # The above call to flush is not enough to write it to disk *now*;
            # according to https://stackoverflow.com/a/41506739/257924 we must
            # also call fsync:
            os.fsync(log_file)
            if len(response['reviews']) == 0:
                if 'cursor' in params:
                    params['cursor'] = cursor.encode()
                    params['num_per_page'] = 100
                    response = send_request(str(appId), params)
                    if 'success' in response and response['success'] == 1:
                        if response['query_summary']['num_reviews'] == 0:
                            break
                    else:
                        reviews += response['reviews']
                        count += response['query_summary']['num_reviews']
                        cursor = response['cursor']
                        reviews += response['reviews']
                        sleep(1)
                        if count > thousand_counts + 1000:
                            thousand_counts = count
                            log_file.write(f'got {count} reviews\n')
                        log_file.flush()
                        # The above call to flush is not enough to write it to disk *now*;
                        # according to https://stackoverflow.com/a/41506739/257924 we must
                        # also call fsync:
                        os.fsync(log_file)

        log_file.write('expected ' + str(count) + ' reviews | ')
        log_file.write('got ' + str(len(reviews)) + ' reviews\n')
        log_file.write('final params:\n' + str(params) + '\n')
        return reviews
    except KeyboardInterrupt:
        exit(1)
    except Exception as error2:
        print(f'failed for {appIdTemp} with {traceback.format_exception(error2)}\n')
        log_file.write(f'failed for {appIdTemp} with {traceback.format_exception(error2)}\n')
        log_file.write(str(params) + '\n')
        return reviews


def extract_useful_info(reviews):
    filtered = {}
    i = 0
    for review in reviews:
        num = {'steamid': review['author']['steamid'],
               'voted_up': review['voted_up'],
               'timestamp_created': review['timestamp_created'],
               'timestamp_updated': review['timestamp_updated']}
        filtered[i] = num
        i += 1
    return filtered


def write_reviews_to_csv(appId, reviews):
    file = open('reviews/' + str(appId) + '.csv', 'w', newline='')
    writer = csv.writer(file)
    writer.writerow([appIdTemp])
    writer.writerow(['steamid', 'voted_up', 'timestamp_created', 'timestamp_updated'])
    for i in reviews:
        writer.writerow([reviews[i]['steamid'], reviews[i]['voted_up'], reviews[i]['timestamp_created'],
                         reviews[i]['timestamp_updated']])


# appIdList = [2893970]
# 2893970 the new sheriff - 6 review, one in turkish
# 3092660 reverse 1999 weeb shit
# 268910 cuphead
# 703080 planet zoo
# 1272320/Diplomacy_is_Not_an_Option
# 3070070/TCG_Card_Shop_Simulator/
# 2314160/Tactical_Assault_VR
# 1063730, 1150690, 1151340, 1250410, 1284410,
tempList = [570]
appIdList = [1361210, 1407200, 1446780, 1462040, 1465360, 1517290,
             1794680, 211820, 221380, 22320, 233860, 244850, 261550, 268910, 284160,
             289070, 291550, 292030, 311690, 346110, 400, 457140, 47890, 49520, 529340, 544920, 552990,
             588650, 620, 620980, 703080, 730, 739630, 812140, 813780, 8500, 899770, 962130, 730, 570]
for appIdTemp in tempList:
    try:
        log_file = open(f'logs/{appIdTemp}.log', 'a', newline='')
        log_file.write(f'|||||getting reviews for {appIdTemp}|||||:\n')
        time_start = time()
        reviewList = get_all_reviews(appIdTemp)
        time_end_review_gathering = time()
        newList = extract_useful_info(reviewList)
        time_end_cleaning = time()
        write_reviews_to_csv(appIdTemp, newList)
        time_end_writing = time()
        log_file.write(
            f'times for {appIdTemp} were: \n'
            f' {time_end_review_gathering - time_start:.3f} seconds for reviews\n'
            f' {time_end_cleaning - time_end_review_gathering:.3f} seconds for cleaning\n'
            f' {time_end_writing - time_end_cleaning:.3f} seconds for writing\n')
        sleep(5)

    except KeyboardInterrupt:
        exit(1)
    except Exception as error:
        log_file.write(f'failed for {appIdTemp} with {traceback.format_exception(error)}\n')
