# ===============================================================================================
# Program to read Zoom data from the API and write it into a format that is good for analysis
# ===============================================================================================

# -----------------------------
# Imports
# -----------------------------
from typing import Dict, Any

import requests
from bs4 import BeautifulSoup
import json
import re
import csv
# import datetime
from datetime import datetime
from datetime import timedelta
import time
# import os.path
from os import path
from urllib.parse import quote

# -----------------------------
# Constants
# -----------------------------

sleep_1 = 2  # Outer sleep: for API limiter
sleep_2 = 2  # Inner sleep

from authorization import headers

regex_whitelist = "([^a-zA-Z0-9 \(]*)"
# regex_end_string = '[^a-zA-Z]'

datetime_format = '%Y-%m-%dT%H:%M:%SZ'
date_format = '%Y-%m-%d'
# https://docs.python.org/3/library/time.html#time.strftime
# datetime.fromisoformat + 'Z' at end
# Examples: 2021-06-23T22:49:42Z	2021-06-23T23:59:17Z

is_present_seconds = 5 * 60  # 5 minutes
is_present_delta = timedelta(seconds=is_present_seconds)

# datetime_object = datetime.fromisoformat('2021-06-23T22:49:42')
# datetime_object = datetime.strptime('2021-06-23T22:49:42Z', datetime_format)
# datetime_object2 = datetime.strptime('2021-06-23T23:59:17Z', datetime_format)
# print(datetime_object, datetime_object2, (datetime_object2-datetime_object).total_seconds(), is_present_seconds)
# exit()

page_size = '&page_size=300'  # &

file_suffix = '.csv'

data_file_prefix = 'lifering_zoom_part_data'
data_header_file_prefix = 'lifering_zoom_part_data_header'
data_file_suffix = file_suffix

event_file_prefix = 'lifering_zoom_event_data'
event_header_file_prefix = 'lifering_zoom_event_header'
event_file_suffix = file_suffix

# -----------------------------
# Program Macro State
# -----------------------------

participant_data_dict: Dict[Any, Any] = {}
participant_clean_data_dict: Dict[Any, Any] = {}

event_data_dict: Dict[Any, Any] = {}


# -----------------------------
# Main data writer
# Puts all the accumulated new data into 'data' and 'event' files
# -----------------------------

def write_files(infix, event_data_dict_p, participant_clean_data_dict_p):
    """
    :type infix: String
    :type event_data_dict_p: hash
    :type participant_clean_data_dict_p: hash
    """
    data_file_name = data_file_prefix + infix + data_file_suffix
    event_file_name = event_file_prefix + infix + event_file_suffix

    print("Opening: " + event_file_name)
    csv_file1 = open(event_file_name, 'w')
    csv_writer1 = csv.writer(csv_file1)

    count = 0

    for key in sorted(event_data_dict_p):  # .keys().sort():
        event_data = event_data_dict_p[key]
        if count == 0:
            header = event_data.keys()
            csv_writer1.writerow(header)
            count += 1
        csv_writer1.writerow(event_data.values())
        event_data = event_data_dict_p[key]
        print('uuid: {}, topic: {}, start_time: {}'.format(key, event_data['topic'], event_data['start_time']))

    csv_file1.close()

    # -----------------------------

    csv_file2 = open(data_file_name, 'w')
    csv_writer2 = csv.writer(csv_file2)
    count = 0

    for key in sorted(participant_clean_data_dict_p):  # .keys().sort():

        participant_data_l = participant_clean_data_dict_p[key]
        events = participant_data_l['meetings']
        print('key: {}, event_count: {}'.format(key, str(len(events))))
        for event in events:
            print('    event: {}'.format(event))
            if count == 0:
                header = event.keys()
                csv_writer2.writerow(header)
                count += 1
            csv_writer2.writerow(event.values())

    csv_file2.close()


# -----------------------------

# -----------------------------
# Main data reader
# This works day-by-day to prevent too many entries
# Could also use the 'REST' flag of additional results, but I am lazy
# -----------------------------

# timedelta.total_seconds

# The range of months for the given year
year = 2021
month_start = 6
month_end = 8

# This is to enable exiting early after all the days are exhausted (assuming all days have some kind of meeting)
# Can be disabled near the 'break' and would simply not write files for days that have no data
found_any_data = False
for month in range(6, 8):
    for day in range(1, 32):  # 19,32
        date_name = "{:04d}".format(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day)
        try:
            date_dt = datetime.strptime(date_name, date_format)
        except ValueError:
            print("Skipping: " + str(date_name))
            continue

        data_file_date_name = data_file_prefix + '_' + date_name + data_file_suffix
        event_file_date_name = event_file_prefix + '_' + date_name + event_file_suffix

        participant_data_date_dict = {}
        participant_clean_data_date_dict = {}

        event_data_date_dict = {}

        # print ('Data file: '+data_file_date_name)
        if path.exists(data_file_date_name):
            print('File exists: ' + data_file_date_name)
            continue

        URL = 'https://api.zoom.us/v2/metrics/meetings?type=past&from=' + date_name + '&to=' + date_name
        print(URL)
        # URL = 'https://api.zoom.us/v2/metrics/meetings?type=past&from=2021-06-01'+str(day)+'to=2021-06-30'+str(day+1)
        time.sleep(sleep_1)

        page = requests.get(URL, headers=headers)
        # print(page)

        soup = BeautifulSoup(page.content, 'html.parser')
        print(soup)

        site_json = json.loads(soup.text)
        # print(site_json)
        found_data = False
        for meeting in site_json['meetings']:
            found_data = True
            uuid = meeting.get('uuid')
            # print(meeting)
            event_data_dict[uuid] = meeting
            event_data_date_dict[uuid] = meeting
            uuid_encode = str(uuid)
            if uuid_encode[0] == "/":
                uuid_encode = quote(quote(uuid_encode, safe=''), safe='')

            URL2 = 'https://api.zoom.us/v2/metrics/meetings/' + uuid_encode + '/participants?type=past' + page_size  # +'&from='+date_name+'&to='+date_name
            print(URL2)
            time.sleep(sleep_2)

            meeting_page = requests.get(URL2, headers=headers)
            if meeting_page.status_code == 404:
                print('Not Found.')
            elif meeting_page.status_code == 200:
                soup = BeautifulSoup(meeting_page.content, 'html.parser')
                # print(meeting_page)
                # print(soup)

                meeting_json = json.loads(soup.text)
                previous_leave_time='2021-01-01T00:00:00Z'
                for participant in meeting_json['participants']:
                    print(participant)
                    user_name = participant.get('user_name')
                    join_time = participant.get('join_time')  # 2021-06-23T22:49:42Z	2021-06-23T23:59:17Z
                    leave_time = participant.get('leave_time')
                    if leave_time is None:
                        leave_time = previous_leave_time
                    previous_leave_time = leave_time
                    ip_address = participant.get('ip_address')

                    # -----------------------------------------------------------

                    join_dt = datetime.strptime(join_time, datetime_format)
                    leave_dt = datetime.strptime(leave_time, datetime_format)
                    duration_delta = leave_dt - join_dt
                    if duration_delta < is_present_delta:
                        print("Ignored: " + user_name + " in: " + uuid + " delta: " + str(duration_delta))
                        continue
                    else:
                        print("Added:   " + user_name + " in: " + uuid + " delta: " + str(duration_delta))

                    clean_name = user_name
                    clean_name = re.sub(regex_whitelist, '', clean_name).strip()
                    spaces = [m.start() for m in re.finditer(r" ", clean_name)]
                    if len(spaces) > 1:
                        clean_name = clean_name[:spaces[1]]
                    parens = [m.start() for m in re.finditer(r"\(", clean_name)]
                    if len(parens) > 0:
                        clean_name = clean_name[:parens[0]]
                    clean_name = re.sub(' ', '', clean_name).lower()

                    data = {'user_name': user_name, 'clean_name': clean_name, 'ip_address': ip_address, 'uuid': uuid,
                            'join_time': join_time, 'leave_time': leave_time}
                    # print ('user_name: {}, spaces: {}, clean_name: {}'.format(user_name, spaces, clean_name))

                    # -----------------------------------------------------------

                    participant_data = participant_data_dict.get(user_name, {'user_name': user_name, 'meetings': []})
                    participant_data_dict[user_name] = participant_data
                    participant_data['meetings'].append(data)
                    # print(participant_data)

                    participant_data_date = participant_data_date_dict.get(user_name,
                                                                           {'user_name': user_name, 'meetings': []})
                    participant_data_date_dict[user_name] = participant_data_date
                    participant_data_date['meetings'].append(data)

                    participant_clean_data = participant_clean_data_dict.get(clean_name,
                                                                             {'clean_name': clean_name, 'meetings': []})
                    participant_clean_data_dict[clean_name] = participant_clean_data
                    participant_clean_data['meetings'].append(data)
                    # print(participant_clean_data)

                    participant_clean_data_date = participant_clean_data_date_dict.get(clean_name,
                                                                                       {'clean_name': clean_name,
                                                                                        'meetings': []})
                    participant_clean_data_date_dict[clean_name] = participant_clean_data_date
                    participant_clean_data_date['meetings'].append(data)
                print('Leave')
            print('Leave2')

        if found_data or not found_any_data:
            print(participant_data_date_dict)
            write_files('_' + date_name, event_data_date_dict, participant_clean_data_date_dict)
            found_any_data = found_data
        else:
            print('Date is after end of data: ' + date_name)
            break

write_files('_processed', event_data_dict, participant_clean_data_dict)

# ===============================================================================================
