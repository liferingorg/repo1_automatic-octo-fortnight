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
import traceback
import yaml

# -----------------------------
# Constants
# -----------------------------

sleep_1 = 2  # Outer sleep: for API limiter
sleep_2 = 2  # Inner sleep

# -----------------------------
# Secrets
# -----------------------------

from lifering_secrets import *

# -----------------------------

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
json_file_suffix = '.json'

# File Names
user_file_prefix = 'lifering_user_data'
meeting_file_prefix = 'lifering_meeting_schedule_data'
event_file_prefix = 'lifering_zoom_event_data'


# The range of months for the given year
year = 2022
month_start = 1
month_end = 13


# -----------------------------
# Program Macro State
# -----------------------------

participant_data_dict: Dict[Any, Any] = {}
participant_clean_data_dict: Dict[Any, Any] = {}

event_data_dict: Dict[Any, Any] = {}


# -----------------------------
# Test Area
# -----------------------------

foo = '''{
  "xza": {
   "zid": "819 2909 5082",
   "passcode": "800604",
   "url":"https://us02web.zoom.us/j/81929095082?pwd=VThaUi9mN0E4OVFQaXRtVi9ackxZUT09",
   "active": true
   },
   "private" : false,
   "email" : "foo@bar.com"
}'''
#print(foo)
#print(json.loads(foo))
#exit

# -----------------------------
# Main status loop
# -----------------------------

from datetime import timezone

now = datetime.now(timezone.utc)

current_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
print("Current Time =", current_time)


# -----------------------------
# Files
# -----------------------------

infix='_' +current_time #+ user_id+ '_'

#infix='_' + user_id+ '_'+current_time
meeting_file_name = meeting_file_prefix + infix + file_suffix
meeting_file_name_json = meeting_file_prefix + infix + json_file_suffix

print("Opening: " + meeting_file_name)
print("Opening: " + meeting_file_name_json)

csv_file = open(meeting_file_name, 'w')
csv_writer = csv.writer(csv_file)
json_file = open(meeting_file_name_json, 'w')
json_file.write('{"meetings":[\n')
count = 0
#                   current_time // del data['query_time'] #Get rid of the redundant 'query_time'

# -----------------------------
# Requests
# -----------------------------


URL = 'https://api.zoom.us/v2/users/'
print(URL)
users_page = requests.get(URL, headers=headers)
meeting_page = requests.get(URL, headers=headers)
if users_page.status_code == 404:
    print('Not Found.')
elif users_page.status_code == 200:
    soup = BeautifulSoup(users_page.content, 'html.parser')
    #print(soup)

    user_json = json.loads(soup.text)
    for user in user_json['users']:
        user_id = user.get('id')
        user_email =user.get('email')
        
        data = {'user_id': user_id, 'user_email': user_email}
        print(data)
        
        URL = 'https://api.zoom.us/v2/users/'+user_id+''
        print(URL)

        user_page = requests.get(URL, headers=headers)
        if user_page.status_code == 404:
            print('Not Found.')
        elif user_page.status_code == 200:
            soup = BeautifulSoup(user_page.content, 'html.parser')
            #print(soup)

            user_json = json.loads(soup.text)
            #infix='_' + user_id+ '_'+current_time
            #user_file_name = user_file_prefix + infix + file_suffix
            #print("Opening: " + user_file_name)




        URL = 'https://api.zoom.us/v2/users/'+user_id+'/settings'
        print(URL)
        settings_page = requests.get(URL, headers=headers)
        if settings_page.status_code == 404:
            print('Not Found.')
        elif settings_page.status_code == 200:
            soup = BeautifulSoup(settings_page.content, 'html.parser')
            #print(soup)

            settings_json = json.loads(soup.text)
            #user_file_name = user_file_prefix + infix + file_suffix
            #print("Opening: " + user_file_name)



        URL = 'https://api.zoom.us/v2/users/'+user_id+'/meetings?type=schedule'
        print(URL)

        meetings_page = requests.get(URL, headers=headers)
        if meetings_page.status_code == 404:
            print('Not Found.')
        elif meetings_page.status_code == 200:
            soup = BeautifulSoup(meetings_page.content, 'html.parser')
            # print(meeting_page)
            # print(soup)

            meetings_json = json.loads(soup.text)
            
            #========
            

            for meeting in meetings_json['meetings']:
                uuid = meeting.get('uuid')
                topic = meeting.get('topic')  # 2021-06-23T22:49:42Z    2021-06-23T23:59:17Z
                participants = meeting.get('participants')
                start_time = meeting.get('start_time')
                

                uuid_encode = quote(quote(uuid, safe=''), safe='')
                URL = 'https://api.zoom.us/v2/meetings/'+uuid_encode
                print(URL)
                meeting_page = requests.get(URL, headers=headers)
                if meeting_page.status_code == 404:
                    print('Not Found.')
                elif meeting_page.status_code == 200:
                    soup = BeautifulSoup(meeting_page.content, 'html.parser')
                    meeting_detail_json = json.loads(soup.text)
                    #print(meeting_detail_json)
                    join_url = meeting_detail_json.get('join_url')
                    agenda=meeting_detail_json.get ('agenda')
                    agenda_metadata = {}
                    agenda_description = ""
                    #xza={}
                    
                    if agenda is not None and len(agenda) > 0:
                        #Determine what is in the agenda

                        if "#!yaml" in agenda:
                            data_start = agenda.find("#!yaml")
                            agenda_description = agenda[0:data_start]
                            agenda_data_string = agenda[(data_start+6):-1]
                            try:
                                agenda_metadata = yaml.safe_load(agenda_data_string)
                                print('Parsed YAML agenda')
                                print(agenda_metadata)
                            except Exception:
                                print('Could not parse YAML agenda metadata')
                                print(agenda)
                                traceback.print_exc()


                        elif "{" in agenda:
                            data_start = agenda.find("{")
                            agenda_description = agenda[0:data_start]
                            agenda_data_string = agenda[data_start:-1]
                            
                            try:
                                agenda_metadata = json.loads(agenda_data_string)
                                print('Parsed JSON agenda metadata')
                                print(agenda_metadata)
                            except Exception:
                                print('Could not parse JSON agenda metadata')
                                print(agenda_data_string)
                                traceback.print_exc()
                        else:
                            agenda_description = agenda
                    
                    agenda_description=agenda_metadata.get('agenda_description','')
                    meeting_private=agenda_metadata.get('private','')
                    meeting_email=agenda_metadata.get('email','')
                    meeting_zxa=agenda_metadata.get('xza',{})

                    xza_active=meeting_zxa.get('active')
                    xza_zid=meeting_zxa.get('zid')
                    xza_url=meeting_zxa.get('url')
                    xza_passcode=meeting_zxa.get('passcode')
                    
                    meeting_settings = meeting_detail_json.get('settings')
                    mute_upon_entry = meeting_settings.get ('mute_upon_entry')
                    join_before_host = meeting_settings.get ('join_before_host')
                    waiting_room = meeting_settings.get ('waiting_room')
                    alternative_hosts = meeting_settings.get ('alternative_hosts')
                    #uuid='uuid'


                    data = {'query_time': current_time, 'user_id': user_id, 'user_email' : user_email, 'uuid': uuid, 'start_time': start_time,  'topic': topic, 'mute_upon_entry': mute_upon_entry, 'join_before_host': join_before_host, 'waiting_room' : waiting_room, 'join_url' : join_url,
                        'alternative_hosts' : alternative_hosts, 'agenda_description': agenda_description, 'xza_zid': xza_zid, 'xza_passcode':xza_passcode, 'xza_url':xza_url, 'xza_active':xza_active, 'meeting_private':meeting_private, 'meeting_email':meeting_email}
                    #print(data)
                    if count == 0:
                        header = data.keys()
                        csv_writer.writerow(header)
                    else:
                        json_file.write(",\n")
                    csv_writer.writerow(data.values())
                    del data['query_time'] #Get rid of the redundant 'query_time'
                    json.dump(data, json_file, indent=4)
                    json_file.write("\n")
                    count += 1

                uuid_encode = quote(quote(uuid, safe=''), safe='')
                URL = 'https://api.zoom.us/v2/meetings/'+uuid_encode+'/survey'
                #print(URL)
                api_page = requests.get(URL, headers=headers)
                if api_page.status_code == 404:
                    print('Not Found.')
                elif api_page.status_code == 200:
                    soup = BeautifulSoup(api_page.content, 'html.parser')
                    survey_json = json.loads(soup.text)
                    print(survey_json)


            #========

csv_file.close()
json_file.write("]}\n")
json_file.close()
            

#date_name = "{:04d}".format(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day)

#        date_name = "{:04d}".format(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day)
#        URL = 'https://api.zoom.us/v2/metrics/meetings?type=past&from=' + date_name + '&to=' + date_name
#        print(URL)
