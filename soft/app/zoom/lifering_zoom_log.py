# ===============================================================================================
# Program to read Zoom data from the API and write it into a format that is good for analysis
# ===============================================================================================

# -----------------------------
# Imports
# -----------------------------

import requests
import json
import re
import csv
import time
import sys
import collections
import math
import pytz
import os

from typing import Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from os import path
from urllib.parse import quote
from icalendar import Calendar, Event, vCalAddress, vText
from pathlib import Path


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

data_file_prefix = 'lifering_meeting_log_data'
data_file_suffix = file_suffix

event_file_prefix = 'lifering_zoom_event_data'
event_file_suffix = file_suffix

# The range of months for the given year
year = 2022
month_start = 1
month_end = 13

seconds_per_minute = 60
minutes_per_hour = 60  #The 'per' is inverted
seconds_per_hour = seconds_per_minute*minutes_per_hour
hours_per_day=24
days_per_week=7
hours_per_week = hours_per_day * days_per_week

ticks_per_minute = 1
ticks_per_hour = ticks_per_minute*minutes_per_hour

ticks_per_tock = 5
tocks_per_tuck = 3
ticks_per_tuck = ticks_per_tock * tocks_per_tuck

seconds_per_tick = seconds_per_minute*ticks_per_minute
secticks_per_tick = seconds_per_tick
secticks_per_hour = secticks_per_tick*ticks_per_hour

seconds_per_day = seconds_per_hour*hours_per_day
hours_per_week = hours_per_day*days_per_week


one_minute = 1*seconds_per_minute #15 # 5*60 #5 #5*60
five_minutes = 5*seconds_per_minute #15 # 5*60 #5 #5*60




# -----------------------------
# Program Macro State
# -----------------------------

active_meeting_dict: Dict[Any, Any] = {}
previous_active_meeting_dict: Dict[Any, Any] = {}

expected_meeting_dict: Dict[Any, Any] = {}

# -----------------------------
# Functions
# -----------------------------
def qk(key): #Quote-key... short to be easy to use
    return  '‹'+key+'›'


# -----------------------------
# Main status loop
# -----------------------------


program_start_time = datetime.now(timezone.utc)
program_start_time_string = program_start_time.strftime(datetime_format)
print("Start Time =", program_start_time_string)

current_datetime = program_start_time
current_datetime_string = current_datetime.strftime(datetime_format)
current_weekday = current_datetime.weekday()
current_daysectick = (current_datetime - current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
current_weeksectick = (current_daysectick + seconds_per_day * current_weekday)
print ('Weeksectick: ',current_weeksectick)


infix='_' + program_start_time_string
data_file_name = data_file_prefix + infix + data_file_suffix

print("Opening: " + data_file_name)
csv_file = open(data_file_name, 'w')
csv_writer = csv.writer(csv_file)

print("Opening Slack: " + slack_URL)
slack_response = requests.post(slack_URL,json={
    "type": "mrkdwn",
    'text':'*Launched:* LifeRing Zoom Log Bot '
})
print("Status",slack_response.status_code)

print("Opening Slack 2: " + slack_detail_URL)
slack_response = requests.post(slack_detail_URL,json={
    "type": "mrkdwn",
    'text':'*Launched:* LifeRing Zoom Log Bot '
})
print("Status",slack_response.status_code)

most_recent_checkin_tick = current_weeksectick

#==========================================
#==========================================
#==========================================

meeting_data_file_name = None

if __name__ == "__main__":
    # print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        if (i == 1):
            meeting_data_file_name = arg
        # print(f"Argument {i:>6}: {arg}")

print (meeting_data_file_name)

meeting_data_json_file = open(meeting_data_file_name, 'r')
meeting_data = json.load(meeting_data_json_file)
meeting_data_json_file.close()

meeting_info_dict = {}
meeting_info_mid_dict = {}
meeting_hour_array=[]
for i in range(hours_per_week):
    meeting_hour_array.append([])


for meeting in meeting_data['meetings']:
    topic = meeting['topic']
    uuid = meeting['uuid']
    meeting_start_time_string = meeting['start_time']
    if (meeting_start_time_string == None):
        print("No start_time")
        meeting_weekday = -1
        meeting_start_time_string = "1000-01-01T01:01:01Z"
        meeting_starttime = datetime.strptime(meeting_start_time_string, datetime_format).replace(tzinfo=timezone.utc)
        meeting_starttimeofday = -1
        meeting_weeksectick = -1
    else:
        meeting_starttime = datetime.strptime(meeting_start_time_string, datetime_format).replace(tzinfo=timezone.utc)
        meeting_weekday = meeting_starttime.weekday()
        meeting_starttimeofday = meeting_starttime.time()
        meeting_daysectick = (meeting_starttime - meeting_starttime.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
        meeting_weeksectick = (meeting_daysectick + (seconds_per_day * meeting_weekday))

    meeting_weekhour = math.floor(meeting_weeksectick / seconds_per_hour)
    #print ('Meeting Weeksectick: ',meeting_weeksectick, meeting_weekhour, ' • ', meeting_starttime, meeting_weekday, meeting_starttimeofday)

    m = re.search(r'\(\d+\)', topic)
    if (m == None):
        mid = '()'
    else:
        mid = m.group(0)

    at_pos = meeting['user_email'].index('@')
    account_name = meeting['user_email'][:at_pos]
    is_public ="chat" in account_name
    meeting_is_plausibly_active = meeting_weekday >= 0 and (meeting_starttime > current_datetime)

    meeting['starttime']=meeting_starttime
    meeting['weekday']=meeting_weekday
    meeting['starttimeofday']=meeting_starttimeofday
    meeting['meeting_weeksectick']=meeting_weeksectick
    meeting['meeting_weekhour']=meeting_weekhour
    meeting['is_inactive']=not meeting_is_plausibly_active
    meeting['normal_url']=meeting['join_url']
    if (meeting['xza_url'] != None):
        meeting['join_url']=meeting['xza_url']

    meeting['mid']=mid
    meeting['is_public']=is_public
    meeting['account_name']=account_name


    meeting_info_dict[uuid]=meeting
    meeting_info_mid_dict[mid]=uuid
    
    if (meeting_is_plausibly_active and is_public):
        meeting_hour_array[meeting_weekhour].append(meeting)
    
    #if current_weekday == meeting_weekday:
    print(qk(uuid), meeting['topic'], meeting_start_time_string, ' • ', meeting_weekhour, ' • ', meeting_starttime, meeting_weekday, meeting_starttimeofday, meeting_weeksectick, math.floor((meeting_weeksectick -current_weeksectick)/(60*60)))

for index, meeting_list in enumerate(meeting_hour_array):
    print (index, ' • ', meeting_list)

#========================================
#========================================
#========================================

shouldLoop = True;

upcoming_dict: Dict[Any, Any] = {}
started_dict: Dict[Any, Any] = {}
passed_dict: Dict[Any, Any] = {}
active_dict: Dict[Any, Any] = {}
            
while (shouldLoop):
    slack_payload_block_array=[]
    slack_2_payload_block_array=[]
    slack_3_payload_block_array=[]


    current_time = datetime.now(timezone.utc)
    current_time_string = current_time.strftime(datetime_format)
    current_weekday = current_time.weekday()
    current_timeofday = current_time.time()
    current_daysectick = (current_time - current_time.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    current_weeksectick = (current_daysectick + seconds_per_day * current_weekday)
    current_weekhour = math.floor(current_weeksectick / seconds_per_hour)
    current_weekminute = math.floor(current_weeksectick / seconds_per_minute)

    print('Checking: ', current_weekhour, ':', current_weekminute, ' • ', current_time_string, current_time, current_weekday, current_timeofday, current_weeksectick, most_recent_checkin_tick, math.floor(current_weeksectick - most_recent_checkin_tick), secticks_per_hour)
    
    for meeting_key, meeting_entry in meeting_info_dict.items():
        meeting_uuid = meeting_entry['uuid']
        meeting_starttime = meeting_entry['starttime']
        meeting_weekday = meeting_entry['weekday']
        meeting_starttimeofday = meeting_entry['starttimeofday']
        meeting_weeksectick = meeting_entry['meeting_weeksectick']
        meeting_weekhour = meeting_entry['meeting_weekhour']

        meeting_is_active = meeting_weekday >= 0 and (meeting_starttime > current_time)


        time_span_early_hour = math.floor((meeting_weeksectick - current_weeksectick)/(seconds_per_hour))
        time_span_late_tick = math.floor((current_weeksectick - meeting_weeksectick)/seconds_per_minute)
        upcoming_entry = upcoming_dict.get(meeting_uuid)
        meeting_started = started_dict.get(meeting_uuid)

        #print('Check Upcoming: ',meeting_is_active, current_weekhour, '==', meeting_weekhour, current_weekhour == meeting_weekhour, time_span_early_hour < 2, ' • ', time_span_late_tick , ' > ', ticks_per_tock, time_span_late_tick > ticks_per_tock, upcoming_entry != None, meeting_started == None, ' • ', current_weekhour, meeting_weekhour, time_span_early_hour, meeting_weeksectick, current_weeksectick, ' • ', meeting_weekday, meeting_starttime,  current_time, time_span_early_hour, upcoming_entry, meeting_entry)

        if (time_span_early_hour < -1): #Boundary issue for the week?
            #Remove old entries for '-1' and below
            time_span_early_hour = time_span_early_hour+hours_per_week
            
        if ((upcoming_entry != None) and (meeting_started == None) and (time_span_late_tick > ticks_per_tock)): #This meeting should have started (5-minutes / 1-tock late)
            if (passed_dict.get(meeting_uuid) == None):
                passed_dict[meeting_uuid]=current_time
                if meeting_entry['is_public']:
                    slack_text = "*Alert! Not Started:* "+meeting_entry['topic']+"  ‹"+meeting_entry['account_name']+"›"
                    slack_3_payload_block_array.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": slack_text
                    }})

        if (time_span_early_hour >= 0 and time_span_early_hour < 1):  #Do this second (after late-check) to give the meeting a chance to 'report in'
            if (upcoming_entry == None) and meeting_is_active:
                print ('Upcoming: ', time_span_early_hour, meeting_entry)
                upcoming_dict[meeting_uuid]=current_time

                if meeting_entry['is_public']:
                    slack_text = "*Upcoming:* "+meeting_entry['topic']+"  ‹"+meeting_entry['account_name']+"›"+" • "+meeting_entry['join_url']
                    slack_payload_block_array.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": slack_text
                    }})
                    
        if (time_span_late_tick > ticks_per_hour): #Make sure dictionaries are clear if pass the range of possible starts
            upcoming_dict.pop(meeting_uuid, None)
            started_dict.pop(meeting_uuid, None)
            passed_dict.pop(meeting_uuid, None)




    URL = 'https://api.zoom.us/v2/metrics/meetings?type=live'
    #print('Calling: ', URL)




    try:
        meeting_page = requests.get(URL, headers=headers, timeout=10)
    except requests.exceptions.Timeout:
        print ("Timeout occurred", URL)
        time.sleep (five_minutes)
        continue

    if meeting_page.status_code == 404:
        print('Not Found.')
    elif meeting_page.status_code == 200:
        soup = BeautifulSoup(meeting_page.content, 'html.parser')
        # print(meeting_page)
        # print(soup)

        meeting_json = json.loads(soup.text)
        
        #========
        


        count = 0

        slack_data_set = []
        
        for meeting in meeting_json['meetings']:
            uuid = meeting.get('uuid')
            uuid_encode = str(uuid)
            if uuid_encode[0] == "/":
                uuid_encode = quote(quote(uuid_encode, safe=''), safe='')
            URL_participants = 'https://api.zoom.us/v2/metrics/meetings/' + uuid_encode + '/participants?type=live' + page_size  # +'&from='+date_name+'&to='+date_name

            meeting_info = meeting_info_dict.get(uuid)
            topic = meeting.get('topic')  # 2021-06-23T22:49:42Z    2021-06-23T23:59:17Z
            #print(meeting)
            m = re.search(r'\(\d+\)', topic)
            if (m == None):
                mid = '()'
            else:
                mid = m.group(0)
            
            if (meeting_info == None):
                #print("No meeting info for = '",uuid,"' ",topic)
                uuid = meeting_info_mid_dict.get(mid)
                if (uuid == None):
                    meeting_info={'join_url':'???','user_email':'???'}
                else:
                    meeting_info = meeting_info_dict.get(uuid)
                    if (meeting_info == None):
                        meeting_info={'join_url':'???','user_email':'???'}
            print("Meeting running: ",qk(uuid)," ",topic)

            participants = meeting.get('participants')
            meeting_start_time_string = meeting.get('start_time')
            meeting_starttimeofday = meeting.get('starttimeofday')
            account_name = meeting_info.get('account_name')
            previous_meeting_entry = previous_active_meeting_dict.get(uuid)
            has_previous = None == previous_meeting_entry


            active_dict_entry = active_dict.get(uuid)
            if (active_dict_entry == None):
                activity_counter = 1;
                max_participants = participants
                participant_dict = {}
            else:
                max_participants = active_dict_entry['max_participants']
                if (participants > max_participants):
                    max_participants = participants;
                activity_counter = active_dict_entry['activity_counter'] + 1
                participant_dict = active_dict_entry['participant_dict']


            #===================
            
            
            participants_page = requests.get(URL_participants, headers=headers)
            if participants_page.status_code == 404:
                print('No participants found', URL_participants)
            elif participants_page.status_code == 200:
                soup = BeautifulSoup(participants_page.content, 'html.parser')
                # print(meeting_page)
                # print(soup)

                participants_json = json.loads(soup.text)
                for participant in participants_json['participants']:
                    user_name = participant.get('user_name')
                    ip_address = participant.get('ip_address')

                    clean_name = user_name
                    clean_name = re.sub(regex_whitelist, '', clean_name).strip()
                    spaces = [m.start() for m in re.finditer(r" ", clean_name)]
                    if len(spaces) > 1:
                        clean_name = clean_name[:spaces[1]]
                    parens = [m.start() for m in re.finditer(r"\(", clean_name)]
                    if len(parens) > 0:
                        clean_name = clean_name[:parens[0]]
                    clean_name = re.sub(' ', '', clean_name).lower()

                    full_user_name=clean_name+"_"+ip_address
                    participant_entry=participant_dict.get(full_user_name)
                    if ( participant_entry == None):
                        grey_participant = False
                        for greypattern in greylist:
                            if re.match(greypattern, full_user_name):
                                grey_participant = True
                                slack_text = "*Alert! Grey Participant:* "+full_user_name+' in '+topic+"  ‹"+account_name+"›"
                                slack_3_payload_block_array.append({
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": slack_text
                                }})
                                break
                        print("Participant: ",full_user_name, 'grey', grey_participant)
                        participant_entry={'user_name': user_name, 'ip_address': ip_address, 'clean_name': clean_name, 'full_user_name':full_user_name, 'grey_participant': grey_participant}
                        participant_dict[full_user_name]=participant_entry


            #===================
            
            active_dict_entry = {'start_time': meeting_start_time_string, 'participants': participants, 'max_participants': max_participants, 'activity_counter': activity_counter, 'participant_dict':participant_dict}
            active_dict[uuid]=active_dict_entry


            data = {'query_time': current_time_string, 'uuid': uuid, 'start_time': meeting_start_time_string, 'participants': participants, 'topic': topic, 'user_email' : meeting_info['user_email'], 'join_url' : meeting_info['join_url'], 'activity_counter': activity_counter, 'max_participants': max_participants, 'is_public' : meeting_info['is_public'], 'account_name' : meeting_info['account_name'], 'participant_dict':participant_dict} #'has_previous' : has_previous,
            
            active_meeting_dict[uuid] = data;
            
            #--data{text:"..."} # https://hooks.slack.com/services/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
            
            csv_writer.writerow(data.values())
            csv_file.flush()
            
            slack_data_set.append(data) #Yes... it could collide... maybe_fix
            


        slack_data_set.sort(key=lambda x:x['start_time'])
        for slack_data in slack_data_set:
            if (slack_data['activity_counter'] == 1):
                uuid = slack_data['uuid'];
                started_dict[uuid] = current_time
                if slack_data['is_public']:
                    print ("Started", qk(slack_data['uuid']), slack_data['topic'])
                    slack_text = "*Started:* "+slack_data['topic']+"  ‹"+slack_data['account_name']+"›"+" • "+slack_data['join_url']
                    slack_payload_block_array.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": slack_text
                    }})

        for slack_data in slack_data_set:
            activity_counter = slack_data['activity_counter']
            if (activity_counter % tocks_per_tuck == 0):
                if slack_data['is_public']:
                    slack_text = "*Activity:* "+slack_data['topic']+"  ‹"+slack_data['account_name']+"›"+" • "+str(slack_data['participants'])+" participants"+" • "+str(slack_data['activity_counter'] * 5)+" minutes"
                    #slack_text += " • ".join("=".join((str(k),str(v))) for k,v in slack_data.items())
                    slack_2_payload_block_array.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": slack_text
                    }})
        for previous_key, previous_entry in previous_active_meeting_dict.items():
            active_meeting_entry = active_meeting_dict.get(previous_key)
            if (active_meeting_entry == None):
                #ended_dict[uuid] = current_time
                if previous_entry['is_public']:
                    print ("Meeting ended", qk(previous_key))
                    slack_text = "*Ended:* "+previous_entry['topic']+" • Max Participants = "+str(previous_entry['max_participants'])+" • Duration = "+str(previous_entry['activity_counter'] * 5)+" minutes"
                    slack_payload_block_array.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": slack_text
                    }})
                    meeting_uuid = previous_key
                    upcoming_dict.pop(meeting_uuid, None)
                    started_dict.pop(meeting_uuid, None)
                    passed_dict.pop(meeting_uuid, None)
                    active_dict.pop(meeting_uuid, None)



        #slack_text = " • ".join("=".join((str(k),str(v))) for k,v in slack_data.items())
        if (len(slack_payload_block_array) > 0):
            slack_response = requests.post(slack_URL,json={'blocks': slack_payload_block_array})
            print("Slack Status",slack_response.status_code, 'for', slack_payload_block_array)
            most_recent_checkin_tick = current_weeksectick

        if (len(slack_2_payload_block_array) > 0):
            slack_response = requests.post(slack_detail_URL,json={'blocks': slack_2_payload_block_array})
            print("Slack 2 Status",slack_response.status_code, 'for', slack_2_payload_block_array)
            most_recent_checkin_tick = current_weeksectick
            
        if (len(slack_3_payload_block_array) > 0):
            slack_response = requests.post(slack_alert_URL,json={'blocks': slack_3_payload_block_array})
            print("Slack 3 Status",slack_response.status_code, 'for', slack_3_payload_block_array)
            most_recent_checkin_tick = current_weeksectick
            

        if (math.floor(current_weeksectick - most_recent_checkin_tick) > secticks_per_hour):
        
            slack_response = requests.post(slack_detail_URL,json={
                         "type": "mrkdwn",
                'text':'*Heartbeat:* LifeRing Zoom Log Bot'
                                           })
            print("Heartbeat-2",slack_response.status_code,current_weeksectick,most_recent_checkin_tick,math.floor(current_weeksectick - most_recent_checkin_tick), secticks_per_hour)

            most_recent_checkin_tick = current_weeksectick




    previous_active_meeting_dict = active_meeting_dict
    active_meeting_dict = {}

    time.sleep (five_minutes)

#========================================
#========================================
#========================================


csv_file.close()

#date_name = "{:04d}".format(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day)

#        date_name = "{:04d}".format(year) + '-' + "{:02d}".format(month) + '-' + "{:02d}".format(day)
#        URL = 'https://api.zoom.us/v2/metrics/meetings?type=past&from=' + date_name + '&to=' + date_name
#        print(URL)
