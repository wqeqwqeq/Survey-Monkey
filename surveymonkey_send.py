import pandas as pd
import numpy as np
import requests
import math
import json
import datetime
import pymongo
import dns

###############################################################
# def MongoClient
###############################################################

client = pymongo.MongoClient("mongodb+srv://123:GLOBALAI@cluster0-7ww3n.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = client['surveymonkey']

###############################################################
# def Creating Dataframe for Survey Questions
###############################################################

def missing_value_en(df):

#Judge if it is first time or not
    if 'Response' in df.columns:
        r = 1
    else:
        r = 0

    df = df[['METRIC NAME','COMPANY NAME','YEAR','VALUE','RANGE','email']]

#Get company index table
    COMPANY_TABLE=df[['COMPANY NAME','email']].drop_duplicates(subset='COMPANY NAME', keep='first').sort_values(by='COMPANY NAME').reset_index(drop=True)
    COMPANY_TABLE['COMPANY ID'] = pd.Categorical(COMPANY_TABLE['COMPANY NAME'].astype(str)).codes

#Get survey questions for those missing value
    selected_whole = df[((df['VALUE'] == 'Unclear') | (df['VALUE'] == 'Unknown') | (df['VALUE'].isna()) | (df['VALUE'].str.contains(r'^\s*[nN][aA][nN]\s*$',na=False)))].copy()
#Generate pseudo survey questions
    selected_whole['SURVEY QUESTIONS'] = selected_whole['METRIC NAME'].apply(lambda x:'Do you have answer to {}? If yes, please specify; if no,please leave the question blank'.format(x))
    print('----Successfully found out all missing values----')

    return COMPANY_TABLE,selected_whole,r

#def recipients_df
def recipient(company):
    recipients_df = COMPANY_TABLE[COMPANY_TABLE['COMPANY NAME']==company]
    recipients_df = recipients_df.drop(['COMPANY NAME','COMPANY ID'], axis=1)
    return recipients_df

#def selected
def select(company, year):
    selected = selected_whole[(selected_whole['COMPANY NAME']==company)&(selected_whole['YEAR']==year)]
    return selected

#def *open_ended* questions_list
#if not the first time, append three questions
def question(selected):
    additional_questions = ['why you choose not to report?', 'if you do report, what units of measurement do you use?', 'what is the feasibility that you will report in the units defined by the GCI?']
    normal_questions = list()
    if r == 0:
        question_s = list(selected["SURVEY QUESTIONS"])
    if r == 1:
        normal_questions = list(selected["SURVEY QUESTIONS"])
        normal_questions = normal_questions + additional_questions
        question_s = normal_questions

    question_type=["open_ended"]
    questions_list=[[x,y] for x in question_type for y in question_s]
    return questions_list

###############################################################
# def Process Survey Monkey API
###############################################################

# Create and send survey¶
def process(survey_title, questions_list, recipients_df):

# 1. Access surveymonkey api
    def access_api():
        client = requests.Session()
        YOUR_ACCESS_TOKEN = "fh68whZDMCBDYPTv2WD8W4arlGDS5hqhc95f2-X53oCE1VE-XI5FzwYuhuMhXSxI1ter71KtYY.VdZw-v..mVZzJZchPevSxSVu1IPUmcuPubqsDEWvL8bWy-gNX9hbn"
        client.headers.update({"Authorization": "Bearer %s" % YOUR_ACCESS_TOKEN,"Content-Type": "application/json"})
        return client
    client = access_api()

# 2. Create a survey, return survey id
    def create_survey(client, survey_title):
        payload = {"title": survey_title}
        url = "https://api.surveymonkey.com/v3/surveys"
        new = client.post(url, json=payload)
        survey_id = new.json()['id']
        return survey_id
    survey_id = create_survey(client,survey_title)
    print('*Generating survey of {} in year {}...*'.format(company,year))
    print("survey id: {}".format(survey_id))

# 3. Get page, return page_id
    def get_page(client, survey_id):
        payload = {"title": "", "description": ""}
        url = "https://api.surveymonkey.com/v3/surveys/{}/pages".format(survey_id)
        page = client.get(url, json=payload)
        page_id = page.json()['data'][0]['id']
        return page_id
    page_id = get_page(client, survey_id)
    print("page id: {}".format(page_id))
# 3.5. Create a page, return page_id
    def create_page(client, survey_id):
        payload = {"title": "", "description": ""}
        url = "https://api.surveymonkey.com/v3/surveys/{}/pages".format(survey_id)
        page = client.post(url, json=payload)
        page_id = page.json()['id']
        return page_id

# 4. Create questions on a survey page, return question id
# question format: [type, heading, [choices]]
# currently only two types of questions:[open_ended (text-based), single_choice]
    def create_question(client, survey_id, page_id, question):
        if question[0] == 'open_ended':
            payload = { "headings": [{"heading": question[1]}],
                "family": "open_ended",
                "subtype": "single"}
        elif question[0] == 'single_choice':
            choices = [{"text": c} for c in question[2]]
            payload = {"headings": [{"heading": question[1]}],
                "family": "single_choice",
                "subtype": "vertical",
                "answers": {"choices": choices,"other":[{"text": "Other","num_chars": 100,"num_lines": 1}]}}
        else:
            payload = {}
        url = "https://api.surveymonkey.com/v3/surveys/{}/pages/{}/questions".format(survey_id, page_id)
        q = client.post(url, json=payload)
        question_id = q.json()['id']
        return question_id
    question_id_col = list()
    for question in questions_list:
        question_id = create_question(client, survey_id, page_id, question)
        question_id_col.append(question_id)
    print("question ids: {}".format(question_id_col))

# 5. Creates a weblink or email collector for a given survey, return collector_id
    def create_collector(client, survey_id):
        payload = { #"type": "weblink"
                     "type": "email"}
        url = "https://api.surveymonkey.com/v3/surveys/{}/collectors".format(survey_id)
        collector = client.post(url, json=payload)
        collector_id = collector.json()['id']
        return collector_id
    collector_id = create_collector(client, survey_id)
    print("collector id: {}".format(collector_id))

# 6. Create and format an email message, return message_id
    def create_message(client, collector_id):
        payload = {"type": "invite"}
        url = "https://api.surveymonkey.com/v3/collectors/{}/messages".format(collector_id)
        message = client.post(url, json=payload)
        message_id = message.json()['id']
        return message_id
    message_id = create_message(client, collector_id)
    print("message id: {}".format(message_id))

# 7. Upload recipients to receive the message
    def upload_recipients(client, collector_id, message_id, recipients_df):
        # ex. [{"email":"xxx", "first_name":"xx", "last_name":"xx"}]
        contacts = json.loads(recipients_df.to_json(orient='records'))
        payload = {"contacts" : contacts}
        url = "https://api.surveymonkey.com/v3/collectors/{}/messages/{}/recipients/bulk".format(collector_id, message_id)
        recipients = client.post(url, json=payload)
    upload_recipients(client, collector_id, message_id, recipients_df)

# 8. Send your message (default one minute later)
    def send_message(client, collector_id, message_id, lag='00:01'):
        now = datetime.datetime.utcnow()
        sche_time = str(now).replace(' ','T') + '+' + lag
        payload = {"scheduled_date": sche_time}
        url = "https://api.surveymonkey.com/v3/collectors/{}/messages/{}/send".format(collector_id, message_id)
        r = client.post(url, json=payload)
    send_message(client, collector_id, message_id)
# keep survey id in record for responses collection
    return survey_id

###############################################################
#Using loop to send survey
###############################################################

#Update recipients
recipient_update = pd.DataFrame(list(db['recipients'].find()))
recipient_update = recipient_update.drop_duplicates(subset='company', keep='first')

df = pd.DataFrame(list(db['df'].find()))
df = pd.merge(df, recipient_update[['company','email address']], left_on='COMPANY NAME',right_on='company',how='left')
df['email'] = df['email address']
df = df.drop(['company','email address'],axis=1)

try:
    df = df.drop('_id',axis=1)
except:
    pass
print('----Successfully updated recipients----')

#Find missing value
COMPANY_TABLE,selected_whole,r = missing_value_en(df)
companies=set(selected_whole['COMPANY NAME'])
years=set(selected_whole['YEAR'])

survey_id_list = list()
for company in companies:
    for year in years:

# Determine the necessity of running the program
        if any(df[(df['COMPANY NAME'] == company)]['YEAR'] == year) is False:
            print('{} in year {} does not need to be getting missing data'.format(company,year))
            continue

# Judge missing values
        judge = selected_whole[((selected_whole['COMPANY NAME']==company) & (selected_whole['YEAR']==year))].drop_duplicates()
        if len(judge) != 0:
            pass
        else:
            print('{} in year {} has no missing values'.format(company,year))
            continue

# Judge email value
        judge1 = selected_whole.loc[((selected_whole['COMPANY NAME']==company) & (selected_whole['YEAR']==year)),'email'].iloc[0]
        if judge1 is np.nan:
            print('Lack of email address information of *{}*'.format(company))
            continue

# Generate recipients_df/selected/questions_list
        recipients_df = recipient(company)
        selected = select(company, year)
        questions_list = question(selected)

# Running the program
        survey_title='We want missing values in year {}'.format(year)
        survey_id = process(survey_title, questions_list, recipients_df)
        survey_id_list.append([company, year, survey_id])

print('----Successfully sent all surveys----')
survey_id_frame = pd.DataFrame(survey_id_list)
survey_id_frame.columns = ['COMPANY NAME','YEAR','survey_id']
db['surveyid_onetime'].delete_many({})
if survey_id_frame.to_dict('records') != []:
    db['surveyid_onetime'].insert_many(survey_id_frame.to_dict('records'))
    print('----Successfully uploaded "surveyid_onetime" to MongoDB----')
else:
    print('No results of "surveyid_onetime"')

selected_df = pd.merge(selected_whole, survey_id_frame, on=['COMPANY NAME','YEAR'], how='left')
db['selected_onetime'].delete_many({})
if selected_df.to_dict('records') != []:
    db['selected_onetime'].insert_many(selected_df.to_dict('records'))
    print('----Successfully uploaded "selected_onetime" to MongoDB----')
else:
    print('No results of "selected_onetime"')
