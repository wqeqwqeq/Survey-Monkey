import pandas as pd
import numpy as np
import requests
import math
import json
import pymongo
import dns

###############################################################
# def MongoClient
###############################################################

client = pymongo.MongoClient("mongodb+srv://123:GLOBALAI@cluster0-7ww3n.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = client['surveymonkey']

###############################################################
# def Collect results and analysis
###############################################################

def after_survey(survey_id,selected_df,responses):

# 3. Export the Results of a Survey
    def survey_result(client, survey_id):

# This call returns the survey’s design with all question ids and answer
# option ids, as well as the values associated with them
        questions = client.get('https://api.surveymonkey.net/v3/surveys/{}/details'.format(survey_id))

# map question id to question text (and choices if exist)
        questions_map = {}
        for q in questions.json()['pages'][0]['questions']:
            ques_type = q['family']
            questions_map[q['id']] = [q['headings'][0]['heading'],ques_type]
            if ques_type == 'single_choice':
                questions_map[q['id']].append({x['id']:x['text'] for x in q['answers']['choices']})

# build a dataframe of responses data
        rows = list()
# id: respondent ID
        columns = ['survey_id','id','collector_id','date_created','date_modified','ip_address',
               'metadata','pages']

        for r in responses.json()['data']:
            row = {k: r.get(k, None) for k in columns}
            #print(row)
            contact_info = row.pop('metadata')
            if contact_info:
                row['email_address'] = contact_info['contact']['email']['value']
                row['first_name'] = ''
                row['last_name'] = ''

            answers_info = row.pop('pages')

            if answers_info[0]['questions'] != []:
                i = 0
                for a in answers_info[0]['questions']:
                    question = questions_map[a['id']]
                    if question[1] == 'open_ended':
                        row['Q'+str(i).zfill(3)+'_'+question[0]] = a['answers'][0]['text']
                    elif question[1] == 'single_choice':
                        choice_id = a['answers'][0]['choice_id']
                        row['Q'+str(i).zfill(3)+'_'+question[0]] = question[2][choice_id]
                    else:
                        pass   # undifined
                    i += 1
            else:
                i = 0
                for id,question in questions_map.items():
                    row['Q'+str(i).zfill(3)+'_'+question[0]] = ''
                    i += 1

            rows.append(row)
        df_results = rows
        return df_results
    df_results = survey_result(client, survey_id)
    df_results = pd.DataFrame(df_results)

#4. Format the column name firt by splitting the df into 2
    split_list = ['collector_id','date_created','date_modified','email_address',
                        'first_name','id','ip_address','last_name','survey_id']
    df_split = df_results.drop(split_list,axis=1)

#5. Format the questions' column name
    a = df_split.columns
    b = a.str.slice(start=5)
    d = { a[i] : b[i] for i in range(len(a)) }
    df_results.rename(columns=d, inplace=True)
    df_result_answer = df_results

#6. Convert it into a Dataframe
    drop_list_1 = ['collector_id','date_created','date_modified','email_address',
                    'first_name','id','ip_address','last_name','survey_id']
    df_result_answer = df_result_answer.drop(drop_list_1, axis =1)
    df_result_answer = df_result_answer.T
    df_result_answer = df_result_answer.reset_index()
    df_result_answer.columns = ['SURVEY QUESTIONS','Response']
    df_result_answer['survey_id'] = survey_id

    df_result_split = pd.DataFrame()
    df_result_split = df_result_answer.loc[df_result_answer['SURVEY QUESTIONS'].isin(['why you choose not to report?', 'if you do report, what units of measurement do you use?', 'what is the feasibility that you will report in the units defined by the GCI?'])]

    if k == 0:
        additional_frame = df_result_split
    else:
        additional_frame = additional_df
        additional_frame = pd.concat([additional_frame, df_result_split],ignore_index=True)

    if k == 0:
        result_table = pd.merge(selected_df, df_result_answer, on=['survey_id','SURVEY QUESTIONS'], how='left')
    else:
        result_table = selected_table
        result_table = pd.merge(result_table, df_result_answer, on=['survey_id','SURVEY QUESTIONS'], how='left')
    return result_table, additional_frame

###############################################################
#def Evaluation
###############################################################

# set up range type
def split_b (range_value):
    a='['
    b=','
    c=']'
    range_value=range_value.split(a)[1].split(c)[0].split(b)
    return range_value[0],range_value[1]

def change_range(data_value):
    a=int(split_b(data_value)[0])
    b=int (split_b(data_value)[1])
    return range(a,b+1)

def evaluate(selected_table):
    success=pd.DataFrame()
    fail=pd.DataFrame()
    fail_index=[]
    success_index=[]
#NaN, range, yes/no
    for i in range(len(selected_table)):
        try:
            if math.isnan(selected_table.loc[i,'Response']):
                fail_index.append(i)
                continue
        except:
            pass
        if  '[' in selected_table.loc[i,'RANGE']:
            try:
                if int(selected_table.loc[i,'Response']) not in change_range(selected_table.loc[i,'RANGE']):
                    fail_index.append(i)
                    continue
            except:
                fail_index.append(i)
                continue

        if 'Yes' in selected_table.loc[i,'RANGE']:
            if (selected_table.loc[i,'Response'].lower()!= 'Yes'.lower()) or (selected_table.loc[i,'Response'].lower()!= 'No'.lower()):
                fail_index.append(i)
                continue

        else:
            success_index.append(i)

    success=selected_table.iloc[success_index]
    fail=selected_table.iloc[fail_index]
    print('----Successfully evaluated the response----')
    return success,fail

###############################################################
#def heatmap
###############################################################

def heatmapcsv(df):
    heatmap = df[['METRIC NAME','COMPANY NAME','VALUE']].copy()
    heatmap['VALUE'] = heatmap['VALUE'].isnull().astype(int)
    heatmap = heatmap.groupby(['COMPANY NAME','METRIC NAME']).agg({'VALUE':np.sum})
    heatmap = heatmap.unstack(level=0)
    heatmap.columns = [item[1] for item in heatmap.columns]
    heatmap = heatmap-1
    heatmap[heatmap>=0] = 0
    heatmap[heatmap==-1] = 1
    return heatmap

###############################################################
#Using loop to receive survey
###############################################################

# 1. Define variable
k = 0
df = pd.DataFrame(list(db['df'].find()))
df = df[['METRIC NAME','COMPANY NAME','YEAR','VALUE','RANGE','email']]

selected_df = pd.DataFrame(list(db['selected_onetime'].find()))
try:
    selected_df = selected_df.drop('_id',axis=1)
except:
    pass

survey_id_frame = pd.DataFrame(list(db['surveyid_onetime'].find()))
try:
    survey_id_frame = survey_id_frame.drop('_id',axis=1)
except:
    pass

survey_id_list = survey_id_frame['survey_id'].values
if len(survey_id_list) != 0:
    pass
else:
    print('No survey_id exists')
    quit()

blank = 0
blank_max = len(survey_id_list)

# 2. Access surveymonkey api
def access_api():
    client = requests.Session()
    YOUR_ACCESS_TOKEN = "fh68whZDMCBDYPTv2WD8W4arlGDS5hqhc95f2-X53oCE1VE-XI5FzwYuhuMhXSxI1ter71KtYY.VdZw-v..mVZzJZchPevSxSVu1IPUmcuPubqsDEWvL8bWy-gNX9hbn"
    client.headers.update({"Authorization": "Bearer %s" % YOUR_ACCESS_TOKEN,"Content-Type": "application/json"})
    return client
client = access_api()

for survey_id in survey_id_list:
# fetch the first 100 responses to your survey
    responses = client.get('https://api.surveymonkey.net/v3/surveys/{}/responses/bulk?page=1&per_page=100'.format(survey_id))
#Judge if get a response
    if responses.json()['data'] == []:
        blank = blank+1
        print('Not get a response in survey id: {}'.format(survey_id))
        continue

    selected_table, additional_df = after_survey(survey_id,selected_df,responses)
    k = k+1

if blank == blank_max:
    print('All surveys did not answer')
    quit()
    
#Generate and save additional_frame
additional_df = pd.merge(additional_df, survey_id_frame, on='survey_id', how='left')
additional_df = additional_df.drop('survey_id',axis=1)
db['additional_onetime'].delete_many({})
if additional_df.to_dict('records') != []:
    db['additional_onetime'].insert_many(additional_df.to_dict('records'))
    print('----Successfully uploaded "additional_onetime" to MongoDB----')
else:
    print('No results of "additional_onetime"')

#Merge responses to one column

temp=selected_table.iloc[:,list(range(8,selected_table.shape[1]))]
for i in range(len(temp)):
    for j in temp.iloc[i]:
        try:
            if math.isnan(j):
                continue
            else:
                selected_table.loc[i,'Response']=j
        except:
            selected_table.loc[i,'Response']=j
selected_table = selected_table[['METRIC NAME','COMPANY NAME','email','YEAR','RANGE','Response']]
print('----Successfully collected the response----')

###############################################################
#Using loop to evaluate result
###############################################################

success,fail = evaluate(selected_table)
db['success_onetime'].delete_many({})
if success.to_dict('records') != []:
    db['success_onetime'].insert_many(success.to_dict('records'))
    print('----Successfully uploaded "success_onetime" to MongoDB----')
else:
    print('No results of "success_onetime"')

db['fail_onetime'].delete_many({})
if fail.to_dict('records') != []:
    db['fail_onetime'].insert_many(fail.to_dict('records'))
    print('----Successfully uploaded "fail_onetime" to MongoDB----')
else:
    print('No results of "fail_onetime"')

###############################################################
#Refill data
###############################################################

df = pd.merge(df,success[['METRIC NAME','COMPANY NAME','YEAR','Response']],on=['METRIC NAME','COMPANY NAME','YEAR'],how='left')
df.loc[((df['VALUE'] == 'Unclear') | (df['VALUE'] == 'Unknown') | (df['VALUE'].isna()) | (df['VALUE'].str.contains(r'^\s*[nN][aA][nN]\s*$',na=False))),'VALUE'] = np.nan
df['VALUE'].fillna(df['Response'],inplace=True)
db['df'].delete_many({})
if df.to_dict('records') != []:
    db['df'].insert_many(df.to_dict('records'))
    print('----Successfully uploaded "df" to MongoDB----')
else:
    print('No results of "df"')

###############################################################
#Generating heatmap and upload to Mongodb
###############################################################

heatmap = heatmapcsv(df)
db['heatmap_onetime'].delete_many({})
if heatmap.to_dict('records') != []:
    db['heatmap_onetime'].insert_many(heatmap.to_dict('records'))
    print('----Successfully uploaded "heatmap_onetime" to MongoDB----')
else:
    print('No results of "heatmap_onetime"')
