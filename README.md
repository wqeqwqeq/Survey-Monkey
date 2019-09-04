# infra-crwd-surveymky 2.0
1.0) Author: `Xiaojun (Uie) Yao` | Edit time: 2019-07-10

2.0) Author: `Stanley (Zhehui) Yuan` `Jiayu (Jerry) Lin` | Edit time: 2019-08-13
## Introduction
This project automatically find missing value in the target data frame and generate survey composed of open ended questions send to the target'company's email. Once they filled out the survey, we query the answer from the Survey Monkey Api. We evaluate the quality of the answers; we collect those answer left in blank, those out of bounds, those apparently answered without logic. We resend the survey to the company to do the further query. 
We also create heat map to visualiza


## Data

#### automation packages: `surveymonkey_send.py`,`surveymonkey_receive.py`

#### Follow the steps below to under the code

#### Problems Left to solve ：N/A
#### `The bold section is 2.0 update content, welcome new updates.`

## 0. Function definition
**mongoclient - Connect to MongoDB database in python**

missing_value_en() - Create overall missing value dataframe

process() - Create and send survey 

after_survey() - Collect responses and format them

**evaluate() – Evaluate the results**

**heatmapcsv() - Generate a binary result table for plotting**

## 1. Pre-processing
(1) **Obtain original dataset (df) and recipient information table (recipients) from MongoDB to update the e-mail address in the df.**

(2) Detect missing values and define missing value dataframe (selected_whole) including corresponding company, year, automatically generated questions and so on.

(3) For each company each year, generate a list of questions (questions_list) **with three additional questions for company that has not answered or answered incorrectly in previous surveys.**

## 2. Using loop to send survey
(1) **For each year of each company, determine the necessity of running sending program including non-required year, no missing values and invalid email address issues.**

(2) Access surveymonkey API, create new survey, new page, a list of questions.

(3) Create email collector, format email message, upload recipients.

(4) Send message (default one minute later).

## 3. Establish connection
(1) **Upload connection datasets (selected_df/survey_id_frame) to MongoDB for responses collection.**

## 4. Using loop to collect survey
(1) **Retrieve the connection datasets.**

(2) **For each survey id, determine the necessity of running receiving program including empty survey id dataframe, no answer, empty answer and all surveys no answer issues.**

(3) Export the answers of a survey, convert it into a dataframe (df_result_answer), format index and column name.

(4) **Integrate responses into the missing value dataframe (selected_table) using iterative merge.**

## 5. Evaluate and Refill the results
(1) **Judge whether the recipient's reply is reasonable by the RANGE indicator prepared in advance and generate a success table and a failure table.**

(2) **Merge the results on success table into original dataset (df).**

## 6. Heatmap for plotting
(1) **Generate a binary result table (heatmap) with problems as index and companies as column.**

(2) **Upload heatmap table to MongoDB and update original dataset at the same time.**








