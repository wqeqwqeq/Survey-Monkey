import dash
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as pyo
import matplotlib as plt
import seaborn as sns
import pymongo
import dns

client = pymongo.MongoClient("mongodb+srv://123:GLOBALAI@cluster0-7ww3n.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = client['surveymonkey']
collection = db['heatmap']

#After we have the table, we can just do: df = pd.read_excel
# evaluated_response_data = np.random.choice(a=[1, 0], size=(33,5))
# df = pd.DataFrame(data=evaluated_response_data,
#                   columns=['apple', 'google', 'amazon', 'facebook', 'microsoft'])
# df.head()

doc = collection.find_one()
heat_map_df = pd.DataFrame(data=[doc for doc in collection.find()])
heat_map_df.drop(columns=['_id'], inplace=True)
heat_map_df.set_index('METRIC NAME', inplace=True)
heat_map_df.head()

response_df = pd.Dataframe()

app = dash.Dash()
app.layout = html.Div([dcc.Graph(id='heap_map',
                                 figure={'data':[go.Heatmap(z=heat_map_df.values.tolist(),
                                                            x=heat_map_df.columns.tolist(),
                                                            y=heat_map_df.index.tolist())],
                                         'layout':go.Layout(title='Heap Map of Company Response')}
                                )])

if __name__ == '__main__':
    PORT = 8000
    ADDRESS = '127.0.0.1'
    app.run_server(port=PORT, host=ADDRESS)
