# -------------------------------------------------------------------------------------------------------- #
# Overview : Get more insights from Murphy Alert data. Creating dashboard similar to Murphy account        #
#           Alerts dashboard and provide option to the user to convert multiple alerts to convert to Tasks.#
#           Generated tasks can be seen inside ITM application.                                            #
# Modified By : Santosh Chakre,
#               Sandeep B                                                                           #
# Modified Date : 02-04-2022 
#               : 15-04-2022       
# Modified Changes :                                                                        #
# ---------------------------------------------------------------------------------------------------------#

# Importing required libraries
import dash
from pathlib import Path
from dash.dependencies import Input, Output, State
from dash import dcc, html, dash_table, callback_context    
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import mysql.connector
from uuid import uuid4
import datetime
import random
import os

# --------------------------------------
# Import data
# --------------------------------------
# Importing data
df = pd.read_csv('data/Screenout_Python_Data.csv')

# Dropping columns from dataframe
new_df = df.drop(['Column1', 'OFFSET_MUWI', 'ACTIVE_TUBE_PRESSURE - Only For Offset',
                  'ACTIVE_CASE_PRESSURE - Only For Offset', 'THRESHOLD_TUBE_PRESSURE - Only For Offset'
                     , 'THRESHOLD_CASE_PRESSURE - Only For Offset', 'SEVERITY'
                     , 'OFFSET_WELL - Only for Offset Alert', 'DETECTED_COST_ESTIMATE- for Cost Alert'
                     , 'DETECTED_COST_PLANNED- for Cost Alert', 'COST_OVERRUN_THRESHOLD - for Cost Alert'], axis=1)

# Converting Datetime and creating new columns "DATE" and "DATE_TIME" from ALERT_TIME
new_df['Date'] = pd.to_datetime(new_df['ALERT_TIME'], format='%d-%m-%Y %H:%M').dt.date
new_df["Date"] = pd.to_datetime(new_df["Date"])
new_df['Date_Time'] = pd.to_datetime(new_df['ALERT_TIME'], format='%d-%m-%Y %H:%M').dt.time

# print(new_df)

# Joining Active Well + Stage
idx = 0
active_well_stage = new_df['ACTIVE_WELL'].astype(
    str) + ' - ' + new_df['STAGE']
new_df.insert(loc=idx, column='ACTIVE WELL - STAGE', value=active_well_stage)

# Getting only values needed from the "new_df"
dataframe = new_df.loc[:, ['ACTIVE WELL - STAGE', 'ALERT_TIME', 'ELAPSED_TIME(Tn-T0) in Min',
                           'FLUID_VOLUME', 'SAND', 'SAND_CONC', 'DETECTED_SLURRY_RATE', 'DETECTED_WH_PRESSURE']]

# Get the highest active well count
active_well_count = new_df.groupby(['ACTIVE_WELL'])[['OBJECT_ID']].count().reset_index()
active_well_count.sort_values(by=['OBJECT_ID'], inplace=True, ascending=False)

# To get only date
new_df['Only_Date'] = pd.to_datetime(new_df['ALERT_TIME']).dt.date
new_df1 = new_df.groupby(['Only_Date'])[['ACTIVE_WELL']].count().reset_index()

# a temporary lookup set to get unique STAGE
lookup = set()
active_stage = [x for x in new_df['STAGE'].unique() if x not in lookup and lookup.add(x) is None]

# Active well grouping based on date
active_well_group_by_date = new_df.groupby(['Date'])[['ACTIVE_WELL']].count().reset_index()
active_well_group_by_date.sort_values(by=['ACTIVE_WELL'], inplace=True, ascending=False)

# DATA COLUMN - English NAMES - Trying - not yet finalised or used
# english_column_names = ['Active Well - Stage', 'Date & Time', 'Elapsed Time (Min)', 'Fluid Vol (bbl)', 'Height (cm)',
#                 'Weight (kg)']

#datatable headers
english_column_names = ['ACTIVE WELL and STAGE', 'ALERT TIME', 'ELAPSED TIME in Min ',
                'FLUID VOLUME', 'SAND', 'SAND CONC','SLURRY RATE THRESHOLD','WH PRESSURE INCREASE THRESHOLD',
                'TOTAL SAND THRESHOLD','WH SAND CONC THRESHOLD','SLURRY RATE INCREASE THRESHOLD', 
                'DETECTED SLURRY RATE', 'DETECTED WH PRESSURE']
df_columns = ['ACTIVE WELL - STAGE', 'ALERT_TIME', 'ELAPSED_TIME(Tn-T0) in Min',
                'FLUID_VOLUME', 'SAND', 'SAND_CONC','SLURRY_RATE_THRESHOLD','WH_PRESSURE_INCREASE_THRESHOLD',
                'TOTAL_SAND_THRESHOLD','WH_SAND_CONC_THRESHOLD','SLURRY_RATE_INCREASE_THRESHOLD', 
                'DETECTED_SLURRY_RATE', 'DETECTED_WH_PRESSURE']  

# Total Converted task details from TASK_EVENTS table
# sql = "SELECT COUNT(*) FROM TASK_EVENTS WHERE PROC_NAME = 'Murphy - Screenout Alert';"

# -----------------------------------------------
# Database Connection
db = mysql.connector.connect(host="wb-lnd.mysql.database.azure.com",user="Workbox@wb-lnd", passwd="Incture@123", database="workbox_dev")

# Local Database connection used during Azure services are not available
# db = mysql.connector.connect(host='localhost', user='root', password='root', database='mysql')

database_cursor = db.cursor()
#tOTAL CONVERTED TASKS
database_cursor.execute("SELECT COUNT(*) FROM TASK_EVENTS WHERE PROC_NAME = 'Murphy - Screenout Alert'") 
TotalConvertedTaskCount = database_cursor.fetchone()[0]
# database_cursor.execute("SELECT * FROM TASK_EVENTS WHERE () and (CREATED_AT = datetime.da)")
database_cursor.execute("SELECT COUNT(*) FROM TASK_EVENTS WHERE (PROC_NAME = 'Murphy - Screenout Alert') and (CREATED_AT between curdate() and curdate()+1)")
newConvertedTasks = database_cursor.fetchone()[0]
newTaskPercentage = round((newConvertedTasks/TotalConvertedTaskCount)*100,2)

# Calculate percentage change
total_alerts = new_df.loc[new_df['ALERT_TYPE'] == 'Screen Out', 'ACTIVE_MUWI'].count() - TotalConvertedTaskCount
change_percentage = round((( total_alerts -new_df1['ACTIVE_WELL'].iloc[-2]) / total_alerts) * 100, 2)

#Static card data --> will be changed if we connect to database
total_static_card = new_df1['ACTIVE_WELL'].iloc[-1] - new_df1['ACTIVE_WELL'].iloc[-2]

#Select All option for dropdown
dropdown_options=[]

for c in (new_df['ACTIVE_WELL'].unique()):
    dropdown_options.append(c)
dropdown_options.insert(0,"Select All")

# -----------------------------------------------
# App layout

dash_app = dash.Dash(__name__, prevent_initial_callbacks=True, suppress_callback_exceptions=True)
app = dash_app.server
dash_app.layout = html.Div([
    # ----------------------------------------------------
    # Data table definition and data population
    # ---------------------------------------------------
    # First Row - Heading
    html.Div([

        # Project Title



        html.Div([
            html.Div([
                html.H3('Convert  Alerts To Tasks', style={'margin-bottom': '0px', 'color': '#1f2c56'}),
                # html.H6('Track  Alert Data', style={'margin-bottom': '0px', 'color': '#1f2c56'})
            ])
        ], id='title', className='one-third column'),

        html.Div([
            html.Div([
                html.H3('IOT Data', style={'margin-bottom': '0px', 'color': '#1f2c56'}),
            ])
        ], id='title2', className='one-third column'),

        # Last Updated
        html.Div([
            html.H6('Last Updated :' + str(
                datetime.datetime.strptime(new_df['ALERT_TIME'].iloc[-1], '%d-%m-%Y %H:%M').strftime(
                    '%B %d %Y , %H:%M:%S')) + ' (UTC)', style={'color': 'Orange','font-size':'14px'}),
        ], className='one-third column', id='title1'),
    ], id='header', className='row flex-display', style={'margin-bottom': '25px'}),

    # Second Row - Cards
    html.Div([

        # Total Alerts
        html.Div([
            html.H6(children='Total Alerts',
                    style={'textAlign': 'center', 'color': '#1f2c56'}
                    ),
            html.P(f"{total_alerts}",
                   style={'textAlign': 'center', 'color': 'Orange', 'fontSize': 40}
                   ),
            # html.P(
            #     'New ' + f"{total_static_card:,.0f}" + ' (' + str(
            #         change_percentage) + '%)',
            #     style={'textAlign': 'center', 'color': 'Orange', 'fontSize': 15, 'margin-top': '-15px'}),
        ], className='card_container three columns'),
        # Total Screen-out Alert
        html.Div([
            html.H6(children='Total Screenout Alert',
                    # + str(new_df.loc[new_df['ALERT_TYPE'] == 'Screen Out', 'ACTIVE_MUWI'].count()),
                    style={'textAlign': 'center', 'color': '#1f2c56', 'fontSize': 14}
                    ),
            html.P(f"{new_df.loc[new_df['ALERT_TYPE'] == 'Screen Out', 'ACTIVE_MUWI'].count():,.0f}",
                   style={'textAlign': 'center', 'color': '#dd1e35', 'fontSize': 30}
                   ),
        ], className='card_container two columns'),

        # Total Offset Alert
        html.Div([
            html.H6(children='Total Offset Alert',
                    # + str(new_df.loc[new_df['ALERT_TYPE'] == 'Screen Out', 'ACTIVE_MUWI'].count()),
                    style={'textAlign': 'center', 'color': '#1f2c56', 'fontSize': 14}
                    ),
            html.P(f"{new_df.loc[new_df['ALERT_TYPE'] == 'Offset', 'ACTIVE_MUWI'].count():,.0f}",
                   style={'textAlign': 'center', 'color': '#dd1e35', 'fontSize': 30}
                   ),
        ], className='card_container two columns'),

        # Total Cost communication
        html.Div([
            html.H6(children='Cost Communication',
                    # + str(new_df.loc[new_df['ALERT_TYPE'] == 'Cost Communication', 'ACTIVE_MUWI'].count()),
                    style={'textAlign': 'center', 'color': '#1f2c56', 'fontSize': 14}
                    ),
            html.P(f"{new_df.loc[new_df['ALERT_TYPE'] == 'Cost Communication', 'ACTIVE_MUWI'].count():,.0f}",
                   style={'textAlign': 'center', 'color': '#dd1e35', 'fontSize': 30}
                   ),
        ], className='card_container two columns'),

        # Total Converted Task Details
        html.Div([
            html.H6(children='Total Converted Tasks',
                    # + str(new_df.loc[new_df['ALERT_TYPE'] == 'Murphy Account', 'ACTIVE_MUWI'].count()),
                    style={'textAlign': 'center', 'color': '#1f2c56'}
                    ),
            html.P(f"{TotalConvertedTaskCount}",
                   style={'textAlign': 'center', 'color': '#2A5674', 'fontSize': 40}
                   ),
            html.P(
                'New ' + f"{newConvertedTasks}" + ' (' + str(
                    newTaskPercentage) + '%)',
                style={'textAlign': 'center', 'color': '#00CC96', 'fontSize': 15, 'margin-top': '-15px'}),
        ], className='card_container three columns'),
    ], className='row flex-display'),

    # Third Row - Dropdowns
    html.Div([
        # Active Well Dropdown
        html.Div([
            html.P('Select Active Well :', className='fix_label', style={'color': '#1f2c56'}),
            dcc.Dropdown(id='s_active_well',
                         multi=False,
                         searchable=True,
                         value='Select Well',
                         placeholder='Select Active Well',
                         options=[{'label': c, 'value': c }
                                  for c in (dropdown_options)
                                  ], className='dcc_compon')
        ], className='three columns',style={'margin-bottom': '25px'}),

        # Active Stage Dropdown
    #     html.Div([
    #         html.P('Select Active Stage :', className='fix_label', style={'color': '#1f2c56'}),
    #         dcc.Dropdown(id='s_active_stage',
    #                      multi=False,
    #                      searchable=True,
    #                      value='Select Stage',
    #                      placeholder='Select Stage',
    #                      options=[{'label': c, 'value': c}
    #                               for c in (new_df['STAGE'].unique())
    #                               ], className='dcc_compon')
    #         # dcc.Dropdown(id='s_active_stage')
    #     ], className='three columns'),
    # ], className='create_container row flex-display', style={'margin-bottom': '25px'}),

    # Fourth Row - Graphs
    html.Div([
        html.Div([
            dcc.Graph(id='h-bar-chart', config={'displayModeBar': False}),
        ], className='five columns create_container'),
        html.Div([
            dcc.Graph(id='pie-chart', config={'displayModeBar': False}),
        ], className='seven columns create_container'),

    ], className='row flex-display', style={'margin-bottom': '25px'}),

    # # Bar Graph and Multiline chart Row
    html.Div([
        html.Div([
            dcc.Graph(id='multi-line-chart', config={'displayModeBar': False}),
        ], className='twelve columns create_container '),
    ],  style={'margin-bottom': '25px'}),
    # Fifth Row - Data Table

    html.Div([
        dash_table.DataTable(
            id='datatable-interactivity',
            columns=[
                {'name': col, 'id': df_columns[idx], "deletable": False, "selectable": True}
                for (idx, col) in enumerate(english_column_names)
                ],
            data=new_df.to_dict('records'),  # the contents of the table
            editable=False,  # allow editing of data inside all cells
            sort_action="native",  # enables data to be sorted per-column by user or not ('none')
            sort_mode="single",  # sort across 'multi' or 'single' columns
            row_selectable="multi",  # allow users to select 'multi' or 'single' rows
            row_deletable=False,  # choose if user can delete a row (True) or not (False)
            selected_columns=[],  # ids of columns that user selects
            selected_rows=[],  # indices of rows that user selects
            page_action="native",  # all data is passed to the table up-front or not ('none')
            page_current=0,  # page number that user is on
            page_size=15,  # number of rows visible per page
            style_data={
                'color': '#1f2c56',
                'backgroundColor': '#FAFCFF'
            },
            style_table={'overflowX': 'auto'},
            style_header={
                'font-size': '14px',
                'backgroundColor': '#FAFCFF',
                'color': '#1f2c56',
                'fontWeight': 'bold',
                'whiteSpace': 'normal',
                'height': 'auto',
                'lineHeight': '25px',
                'textOverflow': 'ellipsis',
                'textAlign': 'center',
            },

            style_cell_conditional=[

                {
                'if': {'column_id': c},
                'textAlign': 'left'
                } for c in ['ACTIVE WELL - STAGE']
            ]
        ),
    ], className='create_container ', style={'margin-bottom': '25px'}),

    # Sixth Row - Buttons
    html.Div([
        html.Div([
            html.Button('Export to CSV', id='save_to_csv', n_clicks=0
                        , style={'color': 'Orange', 'margin-bottom': '25px'}),
        ], className='three columns'),
        html.Div([
            html.Button('Create Task', id='save_to_db', n_clicks=0, style={'color': 'Orange', 'margin-bottom': '25px'}),
        ], className='three columns'),
        # Create notification when saving to excel
        html.Div(id='container-button-timestamp') ,
    ], className='row flex-display', style={'margin-bottom': '25px'}),
    html.Div(id='output_div'),
    html.Div(id='divout'),
    
    html.Div(id='datatable-interactivity-container'),

    dcc.Store(id="store", data=0),
    dcc.Interval(id='interval', interval=1000),

], id='main-container', style={'display': 'flex', 'flex-direction': 'column'})

])

# Multi Line Chart
@dash_app.callback(Output('multi-line-chart', 'figure'),
              [Input('s_active_well', 'value'), ],
              prevent_initial_call=False,)

def update_multi_line_chart(active_well):
    mul_line_chart_df = new_df.copy()
    mul_line_chart_df['SAND_BY_1000'] = mul_line_chart_df['SAND'] / 1000.0
    mul_line_chart_df['DETECTED_WH_PRESSURE_BY_100'] = mul_line_chart_df['DETECTED_WH_PRESSURE'] / 100
    mul_line_chart_df['FLUID_VOLUME_BY_100'] = mul_line_chart_df['FLUID_VOLUME'] / 100
    mul_line_chart_df.sort_values('ALERT_TIME', inplace=True)
   
    mul_line_chart_df2=mul_line_chart_df
    counter = 1
    if counter!=0:
        if active_well!='Select Well' :
            mask1 = (mul_line_chart_df.ACTIVE_WELL == active_well)
            mul_line_chart_df=mul_line_chart_df.loc[mask1,:]
            counter=0
    # print(mul_line_chart_df)


    # multi_line_fig = go.Figure()
    multi_line_fig = go.Figure()
    multi_line_fig.add_trace(go.Scatter(x=mul_line_chart_df['ALERT_TIME'], y=mul_line_chart_df['FLUID_VOLUME_BY_100'],
                                        mode='lines',
                                        name='Fluid Volume * 100'))
    multi_line_fig.add_trace(
        go.Scatter(x=mul_line_chart_df['ALERT_TIME'], y=mul_line_chart_df['DETECTED_WH_PRESSURE_BY_100'],
                    mode='lines',
                    name='Detected WH Pressure * 100',
                    yaxis="y2"))
    multi_line_fig.add_trace(go.Scatter(x=mul_line_chart_df['ALERT_TIME'], y=mul_line_chart_df['SAND_BY_1000'],
                                        mode='lines', name='Sand * 1000',yaxis="y3"))
    multi_line_fig.add_trace(go.Scatter(x=mul_line_chart_df['ALERT_TIME'], y=mul_line_chart_df['DETECTED_SLURRY_RATE'],
                                        mode='lines', name='Slurry Rate',yaxis="y4"))
    multi_line_fig.update_xaxes(tickangle=80)
    multi_line_fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                                    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                                    },
                                    font_color='#1f2c56',
                                    margin=dict(l=20, r=20, t=30, b=20),
                                        xaxis=dict(
                                            domain=[0.1, 0.9]
                                        ),                                      
                                        yaxis=dict(
                                        title="Fluid Volume (bbl)",
                                        titlefont=dict(
                                            color="#1f77b4"
                                        ),
                                        tickfont=dict(
                                            color="#1f77b4"
                                        )
                                    ),
                                    yaxis2=dict(
                                        title="Detected WH Pressure (psi)",
                                        titlefont=dict(
                                            color="#ff7f0e"
                                        ),
                                        tickfont=dict(
                                            color="#ff7f0e"
                                        ),
                                        anchor="free",
                                        overlaying="y",
                                        side="left",
                                        position=0
                                    ),
                                    yaxis3=dict(
                                        title="Sand (lb)",
                                        titlefont=dict(
                                            color="#00D0D0"
                                        ),
                                        tickfont=dict(
                                            color="#00D0D0"
                                        ),
                                        anchor="x",
                                        overlaying="y",
                                        side="right"
                                    ),
                                    yaxis4=dict(
                                        title="Slurry Rate (bpm)",
                                        titlefont=dict(
                                            color="#9467bd"
                                        ),
                                        tickfont=dict(
                                            color="#9467bd"
                                        ),
                                        anchor="free",
                                        overlaying="y",
                                        side="right",
                                        position=1
                                    )
                                                                    ),
    multi_line_fig.update_layout(
        width=800,
    )

    multi_line_fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                                'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                                },
                                font_color='#1f2c56',
                                margin=dict(l=20, r=20, t=30, b=20),
                                xaxis=dict(showgrid=False),
                                yaxis=dict(showgrid=False),
                                yaxis2=dict(showgrid=False),
                                yaxis3=dict(showgrid=False),
                                yaxis4=dict(showgrid=False)                                
                                )
    multi_line_fig.update_xaxes(linecolor='#1f2c56')
    multi_line_fig.update_yaxes(linecolor='#1f2c56')

    if active_well == None or active_well == "Select All":
        multi_line_fig = go.Figure()
        multi_line_fig.add_trace(go.Scatter(x=mul_line_chart_df2['ALERT_TIME'], y=mul_line_chart_df2['FLUID_VOLUME_BY_100'],
                                            mode='lines',
                                            name='Fluid Volume * 100'))
        multi_line_fig.add_trace(
            go.Scatter(x=mul_line_chart_df2['ALERT_TIME'], y=mul_line_chart_df2['DETECTED_WH_PRESSURE_BY_100'],
                        mode='lines',
                        name='Detected WH Pressure * 100',
                        yaxis="y2"))
        multi_line_fig.add_trace(go.Scatter(x=mul_line_chart_df2['ALERT_TIME'], y=mul_line_chart_df2['SAND_BY_1000'],
                                            mode='lines', name='Sand * 1000',yaxis="y3"))
        multi_line_fig.add_trace(go.Scatter(x=mul_line_chart_df2['ALERT_TIME'], y=mul_line_chart_df2['DETECTED_SLURRY_RATE'],
                                            mode='lines', name='Slurry Rate',yaxis="y4"))
        multi_line_fig.update_xaxes(tickangle=80)
        multi_line_fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                                        'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                                        },
                                        font_color='#1f2c56',
                                        margin=dict(l=20, r=20, t=30, b=20),
                                            xaxis=dict(
                                                domain=[0.1, 0.9]
                                            ),                                      
                                            yaxis=dict(
                                            title="Fluid Volume (bbl)",
                                            titlefont=dict(
                                                color="#1f77b4"
                                            ),
                                            tickfont=dict(
                                                color="#1f77b4"
                                            )
                                        ),
                                        yaxis2=dict(
                                            title="Detected WH Pressure (psi)",
                                            titlefont=dict(
                                                color="#ff7f0e"
                                            ),
                                            tickfont=dict(
                                                color="#ff7f0e"
                                            ),
                                            anchor="free",
                                            overlaying="y",
                                            side="left",
                                            position=0
                                        ),
                                        yaxis3=dict(
                                            title="Sand (lb)",
                                            titlefont=dict(
                                                color="#00D0D0"
                                            ),
                                            tickfont=dict(
                                                color="#00D0D0"
                                            ),
                                            anchor="x",
                                            overlaying="y",
                                            side="right"
                                        ),
                                        yaxis4=dict(
                                            title="Slurry Rate (bpm)",
                                            titlefont=dict(
                                                color="#9467bd"
                                            ),
                                            tickfont=dict(
                                                color="#9467bd"
                                            ),
                                            anchor="free",
                                            overlaying="y",
                                            side="right",
                                            position=1
                                        )
                                                                        ),
        multi_line_fig.update_layout(
            width=800,
        )

        multi_line_fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                                    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                                    },
                                    font_color='#1f2c56',
                                    margin=dict(l=20, r=20, t=30, b=20),
                                    xaxis=dict(showgrid=False),
                                    yaxis=dict(showgrid=False),
                                    yaxis2=dict(showgrid=False),
                                    yaxis3=dict(showgrid=False),
                                    yaxis4=dict(showgrid=False)                                
                                    )
        multi_line_fig.update_xaxes(linecolor='#1f2c56')
        multi_line_fig.update_yaxes(linecolor='#1f2c56')


        return multi_line_fig


    return multi_line_fig


# Pie Chart
@dash_app.callback(Output('pie-chart', 'figure'),
              [Input('s_active_well', 'value'), ],
              prevent_initial_call=False, )
def update_pie_chart(active_well):

    pie_df = new_df.groupby(['ACTIVE_WELL'])[['OBJECT_ID']].count().reset_index()
    pie_df_copy=pie_df
    counter2=1
    if counter2!=0:
        if active_well!='Select Well' :
            mask2 = (pie_df.ACTIVE_WELL==active_well)
            pie_df = pie_df.loc[mask2,:]
            counter2=0
    # print(pie_df)
    pie_fig = px.pie(pie_df, values='OBJECT_ID', names='ACTIVE_WELL', color='ACTIVE_WELL',
                     color_discrete_sequence=px.colors.sequential.Teal,
                     labels={"ACTIVE_WELL": "Active well",
                             "OBJECT_ID": "Count",
                             },  # map the labels
                     title='Active well count',  # figure title
                     template='plotly_white',
                     hole=0.5,
                     )
    pie_fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                           'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                           },
                          font_color='#1f2c56',
                          margin=dict(l=20, r=20, t=30, b=20),
                          )

    if active_well == None or active_well == "Select All":
        pie_fig = px.pie(pie_df_copy, values='OBJECT_ID', names='ACTIVE_WELL', color='ACTIVE_WELL',
                    color_discrete_sequence=px.colors.sequential.Teal,
                    labels={"ACTIVE_WELL": "Active well",
                            "OBJECT_ID": "Count",
                            },  # map the labels
                    title='Active well count',  # figure title
                    template='plotly_white',
                    hole=0.5,
                    )
        pie_fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                            'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                            },
                            font_color='#1f2c56',
                            margin=dict(l=20, r=20, t=30, b=20),
                            )
        return pie_fig

    return pie_fig

# Horizontal Bar Chart
@dash_app.callback(
    Output('h-bar-chart', 'figure'),
    [Input('s_active_well', 'value'), ],
    prevent_initial_call=False,
    # Input('my-date-picker-end','date'),
)
def update_bar(active_well):

    dff = new_df.copy()
    dff = dff[["ACTIVE_WELL"]].value_counts()
    dff = dff.to_frame()
    dff.reset_index(inplace=True)
    dff.rename(columns={0: 'Total Active well'}, inplace=True)
    dffcopy=dff
    counter3=1
    if counter3!=0:
        if active_well!='Select Well' :
            mask3 = (dff.ACTIVE_WELL==active_well)
            dff = dff.loc[mask3,:]
            counter3=0

    fig_bar = px.bar(dff, x='Total Active well', y='ACTIVE_WELL', template='seaborn',
            orientation='h', title="Active well count", text_auto=True,labels=dict(x="Total Active Wells",y="Active Well"))
    fig_bar.update_yaxes(tickangle=30)
    fig_bar.update_xaxes(showgrid=False)
    fig_bar.update_yaxes(showgrid=False)
    fig_bar.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                        'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                        },
                        font_color='#1f2c56',
                        margin=dict(l=20, r=20, t=30, b=20),
                        )

    fig_bar.update_traces(marker_color='Orange', textfont_size=12, textangle=0, textposition="outside",
                        cliponaxis=False)
                        
    # print(dff)
    if active_well == None or active_well == "Select All":
        # print("inside last if")
        fig_bar = px.bar(dffcopy, x='Total Active well', y='ACTIVE_WELL', template='seaborn',
                orientation='h', title="Active wells count", text_auto=True,labels={'x':"Total Active Wells",'y':"Active Well"})
        fig_bar.update_yaxes(tickangle=30)
        fig_bar.update_xaxes(showgrid=False,title="Total Active Wells")
        fig_bar.update_yaxes(showgrid=False,title="Active Well")
        fig_bar.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                            'paper_bgcolor': 'rgba(0, 0, 0, 0)',
                          },
                            font_color='#1f2c56',
                            margin=dict(l=20, r=20, t=30, b=20),
                            )
        fig_bar.update_traces(marker_color='Orange', textfont_size=12, textangle=0, textposition="outside",
                            cliponaxis=False)
        return fig_bar

    return fig_bar

# Save to CSV
@dash_app.callback(
    Output('container-button-timestamp', 'children'),
    [Input('save_to_csv', 'n_clicks'),
     Input('save_to_db', 'n_clicks'),
     Input("interval", "n_intervals")],
    [State('datatable-interactivity', 'data'),
     State('store', 'data')],prevent_initial_call=True,
)
def df_to_csv(n_clicks,nclicks,n_intervals, dataset, s):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]

    if "save_to_csv" in changed_id:
        df = pd.DataFrame(dataset)
        path=str(Path.home() / "Downloads")
        df.to_csv(os.path.join(path,'IOT_Alerts.csv'))
        return(html.Plaintext("The data has been saved to your folder.",
                            style={'color': 'green', 'font-weight': 'bold', 'font-size': 'large'}))
    elif "save_to_db" in changed_id:
        return(html.Plaintext("The Task Has Been Created.",
                            style={'color': 'green', 'font-weight': 'bold', 'font-size': 'large'}))
    
    return dash.no_update


# ---------------------------SAVE to CSV END---------------------

# ---------------------------------------------------------------
# Highlighting Selected cells
@dash_app.callback(
    Output('output_div', 'children'),
    Input('datatable-interactivity', 'active_cell'),
    State('datatable-interactivity', 'data')
)
def getActiveCell(active_cell, data):
    if active_cell:
        col = active_cell['column_id']
        row = active_cell['row']
        cellData = data[row][col]
        # html.P(f'Row: {row}, Col: {col}, value: {cellData}')
        return 

# Highlighting Selected rows

@dash_app.callback(
    Output('datatable-interactivity', 'style_data_conditional'),
    [Input('datatable-interactivity', 'selected_rows')])
def update_styles(selected_rows):
    return [{'if': {'row_index': i}, 'background_color': '#FFFCCF'} for i in selected_rows]

# SAVE TO DB selecting rows data
@dash_app.callback(
    Output('datatable-interactivity-container', "children"),
    [Input('datatable-interactivity', "derived_virtual_row_ids"),
     Input('datatable-interactivity', "derived_virtual_selected_rows"),
     Input('datatable-interactivity', "active_cell"),
     Input('save_to_db', 'n_clicks')
     ], [State('datatable-interactivity', 'data')])
def f(row_ids, derived_virtual_selected_rows, active_cell, n_clicks, data):
    input_triggered = dash.callback_context.triggered[0]["prop_id"].split(".")[0]

    dff = list()
    if derived_virtual_selected_rows is not None:
        for i in derived_virtual_selected_rows:
            dff.append(dataframe.loc[i])

    if input_triggered == "save_to_db":
        # print("save to db")
        for row in dff:
            i = random.randint(1, 1000)
            requestID = "MA00" + str(i)
            tp = tuple(row)
            uniqueEvent_id = str(uuid4())
            uniqueProcess_id = str(uuid4())
            user_id = str('P000006')
            email = str("ShaileshShetty@incture.com")
            username = str("Shailesh Shetty")
            business_status = str("Reserved")
            status = str('READY')
            enroute = int(1)
            name = str("Murphy - Screenout Alert")
            origin = str("SCP")
            priority = str("High")
            subject = str("Murphy Screenout Alert - " + row[0])
            tasktype = str("Approve/Reject")
            processName = str("Murphy Account")
            attkey = str("MURPHY001")

            database_cursor = db.cursor()
            sql = "INSERT INTO Task_Owners (EVENT_ID,TASK_OWNER,EN_ROUTE,GROUP_ID,GROUP_OWNER,IS_PROCESSED," \
                  "IS_REVIEWER,IS_SUBSTITUTED,TASK_OWNER_EMAIL,TASK_OWNER_DISP) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            val = (uniqueEvent_id, user_id, enroute, None, None, 0, None, None, email, username)
            database_cursor.execute(sql, val)
            db.commit()

            sql = "INSERT INTO Task_Events  (EVENT_ID,BUSINESS_STATUS,COMPLETED_AT,COMP_DEADLINE,CREATED_AT," \
                  "CREATED_BY,CUR_PROC,CUR_PROC_DISP,DESCRIPTION,FORWARDED_AT,FORWARDED_BY,NAME,ORIGIN,PRIORITY," \
                  "PROCESS_ID,PROC_NAME,CRITICAL_DEADLINE,STATUS,STATUS_FLAG,SUBJECT,TASK_MODE,TASK_TYPE,UPDATED_AT," \
                  "URL,FORM_ID) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            val = (uniqueEvent_id, business_status, None, str(datetime.datetime.now() + datetime.timedelta(days=10)),
                   str(datetime.datetime.now()), user_id, user_id, username, row[0], None,
                   None, name, origin, priority, uniqueProcess_id, name, str(datetime.datetime.now()), status, 0,
                   subject, None, tasktype, str(datetime.datetime.now()), None, None)
            database_cursor.execute(sql, val)
            db.commit()

            sql = "INSERT INTO process_events (PROCESS_ID,COMPLETED_AT,NAME,REQUEST_ID,STARTED_AT,STARTED_BY," \
                  "STARTED_BY_DISP,STATUS,SUBJECT) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            val = (
                uniqueProcess_id, None, name, requestID, str(datetime.datetime.now()), user_id, username, status,
                row[0])
            database_cursor.execute(sql, val)
            db.commit()

            sql = "INSERT INTO custom_attr_values (TASK_ID,PROCESS_NAME,ATTR_KEY,ATTR_VALUE) VALUES (%s,%s,%s,%s)"
            val = (uniqueEvent_id, processName, attkey, None)
            database_cursor.execute(sql, val)
            db.commit()

            database_cursor.close()
            db.close()
            print("Successfully Inserted into database  Active well-stage: " + row[0])
            
    return


# ---------SAVE to DB End----------------------------

if __name__ == '__main__':
    dash_app.run_server(debug=True)
