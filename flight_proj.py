from flask import Flask, render_template
import pandas as pd
import snowflake.connector
import plotly.graph_objs as go

# Create the Flask app
app = Flask(__name__)

# Define the Snowflake connection parameters
SNOWFLAKE_ACCOUNT = 'qh23977.ca-central-1.aws'
SNOWFLAKE_USER = 'samchaudhari30496'
SNOWFLAKE_PASSWORD = 'SicMundus99$'
SNOWFLAKE_DATABASE = 'flightdb'
SNOWFLAKE_SCHEMA = 'flight_schema'
SNOWFLAKE_WAREHOUSE = 'compute_wh'

# Define a function to execute a SQL query and return the results as a pandas dataframe
def execute_query(query):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    cursor.close()
    conn.close()
    return df

# Define the home page route
@app.route('/')
def home():
    return render_template('index.html')

# Define a route to show the number of flights by day of week
@app.route('/num_flights_by_day_of_week',  methods = ["GET","POST"])
def num_flights_by_day_of_week():
    query = '''
    SELECT DAY_OF_WEEK, COUNT(*) AS NUM_FLIGHTS
    FROM FLIGHT_TABLE
    GROUP BY DAY_OF_WEEK
    ORDER BY DAY_OF_WEEK
    '''
    df = execute_query(query)
    week_dict = dict(zip(df['DAY_OF_WEEK'], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']))
    df['DAY'] = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    # print(week_dict)
    # print(df['DAY_OF_WEEK'], df['DAY'])
    fig = go.Figure(data=[go.Pie(labels=df['DAY'], values=df['NUM_FLIGHTS'], hole=0.2)])
    fig.update_layout(
        title='Number of Flights by Day of Week',
        title_x=0.5,
        xaxis_title='Day of Week',
        yaxis_title='Number of Flights',
        xaxis_tickangle=-45, height = 800,
        font=dict(size=16),
    )
    #plot_div = fig.to_html(full_html=False)
    fig.write_html("templates/delay_day_week.html")
    return render_template('delay_day_week.html')

# Define a route to show the average flight distance by airline
@app.route('/avg_distance_by_airline', methods = ["GET","POST"])
def avg_distance_by_airline():
    query = '''
    SELECT AVG(DISTANCE) AS AVG_DISTANCE, AN.DESCRIPTION
    FROM FLIGHT_TABLE FT JOIN AIRLINE_NAME AN ON FT.BRANDED_CODE_SHARE = AN.CODE
    GROUP BY AN.DESCRIPTION
    ORDER BY AVG_DISTANCE DESC
    LIMIT 10
    '''
    df = execute_query(query)
    fig = go.Figure(data=[go.Bar(x=df['AVG_DISTANCE'], y=df['DESCRIPTION'], orientation='h', marker_color='orange')])
    fig.update_layout(
        title='Average Flight Distance by Airline',
        xaxis_title='Average Distance between airports (miles)',
        yaxis_title='Airline',
        xaxis_tickangle=-45,
        font=dict(size=16),
        height=900,
    )
    #plot_div = fig.to_html(full_html=False)
    
    fig.write_html("templates/dist_airline.html")
    return render_template('dist_airline.html')

# Define a route to show the number of flights by month and destination state
@app.route('/num_flights_by_month_and_dest_state', methods = ["GET","POST"])
def num_flights_by_month_and_dest_state():
    query = '''
    SELECT MONTH_OF_YEAR, DEST_STATE_ABR, COUNT(*) AS NUM_FLIGHTS
    FROM FLIGHT_TABLE
    GROUP BY MONTH_OF_YEAR, DEST_STATE_ABR
    ORDER BY MONTH_OF_YEAR, DEST_STATE_ABR
    '''
    df = execute_query(query)
    fig = go.Figure(data=go.Heatmap(
        x=df['MONTH_OF_YEAR'],
        y=df['DEST_STATE_ABR'],
        z=df['NUM_FLIGHTS'],
        colorscale='Peach',
        zmin=0,
        zmax=80000,
        colorbar=dict(title='Number of Flights'),
        # showgrid=True,
    ))
    fig.update_layout(
        # title='Number of Flights by Month and Destination State', title_x=0.5,
        # xaxis_title='Month',
        # yaxis_title='Destination State',
        # height=1000,
        # width=1800,
        # font=dict(size=16),
    title='Number of Flights by Month and Destination State',
    title_x=0.5,
    xaxis_title='Month',
    yaxis_title='Destination State',
    height=1000,
    width=1800,
    font=dict(size=16),
    xaxis=dict(
        tickmode='array',
        tickvals=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        tickfont=dict(size=14),
        # use tickvals and ticktext to specify the tick values and labels for the x-axis
        # add tickvals and ticktext to specify the vertical lines separating the months
    ),
    yaxis=dict(
        tickfont=dict(size=14),
    ),
    plot_bgcolor='white',
    margin=dict(l=80, r=0, t=80, b=80),
    )

    # add vertical lines separating the months
    for month in range(2, 13):
        fig.add_shape(
            type='line',
            x0=month - 0.5,
            y0=-0.5,
            x1=month - 0.5,
            y1=df['DEST_STATE_ABR'].nunique() - 0.5,
            line=dict(color='white', width=2)
        )
    for state in range(1, df['DEST_STATE_ABR'].nunique()):
        fig.add_shape(
            type='line',
            x0=0.5,
            y0=state - 0.5,
            x1=df['MONTH_OF_YEAR'].nunique() + 0.5,
            y1=state - 0.5,
            line=dict(color='white', width=2)
        )
    #plot_div = fig.to_html(full_html=False)
    fig.write_html("templates/flights_month_dest.html")
    return render_template('flights_month_dest.html')

# Define a route to show the number of delayed flights by airline and month
@app.route('/num_delayed_flights_by_airline_and_month', methods = ["GET","POST"])
def num_delayed_flights_by_airline_and_month():
    query = '''
    SELECT BRANDED_CODE_SHARE, MONTH_OF_YEAR, COUNT(ARR_DELAY > 0) AS NUM_DELAYED_FLIGHTS
    FROM FLIGHT_TABLE
    WHERE ARR_DELAY IS NOT NULL  AND BRANDED_CODE_SHARE NOT IN ('AA_CODESHARE', 'AS_CODESHARE', 'B6_CODESHARE', 'DL_CODESHARE', 'F9_CODESHARE', 'G4_CODESHARE', 'HA_CODESHARE', 'NK_CODESHARE', 'UA_CODESHARE', 'WN_CODESHARE')
    GROUP BY BRANDED_CODE_SHARE, MONTH_OF_YEAR
    '''
    df = execute_query(query)
    airlines = sorted(df['BRANDED_CODE_SHARE'].unique())
    data = []
    for airline in airlines:
        airline_df = df[df['BRANDED_CODE_SHARE'] == airline]
        trace = go.Bar(
            x=airline_df['MONTH_OF_YEAR'],
            y=airline_df['NUM_DELAYED_FLIGHTS'],
            name=airline,
        )
        data.append(trace)
    fig = go.Figure(data=data)
    fig.update_layout(
        title='Number of Delayed Flights by Airline and Month',
        xaxis_title='Month',
        yaxis_title='Number of Delayed Flights', 
        height = 800,
        font=dict(size=16),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1),
    )
    #plot_div = fig.to_html(full_html=False)
    fig.write_html("templates/delay_flight_airline_month.html")
    return render_template('delay_flight_airline_month.html')

# Define a route to show the average taxi-out time by airline and month
@app.route('/avg_taxi_out_time_by_airline_and_month', methods = ["GET","POST"])
def avg_taxi_out_time_by_airline_and_month():
    query = '''
    SELECT BRANDED_CODE_SHARE, MONTH_OF_YEAR, AVG(TAXI_OUT) AS AVG_TAXI_OUT_TIME
    FROM FLIGHT_TABLE
    WHERE TAXI_OUT IS NOT NULL AND BRANDED_CODE_SHARE NOT IN ('AA_CODESHARE', 'AS_CODESHARE', 'B6_CODESHARE', 'DL_CODESHARE', 'F9_CODESHARE', 'G4_CODESHARE', 'HA_CODESHARE', 'NK_CODESHARE', 'UA_CODESHARE', 'WN_CODESHARE')
    GROUP BY BRANDED_CODE_SHARE, MONTH_OF_YEAR
    '''
    df = execute_query(query)
    airlines = sorted(df['BRANDED_CODE_SHARE'].unique())
    data = []
    for airline in airlines:
        airline_df = df[df['BRANDED_CODE_SHARE'] == airline]
        trace = go.Bar(
            x=airline_df['MONTH_OF_YEAR'],
            y=airline_df['AVG_TAXI_OUT_TIME'],
            name=airline,
        )
        data.append(trace)
    fig = go.Figure(data=data)
    fig.update_layout(
        title='Average Taxi-Out Time by Airline and Month',
        xaxis_title='Month',
        yaxis_title='Average Taxi-Out Time (minutes)',
        height = 800,
        font=dict(size=16),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1),
    )
    #plot_div = fig.to_html(full_html=False)
    fig.write_html("templates/taxi_out.html")
    return render_template('taxi_out.html')

# Run the app
if __name__ == '__main__':
    app.run(debug=True)