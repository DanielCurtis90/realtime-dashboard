import paho.mqtt.client as mqtt
import time, csv, ssl, json, math
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import threading
import numpy as np
import pandas as pd
import collections

from bokeh.plotting import gmap, figure
from bokeh.models import GMapOptions, ColumnDataSource, HoverTool, Circle, ImageURL, Plot, Text, Line, Title, LabelSet, Div
from bokeh.io import output_file, show, curdoc
from bokeh.layouts import column, row
from bokeh.palettes import d3
from bokeh.transform import factor_cmap

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def bearing_conversion(bearing):
    converted = 90 - bearing
    if converted < 0:
        converted += 360
    elif converted > 360:
        converted -= 360
    return converted

df = pd.DataFrame()
data_source_dict = {}
image_dict = {}
glyph_dict = {}
kpi_sources = 0
car_type_dict = {}
car_type_sources = 0
speed_plot_dict = {}
line_plot_sources = 0


#How many datapoints back are we looking
limiter = 30

# configuring the Google map 
lat = 40.74
lng = -73.97
map_type = "roadmap"
zoom = 12
google_map_options = GMapOptions(lat = lat, 
                                 lng = lng, 
                                 map_type = map_type, 
                                 zoom = zoom)          
# generating the Google map 
google_api_key = "YOUR KEY HERE!" 
title = "Real Time Taxi Data and Location"
google_map = gmap(google_api_key, 
                  google_map_options, 
                  title = title,
                  width = 1000,
                  height = 800)
google_map.yaxis[0].axis_label = 'Latitude'
google_map.xaxis[0].axis_label = 'Longitude' 
google_map.toolbar.logo = None

#Whitespace message plot
whitespace_plot = Plot(plot_width=40, plot_height=75, min_border=0, toolbar_location=None)
whitespace_plot.outline_line_color = None

header_text = Div(text="""<h1>GINQO Real-time Kafka Dashboard</h1>

Data streamed using Confluent Cloud and ksqlDB.<br>
Dashboard webserver built using Python.
""", 

width=1780, height=100)

#Plot for KPIs
kpi_dimension_h = 200
kpi_dimension_w = 300
kpi_drivers_plot = Plot(plot_width=kpi_dimension_w, plot_height=kpi_dimension_h, min_border=0, toolbar_location=None)
kpi_drivers_plot.title.text = 'Current Number of Drivers'
kpi_drivers_plot.title.align = 'center'
kpi_passengers_plot = Plot(plot_width=kpi_dimension_w, plot_height=kpi_dimension_h, min_border=0, toolbar_location=None)
kpi_passengers_plot.title.text = 'Current Passengers (Total)'
kpi_passengers_plot.title.align = 'center'
kpi_speed_plot = Plot(plot_width=kpi_dimension_w, plot_height=kpi_dimension_h, min_border=0, toolbar_location=None)
kpi_speed_plot.title.text = 'Current 1 Minute Avg Speed'
kpi_speed_plot.title.align = 'center'
kpi_enroute_plot = Plot(plot_width=kpi_dimension_w, plot_height=kpi_dimension_h, min_border=0, toolbar_location=None)
kpi_enroute_plot.title.text = 'Current Number of Enroute Taxis'
kpi_enroute_plot.title.align = 'center'
#Plot for active car type bar chart
car_types_list = ['Sedan', 'Coupe', 'Minivan', 'SUV']
car_type_fig = figure(x_range=car_types_list, plot_height=266, plot_width=500, title="Active Taxis by Type")
#Plot for average speed
average_speed_fig = figure(title="Average Taxi Speed (10 Minute Rolling)", plot_width=500, plot_height=266, min_border=0, x_axis_type='datetime')
#Plot for average meter increment
meterage_fig = figure(title="Average Meterage Increment (5 Second Rolling)", plot_width=500, plot_height=266, min_border=0, x_axis_type='datetime')

#Setup plots and dicts
#Car bar chart
car_type_sources = ColumnDataSource(data=dict(car_count=[], car_type=[]))
car_type_fig.vbar(x='car_type', top='car_count', width=0.9, source=car_type_sources, fill_color=factor_cmap('car_type', palette=d3['Category20c'][4], factors=car_types_list))
car_type_fig.xgrid.grid_line_color = None
car_type_fig.y_range.start = 0
car_type_fig.yaxis[0].axis_label = 'Number of Vehicles'
car_type_fig.xaxis[0].axis_label = 'Vehicle Type'
car_type_tooltips = [("Type", "@car_type"), ("Count", "@car_count")]
car_type_fig.add_tools(HoverTool(tooltips=car_type_tooltips, toggleable=False))
car_type_fig.toolbar.logo = None

#Average rolling 10 min speed line chart
line_plot_sources = ColumnDataSource(data=dict(time=[], avg_speed=[], avg_meterage=[]))
average_speed_fig.line(x='time', y='avg_speed', line_width=2, source=line_plot_sources, line_color="#cc4400")
average_speed_fig.xaxis[0].ticker.desired_num_ticks = 10
average_speed_fig.xaxis[0].axis_label = 'Datetime'
average_speed_fig.yaxis[0].axis_label = 'mph'
speed_tooltips = [("Datetime", "@time{%Y-%m-%d %H:%M:%S}"), ("mph", "@avg_speed{(0.000)}")]
average_speed_fig.add_tools(HoverTool(tooltips=speed_tooltips, toggleable=False, formatters={
        "@time": "datetime"
}))
average_speed_fig.toolbar.logo = None

#Average meter increment, rolling 5 seconds line chart
meterage_fig.line(x='time', y='avg_meterage', line_width=2, source=line_plot_sources, line_color="#004d00")
meterage_fig.xaxis[0].ticker.desired_num_ticks = 10
meterage_fig.xaxis[0].axis_label = 'Datetime'
meterage_fig.yaxis[0].axis_label = 'USD ($)'
meterage_tooltips = [("Datetime", "@time{%Y-%m-%d %H:%M:%S}"), ("USD", "@avg_meterage{(0.0000)}")]
meterage_fig.add_tools(HoverTool(tooltips=meterage_tooltips, toggleable=False, formatters={
        "@time" : "datetime"
}))
meterage_fig.toolbar.logo = None

#KPI number configuration
kpi_sources = ColumnDataSource(data=dict(total_average_speed1=[], driver_count=[], passenger_count=[], enroute_count=[]))

average_speed_glyph = Text(x=10, y=10, text='total_average_speed1', text_color="#cc4400", text_font_size='48pt', text_align='center', text_baseline='middle')
kpi_speed_plot.add_glyph(kpi_sources, average_speed_glyph)
kpi_speed_plot.outline_line_color = None

driver_count_glyph = Text(x=10, y=10, text='driver_count', text_color="#2952a3", text_font_size='48pt', text_align='center', text_baseline='middle')
kpi_drivers_plot.add_glyph(kpi_sources, driver_count_glyph)
kpi_drivers_plot.outline_line_color = None

passenger_count_glyph = Text(x=10, y=10, text='passenger_count', text_color="#004d4d", text_font_size='48pt', text_align='center', text_baseline='middle')
kpi_passengers_plot.add_glyph(kpi_sources, passenger_count_glyph)
kpi_passengers_plot.outline_line_color = None

enroute_glyph = Text(x=10, y=10, text='enroute_count', text_color="#2952a3", text_font_size='48pt', text_align='center', text_baseline='middle')
kpi_enroute_plot.add_glyph(kpi_sources, enroute_glyph)
kpi_enroute_plot.outline_line_color = None

doc = curdoc()

def update_taxi():
    global df
    global kpi_sources
    global google_map
    global hover_tool
    global car_type_sources
    global line_plot_sources
    global data_source_dict

    #Get the average meter increment
    enroute_df = df.tail(500).drop_duplicates(subset = "driver_driver_id", keep = 'last')
    enroute_num = len(enroute_df[enroute_df['ride_status'] == 'enroute'])
    #Get unique drivers and count the total passengers
    passenger_num = df.tail(500).drop_duplicates(subset = "driver_driver_id", keep = 'last')['passenger_count'].sum()
    #Add the current number of drivers to the driver count data storage
    #Check the last ~500 entries for uniqueness
    unique_df = df.tail(500).groupby('driver_driver_id')
    #How many drivers are active at this moment?
    num_drivers = unique_df.ngroups
    #Get the average speed across all drivers, we need to approximate this to each taxi getting data every second, so over 1 minute multiply by 60. For 10 minutes, multiply by 600.
    #For the 1 min KPI:
    kpi_sources.stream(dict(driver_count=[str(num_drivers)], total_average_speed1=[df['speed'].tail(num_drivers*60).mean().round(2)], passenger_count=[passenger_num], enroute_count=[enroute_num]), 1)
    #For the 10 minute average:
    average_speed = [df['speed'].tail(num_drivers*600).mean()]
    #Acquire rolling 5 second average meter increment
    meter_inc_avg = [df['meter_increment'].tail(num_drivers*5).mean()]
    now = [datetime.now()]
    line_plot_sources.stream(dict(time=now, avg_speed=average_speed, avg_meterage=meter_inc_avg))

    #Group by car type, add data to sources. 
    unique_df = df.tail(500).groupby('driver_car_class')
    num_types = unique_df.ngroups
    type_iterator = 0
    car_type_dict['car_count'] = []
    car_type_dict['car_type'] = []
    for car_class, sub_df in unique_df:
        #Drop duplicate drivers in our sample grab
        temp_df = sub_df.drop_duplicates(subset = "driver_driver_id", keep = 'last') 
        #Feed the source
        car_type_dict['car_count'].append(len(temp_df.index))
        car_type_dict['car_type'].append(car_class)
        type_iterator += 1
    
    car_type_sources.stream(car_type_dict, num_types)

    #Group by driver, create streams and plots for each taxi
    grouped_df = df.groupby('driver_driver_id')
    for driver, sub_df in grouped_df:
        if driver in data_source_dict:
            #Get the last row of info in the sub dataframe and covert it to a dictionary with no index
            temp_dict = sub_df.tail(1).to_dict('r')[0]
            #Change all the values to a list format.
            list_dict = {k: [v] for k, v in temp_dict.items()}
            #Stream the new list format values into the data source
            data_source_dict[driver].stream(list_dict, 1)
        else:
            #If driver doesn't exist yet, instantiate a column data source.
            data_source_dict[driver] = ColumnDataSource(data=dict(ride_id=[], information_source=[], angle=[], point_idx=[], latitude=[],longitude=[],heading=[],speed=[],meter_reading=[],meter_increment=[],ride_status=[],passenger_count=[],driver_driver_id=[],driver_first_name=[],driver_last_name=[],
                                                        driver_rating=[],driver_car_class=[],passenger_passenger_id=[],passenger_first_name=[],passenger_last_name=[],passenger_rating=[], timestamp=[]))
            #Instantiate a plot
            taxi_image = ImageURL(url=['mqtt-taxi/static/taxi2.png'], x='longitude', y='latitude', anchor="center", angle='angle', angle_units='deg')
            hover_image = Circle(x='longitude', y='latitude', size=15, fill_color="blue", fill_alpha=0, line_alpha=0)

            image_dict[driver] = google_map.add_glyph(data_source_dict[driver], taxi_image)
            glyph_dict[driver] = google_map.add_glyph(data_source_dict[driver], hover_image)
            hover_tooltips = [("Ride ID", "@ride_id"), ("Info Source", "@information_source"), ("Driver ID", "@driver_driver_id"), ("Latitude", "@latitude"), ("Longitude", "@longitude"), ("Heading", "@heading"), ("Speed", "@speed"), 
                    ("Timestamp", "@timestamp"), ("Meter Reading", "@meter_reading"), ("Ride Status", "@ride_status"), ("Passengers", "@passenger_count"), ("Car Type", "@driver_car_class"), 
                    ("Passenger ID", "@passenger_passenger_id")]
            google_map.add_tools(HoverTool(tooltips=hover_tooltips, renderers=[glyph_dict[driver]], toggleable=False))


def client_setup():
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        print("Connected with result code "+ str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("taxinyc/#")
        #client.subscribe("taxinyc/ops/ride/updated/v1/enroute/00000209/#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        #Decode the message sent back to our subscribed application
        decoded_record = str(msg.payload.decode('ascii'))
        global source
        global df
        #Decode the message sent back to our subscribed application
        js_dict = json.loads(decoded_record)
        #Flatten the nested dicts from the json file
        js_dict = flatten(js_dict)

        if "heading" in js_dict:
            js_dict['angle'] = bearing_conversion(js_dict['heading'])
            #Turn dict into a dataframe and append.
            js_df = pd.DataFrame([js_dict], columns=js_dict.keys())
            try:
                df = df.append(js_df)
            except IndexError:
                print('Message not properly indexed, discarding')
                pass
        else:
            print("Message discarded due to missing field: heading")
        
    
    #Define our solace url, User and PW (Note that these should be probably not be hardcoded in here)
    url = "URL HERE"
    username = "USERNAME HERE"
    password = "PASSWORD HERE"
    #Make an mqtt Client object
    sol_client = mqtt.Client() 
    #Give our client the user and password
    sol_client.username_pw_set(username=username, password=password)

    sol_client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS, ciphers=None)

    sol_client.on_connect = on_connect
    sol_client.on_message = on_message
    sol_client.connect(url, 8883) #connect to broker
    sol_client.loop_forever()

kpi_column = column(kpi_drivers_plot, kpi_enroute_plot, kpi_speed_plot, kpi_passengers_plot)
chart_column = column(car_type_fig, average_speed_fig, meterage_fig)
main_row = row(google_map, kpi_column, chart_column)
header_row = row(whitespace_plot, header_text)
final_col = column(header_row, main_row)
doc.add_root(final_col)
doc.title = "Taxi Data Demo"

thread = threading.Thread(target=client_setup)
thread.start()
time.sleep(3)



doc.add_periodic_callback(update_taxi, 500)




