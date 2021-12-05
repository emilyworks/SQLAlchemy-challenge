#import dependencies
 
import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#set up database connections

engine = create_engine("sqlite:////Users/emilyye/BOOTCAMP WORK/upenn-phi-virt-data-09-2021-u-c/10-Advanced-Data-Storage-and-Retrieval/Homework/Instructions/Resources/hawaii.sqlite", echo=False)

Base = automap_base()
Base.prepare(engine, reflect=True)

measurement = Base.classes.measurement
station = Base.classes.station

# session = Session(engine)

# Set up flask

climateapp = Flask(__name__)

# Home page
@climateapp.route('/')
def Home():
    return (
        f"Welcome to the home page!<br>"
        f"Available Routes:<br>"
        f"/api/v1.0/precipitation<br>"
        f"/api/v1.0/stations<br>"
        f"/api/v1.0/tobs<br>"
        f"/api/v1.0/start<br>"
        f"/api/v1.0/start/end"
    )
@climateapp.route("/precipitation")
def precipitation():
    session = Session(engine)

    sel = [measurement.date, measurement.prcp]
    query = session.query(*sel).\
    filter((func.strftime('%Y-%m-%d',measurement.date) >='2016-08-23'), func.strftime('%Y-%m-%d',measurement.date) <='2017-08-23').all()
    
    session.close()

    dict = {}

    for x in query:
        dict[x[0]] = str(x[1])

    return dict

@climateapp.route("/stations")
def stations():
    session = Session(engine)

    station = Base.classes.station

    query_stations = session.query(station.station, station.name, station.latitude, station.longitude, station.elevation).all()

    session.close()

    all_stations = []

    for station, name, latitude, longitude, elevation in query_stations:
        station_dict = {}
        station_dict['station'] = station
        station_dict['name'] = name
        station_dict['latitude'] = latitude
        station_dict['longitude'] = longitude
        station_dict['elevation'] = elevation
        all_stations.append(station_dict)

    return jsonify(all_stations)

@climateapp.route("/tobs")
def tobs():
    session = Session(engine)

    query_temp = session.query(measurement.date, measurement.tobs).\
    filter(measurement.station =='USC00519281').\
    filter(measurement.date >= '2016-08-18').\
    order_by(measurement.date.desc()).all()

    session.close()

    temp_list = []

    for date, tobs in query_temp:
        temp_dict = {}
        temp_dict['date'] = date
        temp_dict['tobs'] = tobs
        temp_list.append(temp_dict)

    return jsonify(temp_list)

@climateapp.route("/<start_date>")
def temp_start(start_date):

    session = Session(engine)

    input_date = dt.datetime.strptime(str(start_date), '%Y-%m-%d')
    input_date = input_date.date().isoformat()

    max_temp = session.query(measurement.station, measurement.date, measurement.tobs).\
    filter(measurement.date >= input_date).order_by((measurement.tobs).desc()).first()
    max_temp_dict = {
        'station': max_temp[0],
        'date': max_temp[1],
        'max tobs': max_temp[2]
    }

    min_temp = session.query(measurement.station, measurement.date, measurement.tobs).\
    filter(measurement.date >= input_date).order_by((measurement.tobs).asc()).first()
    min_temp_dict = {
        'station': min_temp[0],
        'date': min_temp[1],
        'min tobs': min_temp[2]
    }


    avg_temp = session.query(measurement.station, func.avg(measurement.tobs)).\
    filter(measurement.station >= input_date).all()    
    avg_temp_dict = {
        'station': avg_temp[0][0],
        'average tobs': avg_temp[0][1]
    }

    session.close()

    temp_stats = [max_temp_dict, min_temp_dict, avg_temp_dict]

    return jsonify(temp_stats)

@climateapp.route("/<start_date>/<end_date>")
def temp_start_se(start_date, end_date):

    session = Session(engine)

    input_start = dt.datetime.strptime(start_date, '%Y-%m-%d')
    input_start = input_start.date().isoformat()

    input_end = dt.datetime.strptime(end_date, '%Y-%m-%d')
    input_end = input_end.date().isoformat()

    max_temp_se = session.query(measurement.station, measurement.date, measurement.tobs).\
    filter(measurement.date >= input_start).\
    filter(measurement.date <= input_end).\
    order_by((measurement.tobs).desc()).first()
    max_temp_dict_se = {
        'station': max_temp_se[0],
        'date': max_temp_se[1],
        'max tobs': max_temp_se[2]
    }

    min_temp_se = session.query(measurement.station, measurement.date, measurement.tobs).\
    filter(measurement.date >= input_start).\
    filter(measurement.date <= input_end).\
    order_by((measurement.tobs).asc()).first()
    min_temp_dict_se = {
        'station': min_temp_se[0],
        'date': min_temp_se[1],
        'min tobs': min_temp_se[2]
    }


    avg_temp_se = session.query(measurement.station, func.avg(measurement.tobs)).\
    filter(measurement.station >= input_start).\
    filter(measurement.date <= input_end).all()    
    avg_temp_dict_se = {
        'station': avg_temp_se[0][0],
        'average tobs': avg_temp_se[0][1]
    }

    session.close()
    
    temp_stats_se = [max_temp_dict_se, min_temp_dict_se, avg_temp_dict_se]

    return jsonify(temp_stats_se)


#closing
if __name__ == "__main__":
    climateapp.run(debug=True)