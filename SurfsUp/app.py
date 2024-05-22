# Import the dependencies.
import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement 
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available API routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitations<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitations")
def precipitations():
    """Convert the query results from your precipitation analysis (i.e. retrieve only the last 12 months of data) to a dictionary"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    # Starting from the most recent data point in the database. Convert it to the right format
    formatted_most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')

    # Calculate the date one year from the last date in the data set.
    date_one_year_ago = formatted_most_recent_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    precipitation_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= date_one_year_ago).order_by(Measurement.date).all()

    session.close()

    # Convert list of tuples into a dictionary
    precipitation_dict = {date: prcp for date, prcp in precipitation_data}

    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all stations
    stations_all = session.query(Station.station).all()
    
    session.close()

    # Convert list of tuples into normal list
    station_list = list(np.ravel(stations_all))

    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return a JSON list of temperature observations for the previous year of the most active station"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query to find the most active station
    most_active_station = session.query(Measurement.station, func.count(Measurement.station).label('count')).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
    
    # Unpack the most active station id
    most_active_station_id = most_active_station.station
    
    # Find the most recent date in the data set.
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    # Starting from the most recent data point in the database. Convert it to the right format
    formatted_most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')

    # Calculate the date one year from the last date in data set.
    date_one_year_ago = formatted_most_recent_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and temperature scores for the most active station
    temperature_data = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= date_one_year_ago).filter(Measurement.station == most_active_station_id).order_by(Measurement.date).all()
    
    session.close()

    # Convert list of tuples into a dictionary
    temperature_dict = {date: tobs for date, tobs in temperature_data}

    return jsonify(temperature_dict)

@app.route("/api/v1.0/<start>")
def start_date(start):
    """Return Tmin, Tavg, Tmax for all dates greater than or equal to the start date"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Convert the start date string to a datetime object
    start_date = dt.datetime.strptime(start, '%Y-%m-%d')

    # Query to calculate Tmin, Tavg, Tmax
    results = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(Measurement.date >= start_date).all()
    
    session.close()

    # Convert the query results to a list
    temps = list(np.ravel(results))

    return jsonify({
        "Tmin": temps[0],
        "Tavg": temps[1],
        "Tmax": temps[2]
    })

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    """Return Tmin, Tavg, Tmax for dates from the start date to the end date inclusive"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Convert the start and end date strings to datetime objects
    start_date = dt.datetime.strptime(start, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end, '%Y-%m-%d')

    # Query to calculate Tmin, Tavg, Tmax
    results = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    session.close()

    # Convert the query results to a list
    temps = list(np.ravel(results))

    return jsonify({
        "Tmin": temps[0],
        "Tavg": temps[1],
        "Tmax": temps[2]
    })

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)


