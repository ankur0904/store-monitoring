from flask import Flask, jsonify, request, send_file
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import uuid
from sqlalchemy import text
from dateutil import tz
from datetime import datetime, timedelta
import os
import csv
from io import StringIO


app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///users.db"

db = SQLAlchemy(app)


class Status(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    store_id = db.Column(db.Integer)
    timestamp_utc = db.Column(db.DateTime)
    status = db.Column(db.String(10))


class StoreInfo(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    store_id = db.Column(db.Integer)
    day = db.Column(db.Integer)
    start_time_local = db.Column(db.Time)
    end_time_local = db.Column(db.Time)


class StoreTimeZone(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    store_id = db.Column(db.Integer)
    timezone = db.Column(db.String(20))


from datetime import datetime, time


def store_info():
    df2 = pd.read_csv("2.csv")
    for index, row in df2.iterrows():
        start_time_local = datetime.strptime(row["start_time_local"], "%H:%M:%S").time()
        end_time_local = datetime.strptime(row["end_time_local"], "%H:%M:%S").time()

        new_store_info = StoreInfo(
            store_id=int(row["store_id"]),
            day=int(row["day"]),
            start_time_local=start_time_local,
            end_time_local=end_time_local,
        )
        db.session.add(new_store_info)
    db.session.commit()


def store_status():
    df1 = pd.read_csv("1.csv")
    for index, row in df1.iterrows():
        timestamp_str = row["timestamp_utc"]
        formats = ["%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M:%S.%f UTC"]

        for fmt in formats:
            try:
                timestamp_utc = datetime.strptime(timestamp_str, fmt)
                break
            except ValueError:
                pass

        else:
            print(f"Could not parse timestamp: {timestamp_str}")
            continue

        new_status = Status(
            store_id=int(row["store_id"]),
            status=row["status"],
            timestamp_utc=timestamp_utc,
        )
        db.session.add(new_status)

    db.session.commit()


def store_timezone():
    df3 = pd.read_csv("3.csv")
    for index, row in df3.iterrows():
        new_store_timezone = StoreTimeZone(
            store_id=int(row["store_id"]), timezone=row["timezone_str"].strip()
        )
        db.session.add(new_store_timezone)
    db.session.commit()


@app.route("/trigger_report", methods=["POST"])
def trigger_report():
    report_id = str(uuid.uuid4())
    generate_report(report_id)
    return jsonify({"report_id": report_id})


def generate_report(report_id):
    # Get all store ids
    store_ids = [result[0] for result in db.session.query(Status.store_id).distinct()]

    # Define report directory path
    report_dir = "reports"
    os.makedirs(report_dir, exist_ok=True)  # Create the directory if it doesn't exist

    # Define report file path
    report_file_path = os.path.join(report_dir, f"{report_id}.csv")

    # Create a CSV file for the report
    with open(report_file_path, mode="w", newline="") as report_file:
        fieldnames = [
            "store_id",
            "uptime_last_hour",
            "uptime_last_day",
            "uptime_last_week",
            "downtime_last_hour",
            "downtime_last_day",
            "downtime_last_week",
        ]
        writer = csv.DictWriter(report_file, fieldnames=fieldnames)
        writer.writeheader()

        for store_id in store_ids:
            report_data = generate_store_report(store_id)
            print(report_data)
            writer.writerow(report_data)


def generate_store_report(store_id):
    print(f"Generating report for store {store_id}")
    store_info = StoreInfo.query.filter_by(store_id=store_id).first()
    if not store_info:
        print("Store info not found")
        return {}

    # Get business hours
    business_hours_start = datetime.combine(datetime.min, store_info.start_time_local)
    business_hours_end = datetime.combine(datetime.min, store_info.end_time_local)

    # Calculate time intervals
    current_time = datetime.now()
    last_hour_start = current_time - timedelta(hours=1)
    last_day_start = current_time - timedelta(days=1)
    last_week_start = current_time - timedelta(weeks=1)

    # Fetch status data for the store within the specified time intervals
    statuses = Status.query.filter(
        Status.store_id == store_id,
        Status.timestamp_utc >= last_week_start,
        Status.timestamp_utc <= current_time,
    ).all()

    print(f"Number of statuses for store {store_id}: {len(statuses)}")

    # Initialize variables
    uptime_last_hour = downtime_last_hour = uptime_last_day = downtime_last_day = (
        uptime_last_week
    ) = downtime_last_week = 0

    # Calculate uptime and downtime
    for status in statuses:
        print(f"Status timestamp: {status.timestamp_utc}")
        if (
            business_hours_start.time()
            <= status.timestamp_utc.time()
            <= business_hours_end.time()
        ):
            if status.status == "active":
                uptime_last_week += 1
                if status.timestamp_utc >= last_hour_start:
                    uptime_last_hour += 1
                if status.timestamp_utc >= last_day_start:
                    uptime_last_day += 1
            elif status.status == "inactive":
                downtime_last_week += 1
                if status.timestamp_utc >= last_hour_start:
                    downtime_last_hour += 1
                if status.timestamp_utc >= last_day_start:
                    downtime_last_day += 1

    print(f"Uptime last hour: {uptime_last_hour}")
    print(f"Downtime last hour: {downtime_last_hour}")

    uptime_last_hour /= 60
    downtime_last_hour /= 60
    uptime_last_day /= 60
    downtime_last_day /= 60
    uptime_last_week /= 60
    downtime_last_week /= 60

    report_data = {
        "store_id": store_id,
        "uptime_last_hour": uptime_last_hour,
        "uptime_last_day": uptime_last_day,
        "uptime_last_week": uptime_last_week,
        "downtime_last_hour": downtime_last_hour,
        "downtime_last_day": downtime_last_day,
        "downtime_last_week": downtime_last_week,
    }

    print("Report data:", report_data)

    return report_data


@app.route("/get_report", methods=["POST"])
def get_report():
    report_id = request.args.get("report_id")
    report_file_path = f"reports/{report_id}.csv"

    try:
        return send_file(report_file_path, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Report not found"}), 404


if __name__ == "__main__":
    with app.app_context():
        db.drop_all()
        db.create_all()
        store_status()
        store_info()
        store_timezone()
    app.run(debug=True, port=5000)
