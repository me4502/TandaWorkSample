from flask import Flask, abort, g
import sqlite3
import sys
import json
import re
import dateutil
import dateutil.parser
import datetime

app = Flask(__name__)

# The database the data is being stored in.
DATABASE = 'pings.db'

# Regex to validate ISO Date formatting.
ISO_REGEX = re.compile("^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])"
                       "|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)"
                       "|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])"
                       "|(?:[2468][048]|[13579][26])00)-02-29)$")

# Seconds per day, to easily calculate end-time from beginning time for a single day.
SECONDS_PER_DAY = 60 * 60 * 24

# Default datetime, to act as a default for parsing.
DEFAULT_DATETIME = datetime.datetime.fromtimestamp(0)


# Initializes the database with the necessary tables, if they don't currently exist.
def initialize_schema():
    connection = None

    try:
        connection = sqlite3.connect(DATABASE)
        cursor = connection.cursor()

        with connection:
            cursor.execute("CREATE TABLE IF NOT EXISTS Devices (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                           "device_id CHAR(36) UNIQUE)")
            cursor.execute("CREATE TABLE IF NOT EXISTS PingTimes (id INTEGER, time BIGINT)")

        cursor.close()
    except sqlite3.Error as e:
        print("Failed to connect to SQLite Database, %s" % e.args[0])
        sys.exit(1)
    finally:
        if connection:
            connection.close()


# Grabs the database connection from the session, or creates a new one if needed.
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db


# Closes the database connection stored in the session.
# noinspection PyUnusedLocal
@app.teardown_appcontext
def close_connection(e):
    db = getattr(g, '_database', None)
    if db:
        db.close()


@app.route('/<device_id>/<epoch_time>', methods=['POST'])
def store_device(device_id=None, epoch_time=None):
    if not device_id or not epoch_time:
        # If one of these are not given for some reason, return a 400 error.
        abort(400)
    else:
        conn = get_db()
        cursor = get_db().cursor()
        with conn:
            cursor.execute("INSERT OR IGNORE INTO Devices (device_id) VALUES(?)", (device_id,))
            cursor.execute("INSERT INTO PingTimes (id, time) "
                           "SELECT id, ? FROM Devices WHERE device_id=?", (epoch_time, device_id))
        cursor.close()
        return "Added Result"


@app.route('/<device_id>/<date>', methods=['GET'])
def get_for_date(device_id=None, date=None):
    if not device_id or not date or not ISO_REGEX.match(date):
        # If one of these are not given for some reason, or the date given is not of ISO format, return a 400 error.
        abort(400)
    else:
        from_time = int(dateutil.parser.parse(date, default=DEFAULT_DATETIME).timestamp())
        to_time = from_time + SECONDS_PER_DAY
        return get_for_range(device_id, from_time, to_time)


@app.route('/<device_id>/<from_time>/<to_time>', methods=['GET'])
def get_for_range(device_id=None, from_time=None, to_time=None):
    # from is a reserved keyword in Python, call it 'from_time' instead
    if not device_id or not from_time or not to_time:
        # If one of these are not given for some reason, return a 400 error.
        abort(400)
    else:
        if isinstance(from_time, str) and ISO_REGEX.match(from_time):
            from_time = int(dateutil.parser.parse(from_time, default=DEFAULT_DATETIME).timestamp())
        if isinstance(to_time, str) and ISO_REGEX.match(to_time):
            to_time = int(dateutil.parser.parse(to_time, default=DEFAULT_DATETIME).timestamp())

        conn = get_db()
        cursor = get_db().cursor()

        with conn:
            if device_id == 'all':
                # If this is for all devices, use a dictionary - as that's what the test case requires.
                ping_times = dict()

                cursor.execute("SELECT device_id, time FROM PingTimes LEFT JOIN Devices ON PingTimes.id = Devices.id "
                               "WHERE time >= ? AND time < ?", (from_time, to_time))

                # Fill the device keys with a list of times.
                for row in cursor.fetchall():
                    if not row[0] in ping_times:
                        ping_times[row[0]] = list()
                    ping_times[row[0]].append(row[1])
            else:
                ping_times = list()

                cursor.execute("SELECT time FROM PingTimes LEFT JOIN Devices ON PingTimes.id = Devices.id "
                               "WHERE device_id=? AND time >= ? AND time < ?", (device_id, from_time, to_time))
                for row in cursor.fetchall():
                    ping_times.append(row[0])
        cursor.close()
        return json.dumps(ping_times)


@app.route('/clear_data', methods=['POST'])
def clear_data():
    conn = get_db()
    cursor = get_db().cursor()
    with conn:
        cursor.execute("DELETE FROM Devices")
        cursor.execute("DELETE FROM PingTimes")
        cursor.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='Devices'")  # Reset AUTOINCREMENT
        cursor.execute("VACUUM")  # Run VACUUM to ensure that any free space can be cleared.
    cursor.close()
    return "Cleared Tables"


@app.route('/devices', methods=['GET'])
def get_devices():
    conn = get_db()
    cursor = get_db().cursor()
    data = list()
    with conn:
        cursor.execute("SELECT device_id FROM Devices")
        for row in cursor.fetchall():
            data.append(row)
    cursor.close()
    return json.dumps(data)


# Call the schema initialization
initialize_schema()

# Run the webserver
if __name__ == '__main__':
    app.run(port=3000)
