import sys
import io
import sqlite3
import urllib.request
import csv
import re

# Parses the EIBI CSV schedule row.
def parse_schedule(row):
	schedule = {
		"freq": float(row[0]),
		"tstart": int(row[1].split("-")[0]),
		"tstop": int(row[1].split("-")[1]),
		"days": row[2],
		"home": row[3],
		"name": row[4],
		"lang": row[5],
		"target": row[6],
		"txsite": row[7],
		"persistence": int(row[8]),
		"stdate": row[9],
		"spdate": row[10]
	}

	# Make the days field a little bit more uniform.
	if not any(char.isdigit() for char in schedule["days"]):
		# This is a string with no digits in it.
		if re.search(r"Mo|Tu|We|Th|Fr|Sa|Su", schedule["days"]):
			# Convert day strings to numbers.
			for day, num in { "Mo": "1", "Tu": "2", "We": "3", "Th": "4", "Fr": "5", "Sa": "6", "Su": "7" }.items():
				schedule["days"] = schedule["days"].replace(day, num)

			# Remove the commas if there are any.
			schedule["days"] = schedule["days"].replace(",", "")

			# Expand the days if there are any to expand (ie Mo-Tu).
			rng = schedule["days"].split("-")
			if len(rng) is 2:
				schedule["days"] = "".join(str(n) for n in range(int(rng[0]), int(rng[1]) + 1))

	return schedule

# Main program.
if __name__ == "__main__":
	try:
		# Open the database and get a cursor.
		print("Opening database: schedule.db")
		db = sqlite3.connect("schedule.db")
		sql = db.cursor()

		# Clean the database if necessary.
		sql.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schedule'")
		if sql.fetchone():
			resp = input("Older schedule found. Do you wish to dump it and update with a new one? ")
			if "yes".startswith(resp.lower()):
				print("Cleaning up the schedule...")
				sql.execute("DROP TABLE schedule")
				db.commit()
				print("Schedule clean up finished.")
			else:
				print("Nothing to do then. Bye!")
				sys.exit(0)

		# Create the main schedule table.
		print("Creating the schedule table...")
		sql.execute("CREATE TABLE schedule(id INTEGER PRIMARY KEY, freq REAL, tstart INTEGER, tstop INTEGER, days TEXT, home TEXT, name TEXT, lang TEXT, target TEXT, txsite TEXT, persistence INTEGER, stdate TEXT, spdate TEXT)")
		db.commit()

		# Grabbing the latest schedule.
		eibi_season = input("What is the EIBI season code of the schedule you wish to download (ie A18)? ")
		eibi_url = "http://eibispace.de/dx/sked-" + eibi_season.lower() + ".csv"
		print("Fetching database from " + eibi_url)

		with urllib.request.urlopen(eibi_url) as csv_file:
			# Open the CSV database. Remember that it is actually separated by semi-colons.
			reader = csv.reader(io.TextIOWrapper(csv_file), delimiter = ";", skipinitialspace = True)

			# Skip the header of the CSV.
			next(reader)

			# Parse each row and put it in the database.
			for row in reader:
				schedule = parse_schedule(row)
				print("Adding schedule for " + schedule["name"])
				sql.execute("INSERT INTO schedule(freq, tstart, tstop, days, home, name, lang, target, txsite, persistence, stdate, spdate) VALUES(:freq, :tstart, :tstop, :days, :home, :name, :lang, :target, :txsite, :persistence, :stdate, :spdate)", schedule)

			# Commit all my inserts!
			db.commit()
	except Exception as e:
		# Rollback changes if some shit went down.
		db.rollback()
		raise e
	finally:
		# Close the database connection.
		db.close()
