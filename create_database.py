import sys
import io
import sqlite3
import urllib.request
import csv

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
		sql.execute("CREATE TABLE schedule(id INTEGER PRIMARY KEY, freq INTEGER, tstart INTEGER, tstop INTEGER, days TEXT, home TEXT, name TEXT, lang TEXT, target TEXT, txsite TEXT, persistence INTEGER, stdate TEXT, spdate TEXT)")
		db.commit()

		# Grabbing the latest schedule.
		eibi_season = input("What is the EIBI season code of the schedule you wish to download (ie A18)? ")
		eibi_url = "http://eibispace.de/dx/sked-" + eibi_season.lower() + ".csv"
		print("Fetching database from " + eibi_url)

		with urllib.request.urlopen(eibi_url) as csv_file:
			# Open the CSV database. Remember that it is actually separated by semi-colons.
			reader = csv.reader(io.TextIOWrapper(csv_file), delimiter = ";", skipinitialspace = True)
			for row in reader:
				print(", ".join(row))
	except Exception as e:
		# Rollback changes if some shit went down.
		db.rollback()
		raise e
	finally:
		# Close the database connection.
		db.close()
