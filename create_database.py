import sys
import io
import sqlite3
import urllib.request
import csv
import re
import filecmp
import pprint

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

# Parses the language definition file.
def parse_language_def():
	langs = []

	with open("archive/LANGUAGES.TXT", "r") as f:
		for line in f:
			# Ignore the lines that start with a tab. They are just "see also" crap.
			if line[0] is "\t":
				continue

			# Check if it is a "non-language".
			if line[0] is "-":
				data = re.match(r"^(-\w+)\s+(.+)", line)
				langs.append({
					"code": data.group(1),
					"name": data.group(2),
					"info": "",
					"itu": ""
				})
				continue

			# Try to match the more general of definitions.
			data = re.match(r"^([\w-]{1,3})\s+(.+):\s(.+)\s+\[([\w,]+)\]", line)
			if data:
				langs.append({
					"code": data.group(1),
					"name": data.group(2),
					"info": data.group(3).strip(),
					"itu": data.group(4)
				})
				continue

			# Try to match the complete lines without the info field.
			data = re.match(r"^([\w-]{1,3})\s+(.+)\s+\[([\w,]+)\]", line)
			if data:
				langs.append({
					"code": data.group(1),
					"name": data.group(2),
					"info": "",
					"itu": data.group(3)
				})
				continue

			# The last kind, which is just the code and the name.
			data = re.match(r"^([\w-]{1,3})\s+(.+)", line)
			if data:
				langs.append({
					"code": data.group(1),
					"name": data.group(2),
					"info": "",
					"itu": ""
				})
				continue

	return langs

i = 1
for lang in parse_language_def():
	print(str(i) + ":")
	pprint.pprint(lang)
	i += 1

# Main program.
if False:#__name__ == "__main__":
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
		eibi_season = input("What is the EIBI season code of the schedule you wish to download (ie A18)? ").lower()
		eibi_url = "http://eibispace.de/dx/sked-" + eibi_season + ".csv"
		print("Fetching database from " + eibi_url)

		# Fetch database and save to archive.
		urllib.request.urlretrieve(eibi_url, "archive/sked-" + eibi_season + ".csv")

		# Open the CSV database. Remember that it is actually separated by semi-colons.
		reader = csv.reader(open("archive/sked-" + eibi_season + ".csv", newline = ""), delimiter = ";", skipinitialspace = True)

		# Skip the header of the CSV.
		next(reader)

		# Parse each row and put it in the database.
		for row in reader:
			schedule = parse_schedule(row)
			print("Adding schedule for " + schedule["name"])
			sql.execute("INSERT INTO schedule(freq, tstart, tstop, days, home, name, lang, target, txsite, persistence, stdate, spdate) VALUES(:freq, :tstart, :tstop, :days, :home, :name, :lang, :target, :txsite, :persistence, :stdate, :spdate)", schedule)

		# Commit all my inserts!
		db.commit()

		# Download the EIBI README file.
		print("Downloading README.txt...")
		urllib.request.urlretrieve("http://eibispace.de/dx/README.TXT", "archive/README-" + eibi_season + ".txt")

		# Check if the downloaded README.txt and the previous one are the same.
		if filecmp.cmp("archive/README.txt", "archive/README-" + eibi_season + ".txt"):
			print("Parsing the languages definitions.")

			# TODO: Parse transmitter sites.
			# TX site regex: /^(\w\w?)-(.*)\s([0-9][\w\d\'\"]+)?-?([0-9][\w\d\'\"]+)?/g
			# TODO: Recreate the regex above for lines like this: F:   Issoudun 46N56-01E54 except:
		else:
			print("================================================================")
			print("= There is a new version of the EIBI README.TXT, please update =")
			print("= the LANGUAGES.TXT, COUNTRIES.TXT, TARGET_AREAS.TXT and TRANS =")
			print("= MITTERS.TXT files.                                           =")
			print("================================================================")
	except Exception as e:
		# Rollback changes if some shit went down.
		db.rollback()
		raise e
	finally:
		# Close the database connection.
		db.close()
