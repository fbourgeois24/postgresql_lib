import os
# Si problème d'import des librairies, on essaie de les installer
try:
	from postgresql_lib.postgresql_lib import postgresql_database
except:
	
	os.popen(f"pip install --no-cache-dir -r {os.path.dirname(os.path.realpath(__file__))}/requirements.txt").read()
	from postgresql_lib.postgresql_lib import postgresql_database