""" Gestion d'une base de données postgresql
	Pour installer la librairie : 'pip install psycopg2-binary'

"""

import psycopg2
import os
import platform



class postgresql_database:
	""" Classe pour la gestion de la DB """

	def __init__(self, db_name, db_server, db_port="5432", db_user="postgres", db_password = "", GUI=False, sslmode="allow", options = ""):
		""" sslmode | valeurs possibles: disable, allow, prefer, require, verify-ca, verify-full 
			options peut servir à chercher dans un schéma particulier : options="-c search_path=dbo,public")
		"""
		self.db = None
		self.cursor = None
		self.database = db_name
		self.host = db_server
		self.port = db_port
		self.user = db_user
		self.password = db_password
		self.GUI = GUI
		self.sslmode = sslmode
		self.options = options


	def connect(self):
		""" Méthode pour se connecter à la base de données
			On commence par pinguer la db
		"""
		# Ping de la db
		if "indows" in platform.system(): # Pas de premier w comme ça qu'il soit maj ou min ça ne change rien
			command = "ping -n 1 "
		else:
			command = "ping -c 1 "
		# Si le ping ne passe par on s'arrête là
		if os.system(command + self.host) != 0:
			return False
		# Si le ping est passé on essaie de se connecter à la db
		self.db = psycopg2.connect(host = self.host, port = self.port, database = self.database, user = self.user, password = self.password, sslmode = self.sslmode, options = self.options)
		if self.db is None:
			return False
		else:
			return True

	def disconnect(self):
		""" Méthode pour déconnecter la db """
		self.db.close()

	def open(self):
		""" Méthode pour créer un curseur """
		self.connect()
		# On essaye de fermer le curseur avant d'en recréer un 
		try:
			self.cursor.close()
		except:
			pass
		self.cursor = self.db.cursor()
		if self.cursor is not None:
			return True
		else:
			return False


	def commit(self):
		""" Méthode qui met à jour la db """
		self.db.commit()


	def close(self, commit = False):
		""" Méthode pour détruire le curseur, avec ou sans commit """
		# Si commit demandé à la fermeture
		if commit:
			self.db.commit()
		self.cursor.close()
		self.disconnect()
		


	def execute(self, query, params = None):
		""" Méthode pour exécuter une requête mais qui gère les drop de curseurs """
		self.cursor.execute(query, params)


	def exec(self, query, params = None, fetch = "all"):
		""" Méthode pour exécuter une requête et qui ouvre et ferme  la db automatiquement """
		# Détermination du commit
		if not "SELECT" in query[:20]:
			commit = True
		else:
			commit = False
		if self.open():
			self.cursor.execute(query, params)
			# Si pas de commit ce sera une récupération
			if not commit or "RETURNING" in query:	
				if fetch == "all":
					value = self.fetchall()
				elif fetch == "one":
					value = self.fetchone()
				else:
					raise ValueError("Wrong fetch type")
				self.close()
				return value
			else:
				self.close(commit=commit)
		else:
			raise AttributeError("Erreur de création du curseur pour l'accès à la db")


	def fetchall(self):
		""" Méthode pour le fetchall """

		return self.cursor.fetchall()


	def fetchone(self):
		""" Méthode pour le fetchone """

		return self.cursor.fetchone()


	def dateToPostgres(self, date):
		""" Méthode pour convertir une date au format JJ/MM/AAAA au format AAAA-MM-JJ pour l'envoyer dans la db """
		# print(date.split("/"))
		return str(date.split("/")[2]) + "-" + str(date.split("/")[1] + "-" + str(date.split("/")[0]))













