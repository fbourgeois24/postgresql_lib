""" Gestion d'une base de données postgresql
	Pour installer la librairie : 'pip install psycopg2-binary'

"""

import psycopg2
import psycopg2.extras
import os
import platform
from pythonping import ping #Installer avec 'pip install pythonping'
import logging as log



class postgresql_database:
	""" Classe pour la gestion de la DB """

	def __init__(self, db_name=None, db_server=None, db_port="5432", db_user="postgres", db_password = "", GUI=False, sslmode="allow", options = "", config=None):
		""" sslmode | valeurs possibles: disable, allow, prefer, require, verify-ca, verify-full 
			options peut servir à chercher dans un schéma particulier : options="-c search_path=dbo,public")
			config: Premet de passer toute la config via un dictionnaire. Les clés sont:
				- name
    			- addr
    			- port
    			- user
    			- passwd
		"""
		if (db_name is None or db_server is None) and config is None:
			raise AttributeError("Vous devez spécifier le db_name et le db_server ou passer une config")

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

		if config is not None:
			self.database = config.get("name")
			self.host = config.get("addr")
			self.port = config.get("port")
			self.user = config.get("user")
			self.password = config.get("passwd")
			self.sslmode = config.get("sslmode", "allow")
			self.options = config.get("options", "")


	def connect(self):
		""" Méthode pour se connecter à la base de données
			On commence par pinguer la db
		"""
		# Ping de la db
		# if "indows" in platform.system(): # Pas de premier w comme ça qu'il soit maj ou min ça ne change rien
		# 	command = "ping -n 1 "
		# 	redirect = ""
		# else:
		# 	command = "ping -c 1 "
		# 	redirect = " >/dev/null"
		# # Si le ping ne passe par on s'arrête là
		# if os.system(command + self.host + redirect) != 0:
		if "Request timed out" in ping(self.host, count=1):
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

	def open(self, auto_connect=True, fetch_type='tuple'):
		""" Méthode pour créer un curseur """
		if auto_connect:
			self.connect()
		# On essaye de fermer le curseur avant d'en recréer un 
		try:
			self.cursor.close()
		except:
			pass

		if fetch_type in ('tuple', 'list'):
			self.cursor = self.db.cursor()
		elif fetch_type in ('dict', 'dict_name'):
			self.cursor = self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		else:
			raise ValueError("Incorrect fetch_type")

		if self.cursor is not None:
			return True
		else:
			return False


	def commit(self):
		""" Méthode qui met à jour la db """
		self.db.commit()


	def close(self, commit = False, auto_connect=True):
		""" Méthode pour détruire le curseur, avec ou sans commit """
		# Si commit demandé à la fermeture
		if commit:
			self.db.commit()
		self.cursor.close()
		if auto_connect:
			self.disconnect()
		


	def execute(self, query, params = None):
		""" Méthode pour exécuter une requête mais qui gère les drop de curseurs """
		self.cursor.execute(query, params)


	def exec(self, query, params = None, fetch = "all", auto_connect=True, fetch_type='tuple'):
		""" Méthode pour exécuter une requête et qui ouvre et ferme  la db automatiquement """
		# Détermination du commit
		if not "SELECT" in query.upper()[:20]:
			commit = True
		else:
			commit = False
		if self.open(auto_connect=auto_connect, fetch_type=fetch_type):
			self.execute(query, params)
			# Si pas de commit ce sera une récupération
			if not commit:	
				# S'il faut récupérer les titres
				if fetch_type == "dict_name":
					fetch_title = True
				else:
					fetch_title = False
				# Type de récupération des données
				if fetch == "all":
					value = self.fetchall()
					if fetch_title:
						value = self.extract_title(value, 'all')
				elif fetch == "one":
					value = self.fetchone()
					if fetch_title:
						value = self.extract_title(value, 'one')
				elif fetch == "single":
					value = self.fetchone()
					if fetch_title:
						value = self.extract_title(value, 'single')
					elif value is not None:
						value = value[0]
				elif fetch == 'list':
					# On renvoie une liste composée du premier élément de chaque ligne
					value = [item[0] for item in self.fetchall()]
				else:
					raise ValueError("Wrong fetch type")
				self.close(auto_connect=auto_connect)
				# Si fetch_type == 'list' on transforme le tuple en liste
				if fetch_type == "list":
					if fetch == "all":
						value = [list(item) for item in value]
					elif fetch in ("one", "single"):
						value = list(value)
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


	def replace_none_list(self, liste):
		""" Remplacer les None contenus dans la liste par une string vide """
		# On regarde si c'est une liste à un ou deux niveaux
		level = 1
		if type(liste[0]) == list:
			level = 2

		for seq, item in enumerate(liste):
			if level == 1:
				if item == None:
					liste[seq] = ""
			elif level == 2:
				for sub_seq, sub_item in enumerate(item):
					if sub_item == None:
						liste[seq][sub_seq] = ""

		return liste

	def extract_title(self, value, fetch):
		""" On extrait les titres du résultat et on renvoie le bon type de donnée en fonction du fetch 
			on renvoie une liste dont le premier élément sera une liste avec les titres des colonnes
			et le 2e élément sera une liste avec les données
		"""
		result = []

		if fetch == "all":
			# Si pas de données renvoyées
			if value != []:
				result.append(value[0].keys())
				result.append(self.replace_none_list([list(row.values()) for row in value]))
			else:
				result = value
		elif fetch == "one":
			result.append(value.keys())
			result.append(self.replace_none_list(list(value.values())))
		elif fetch == "single":
			result.append(list(value.keys())[0])
			result.append(self.replace_none_list(list(value.values()))[0])

		return result









