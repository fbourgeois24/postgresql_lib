# postgresql_lib

## Import
```python
import postgresql_lib
```

## Instanciation
```python
db = postgresql_lib.postgresql_database(db_name = '', db_server = '', db_port = '', db_user = '', db_password='')
# OU
db = postgresql_lib.postgresql_database(config='') # Dans le cas de l'utilisation des secrets
```