import vertica_python

connection_info = {'host': 'vertica.tgcloudenv.ru',
             'port': '5433',
             'user': 'stv2023081266',
             'database': 'dwh',
             'autocommit': False,
             }

vertica_user = connection_info['user']
