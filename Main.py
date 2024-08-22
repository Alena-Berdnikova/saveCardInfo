import requests
import mysql.connector
import time

config = {
    'user': '***',
    'password': '***',
    'host': '***.amazonaws.com',
    'database': '***'
}

try:
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    sql = ("INSERT INTO output_tbl_berdnikova_python "
           "(name, numbers, subtypes, rules, attack_names) "
           "VALUES (%s, %s, %s, %s, %s)")

    for i in range(1, 151):
        try:
           response = requests.get("https://***/v2/cards/xy1-"+str(i))
           response.raise_for_status()
           json = response.json()
           name = json['data']['name']
           number = json['data']['number']
           subtypes = ', '.join(json['data'].get('subtypes', []))
           rules = ', '.join(json['data'].get('rules', []))
           attacks = ', '.join([attack['name'] for attack in json['data'].get('attacks', [])])

           cursor.execute(sql, (name, number, subtypes, rules, attacks))
           db.commit()
        except requests.HTTPError as e:
            if str(e).startswith('429'):
                i = -1
                time.sleep(1)
            else:
                print(f"Request failed for card xy1-{i}: {e}")

finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'db' in locals() and db:
        db.close()
