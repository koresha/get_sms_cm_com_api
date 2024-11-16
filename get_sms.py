import requests
import mysql.connector
from datetime import datetime, timedelta, timezone

# Configuration
API_URL = "https://api.cmtelecom.com/v1.2/transactions/"
HEADERS = {
    "X-CM-PRODUCTTOKEN": "PRODUCTTOKEN"
}
DB_CONFIG = {
    "host": "x.x.x.x",  
    "user": "user",
    "password": "password",
    "database": "database"
}


end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=1)

startdate = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
enddate = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

print("Récupération des logs SMS des dernières 24 heures...")
response = requests.get(
    f"{API_URL}?startdate={startdate}&enddate={enddate}",
    headers=HEADERS
)

if response.status_code == 200:
    data = response.json()
    sms_logs = data.get("result", [])

    if sms_logs:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        for sms in sms_logs:
            # Vérification et gestion des valeurs manquantes
            user_id = sms.get("customgrouping") or "UNKNOWN"  # Défaut "UNKNOWN" si absent ou NULL
            phone_number = sms.get("recipient", "UNKNOWN")  # Défaut "UNKNOWN"
            status = sms.get("statusdescription", "UNKNOWN")  # Défaut "UNKNOWN"
            send_date = sms.get("created")  # Obligatoire, erreur si absent
            
            if not send_date:
                print(f"Erreur: 'send_date' manquant pour une entrée : {sms}")
                continue

            query = """
                INSERT INTO sms_logs (send_date, user_id, phone_number, status, retry_count, retry_status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (
                send_date, 
                user_id, 
                phone_number, 
                status, 
                0,  
                None  
            )
            cursor.execute(query, values)

        connection.commit()
        print("Insertion des logs dans la base de données terminée.")

        cursor.close()
        connection.close()
    else:
        print("Aucun log trouvé pour les dernières 24 heures.")

else:
    print(f"Erreur lors de l'appel API : {response.status_code}")
    print(response.text)
