import requests
import pandas as pd
import time
from datetime import datetime

# Caminhos dos arquivos
csv_file = "transactional-sample.csv"
output_file = "transactional-results.csv"
denied_file = "denied_logs.csv"
logs_file = "logs.csv"

# URL da API
url = "http://127.0.0.1:8000/recommend"

# Ler o CSV de entrada
df = pd.read_csv(csv_file)

# Listas para armazenar resultados
results = []
denied = []
logs = []

for idx, row in df.iterrows():
    tx_data = row.fillna({
        'transaction_id': 0,
        'merchant_id': 0,
        'user_id': 0,
        'card_number': 'unknown',
        'transaction_date': pd.Timestamp.now().isoformat(),
        'transaction_amount': 0.0,
        'device_id': 0
    })

    try:
        payload = {
            "transaction_id": int(tx_data['transaction_id']),
            "merchant_id": int(tx_data['merchant_id']),
            "user_id": int(tx_data['user_id']),
            "card_number": str(tx_data['card_number']),
            "transaction_date": pd.to_datetime(tx_data['transaction_date']).isoformat(),
            "transaction_amount": float(tx_data['transaction_amount']),
            "device_id": int(tx_data['device_id'])
        }
    except Exception as e:
        print(f"Fail to prepare transaction {row['transaction_id']}: {e}")
        logs.append({
            "transaction_id": row.get("transaction_id", None),
            "status": "error_preparing",
            "recommendation": None,
            "reason": str(e),
            "timestamp": datetime.now().isoformat()
        })
        continue

    try:
        response = requests.post(url, json=payload)

        if response.status_code == 200:
            result = response.json()
            recommendation = result.get('recommendation', 'unknown')
            reason = result.get('reason', 'N/A')

            results.append({**payload, "recommendation": recommendation, "reason": reason})
            logs.append({
                "transaction_id": payload['transaction_id'],
                "status": "success",
                "recommendation": recommendation,
                "reason": reason,
                "timestamp": datetime.now().isoformat()
            })

            if recommendation.lower() == "deny":
                denied.append({**payload, "recommendation": recommendation, "reason": reason})

            print(f"Transaction {payload['transaction_id']} processed: {recommendation} - {reason}")

        else:
            logs.append({
                "transaction_id": payload['transaction_id'],
                "status": f"error_{response.status_code}",
                "recommendation": None,
                "reason": response.text,
                "timestamp": datetime.now().isoformat()
            })
            print(f"Error in transaction {payload['transaction_id']}: {response.text}")

    except Exception as e:
        logs.append({
            "transaction_id": payload['transaction_id'],
            "status": "failed_request",
            "recommendation": None,
            "reason": str(e),
            "timestamp": datetime.now().isoformat()
        })
        print(f"Fail to send transaction {payload['transaction_id']}: {e}")

    time.sleep(0.2)  # Mantido para estabilidade

# Salvar resultados
if results:
    pd.DataFrame(results).to_csv(output_file, index=False)
if denied:
    pd.DataFrame(denied).to_csv(denied_file, index=False)
if logs:
    pd.DataFrame(logs).to_csv(logs_file, index=False)

print("\nProcessamento concluído.")
print(f"Resultados salvos em: {output_file}")
print(f"Transações negadas salvas em: {denied_file}")
print(f"Logs completos salvos em: {logs_file}")
