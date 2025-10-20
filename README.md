# Fraud Detection System — SQL + Python API

                                  © 2025 André Borghi — Data & Software Engineering

A complete anti-fraud pipeline combining **SQL analytics** and a **real-time Python API** to identify potentially fraudulent credit card transactions.

---

## Project Context

The project simulates real-world payment processing to detect fraud. In a typical acquiring ecosystem:

- **Money flow & information flow:**  
  Funds move from the customer’s bank to the merchant via the acquiring bank, while transaction information is exchanged for authorization and settlement.  

- **Roles of main players:**  
  - **Acquirer:** Bank or institution processing merchant transactions  
  - **Sub-acquirer:** Partner handling smaller merchants under an acquirer  
  - **Payment gateway:** Technology layer transmitting transaction info for authorization  

- **Chargebacks vs cancellations:**  
  - **Chargeback:** Customer disputes a transaction after it’s authorized; often linked to fraud  
  - **Cancellation:** Transaction voided before settlement; usually not fraud-related  

The challenge focuses on analyzing transaction data to identify suspicious patterns and automatically recommend whether to approve or deny transactions.

Components:

- SQL analytical layer for fraud signal detection  
- FastAPI microservice for dynamic evaluation  
- Python script for batch testing and logging

---

## Project Structure

**antifraud folder:**

- `app.py` — FastAPI application (anti-fraud logic)  
- `transactional-sample.csv` — Input dataset  
- `transactional-results.csv` — Results with recommendations  
- `denied_logs.csv` — Denied transactions only  
- `logs.csv` — Full logs  
- `send_all_transactions.py` — Batch testing script  
- `fraud_detection.sql` — SQL views and analysis  
- `README.md` — Documentation  

---

## SQL Analysis Overview

**Goal:** Identify potential fraud patterns using pure SQL logic.  

**Data Source Example Columns:**

| Column               | Description |
|---------------------|-------------|
| `transaction_id`    | Unique transaction identifier |
| `merchant_id`       | Merchant performing the transaction |
| `user_id`           | Customer performing the transaction |
| `card_number`       | Card used in the transaction |
| `transaction_date`  | ISO timestamp |
| `transaction_amount`| Numeric transaction value |
| `device_id`         | Device identifier |
| `has_cbk`           | Chargeback flag (TRUE/FALSE) |

**Views:**

- `v_classified_transactions` — LOW / MED / HIGH by transaction amount  
- `v_rapid_transactions` — Detects consecutive transactions within 5 min  
- `v_fraud_summary` — Aggregates high-value, rapid, and chargeback indicators  

**Example SQL:**
```sql
SELECT *
FROM v_rapid_transactions
WHERE transaction_class = 'HIGH'
  AND (rapid_user_transaction = 1 OR rapid_device_transaction = 1)
  AND has_cbk = 1
ORDER BY transaction_ts;
```

---

## Python API — Real-Time Anti-Fraud

**Endpoint:** `POST /recommend`

**Example Request:**
```json
{
  "transaction_id": 21320398,
  "merchant_id": 29744,
  "user_id": 97051,
  "card_number": "434505******9116",
  "transaction_date": "2019-12-01T23:16:32.812632",
  "transaction_amount": 374.56,
  "device_id": 285475
}
```

**Example Response:**
```json
{
  "transaction_id": 21320398,
  "recommendation": "deny",
  "reason": "User has previous chargebacks and recent rapid transactions"
}
```

**Rules Implemented:**

| Rule                 | Description |
|---------------------|-------------|
| User frequency       | Deny if too many sequential transactions |
| Amount threshold     | Deny if above dynamic range |
| Previous chargebacks | Deny if user/card has past chargebacks |


Performance and security considerations were addressed: the API is lightweight, handles batch requests efficiently, and is designed to prevent unauthorized access.

---

## Automated Testing Script

`send_all_transactions.py`:

- Reads each transaction from `transactional-sample.csv`  
- Sends POST requests to `/recommend`  
- Saves results in CSVs:

| File                     | Description |
|--------------------------|-------------|
| `transactional-results.csv` | All transactions with recommendation |
| `denied_logs.csv`        | Only denied transactions |
| `logs.csv`               | Full request/response logs |

**Run Example:**
```bash
python send_all_transactions.py
```

---

## Results Example

| transaction_id | recommendation | reason |
|----------------|----------------|--------|
| 21320398       | deny           | High-value transaction after chargeback |
| 21320399       | approve        | Normal behavior pattern |
| 21320400       | deny           | Rapid user transactions detected |

---

## Future Enhancements

- Real database integration (PostgreSQL / MongoDB)  
- Docker deployment  
- Risk scoring for probabilistic evaluation  
- Machine learning to complement rule-based filtering  

---

## Conclusion

This project demonstrates a full fraud detection lifecycle:

- SQL analytics → real-time Python API → automated validation  
- Skills: Data analysis, API development, data engineering, batch automation  

A practical, modular, and extensible approach to detecting fraud in payment ecosystems.
