from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import pandas as pd

app = FastAPI(title="AntiFraud")

# Load csv
try:
    transactions_df = pd.read_csv("transactional-sample.csv")
    transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date'])
except FileNotFoundError:
    transactions_df = pd.DataFrame()

# Logs
log_df = transactions_df.copy()
denied_log_df = pd.DataFrame(columns=transactions_df.columns)

#Transaction model
class TransactionIn(BaseModel):
    transaction_id: int
    merchant_id: int
    user_id: int
    card_number: str
    transaction_date: str
    transaction_amount: float
    device_id: Optional[int] = 0

# Function classify LOW/MED/HIGH based percentis
def classify_transaction(amount):
    if transactions_df.empty:
        return "MED"
    q1 = transactions_df['transaction_amount'].quantile(0.25)
    q3 = transactions_df['transaction_amount'].quantile(0.75)
    if amount < q1:
        return "LOW"
    elif amount <= q3:
        return "MED"
    else:
        return "HIGH"

# Function detect rapid transactions
def is_rapid_transaction(user_id, device_id, tx_dt, minutes=5):
    if transactions_df.empty:
        return False, False
    user_tx = transactions_df[transactions_df['user_id'] == user_id]
    rapid_user = ((tx_dt - user_tx['transaction_date']).dt.total_seconds() / 60 <= minutes).any() if not user_tx.empty else False
    device_tx = transactions_df[transactions_df['device_id'] == device_id]
    rapid_device = ((tx_dt - device_tx['transaction_date']).dt.total_seconds() / 60 <= minutes).any() if not device_tx.empty else False
    return rapid_user, rapid_device

# Function verify daily limit
def exceeds_daily_limit(user_id, tx_dt, amount, daily_limit=3000.0):
    if transactions_df.empty:
        return False
    user_tx = transactions_df[transactions_df['user_id'] == user_id]
    same_day_tx = user_tx[user_tx['transaction_date'].dt.date == tx_dt.date()]
    total_amount = same_day_tx['transaction_amount'].sum() + amount
    return total_amount > daily_limit


@app.get("/")
def read_root():
    return {"message": "Welcome to the AntiFraud API"}

@app.post("/recommend")
def recommend(tx: TransactionIn):
    global log_df, denied_log_df
    try:
        tx_dt = datetime.fromisoformat(tx.transaction_date)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use ISO format YYYY-MM-DDTHH:MM:SS")

    tx_class = classify_transaction(tx.transaction_amount)
    rapid_user, rapid_device = is_rapid_transaction(tx.user_id, tx.device_id, tx_dt)

    user_cbk = transactions_df[(transactions_df['user_id'] == tx.user_id) & (transactions_df['has_cbk'] == True)]

    # antifraude rules
    if not user_cbk.empty:
        recommendation = "deny"
        reason = "previous_chargeback"
    elif tx_class == "HIGH" and (rapid_user or rapid_device):
        recommendation = "deny"
        reason = "high_value_rapid_tx"
    elif exceeds_daily_limit(tx.user_id, tx_dt, tx.transaction_amount):
        recommendation = "deny"
        reason = "daily_limit_exceeded"
    else:
        recommendation = "approve"
        reason = "looks_ok"

    #logs
    new_row = {
        "transaction_id": tx.transaction_id,
        "merchant_id": tx.merchant_id,
        "user_id": tx.user_id,
        "card_number": tx.card_number,
        "transaction_date": tx.transaction_date,
        "transaction_amount": tx.transaction_amount,
        "device_id": tx.device_id,
        "has_cbk": False,
        "transaction_class": tx_class,
        "rapid_user": int(rapid_user),
        "rapid_device": int(rapid_device),
        "recommendation": recommendation,
        "reason": reason
    }

    log_df = pd.concat([log_df, pd.DataFrame([new_row])], ignore_index=True)
    log_df.to_csv("logs.csv", index=False)
    if recommendation == "deny":
        denied_log_df = pd.concat([denied_log_df, pd.DataFrame([new_row])], ignore_index=True)
        denied_log_df.to_csv("denied_logs.csv", index=False)

    return {
        "transaction_id": tx.transaction_id,
        "recommendation": recommendation,
        "reason": reason
    }
