# 💵 Cash Transaction App

A Streamlit app for Ops Executives to record cash collection transactions for each delivery order.

## Features

- **Login** — Secure email & password authentication
- **Record Transaction** — Select Date → Facility → Mode → Driver → Customer and record payment
- **Multi-transaction support** — A single order can have multiple transactions (e.g., ₹500 + ₹500)
- **Auto-calculated fields** — Outstanding Amount and Payment Status auto-fill based on amount entered
- **Outstanding Remark** — Shown only when there is a pending outstanding amount (Credit / Others)
- **Collection Window** — Time of Delivery / Before 5 / After 5 / Next Day
- **Driver-wise Summary** — View Records shows total orders, invoice value, collected and pending per driver

## How It Works

1. Ops Executive logs in
2. Selects **Delivery Date → Facility → Mode (Delivery/Pickup) → Driver**
3. Summary metrics show: Total Customers, Pending, Fully Paid, Cash Expected, Total Pending
4. Selects a customer — order details (Invoice, UPI, Cash, Paid So Far, Pending, Txn #) shown in one row
5. Enters **Amount Paid** and selects **Payment Mode**
6. **Outstanding Amount** and **Payment Status** auto-calculate live
7. If outstanding > 0, **Outstanding Remark** appears (Credit or Others with free text)
8. Selects **Collection Window** and saves

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Secrets

Create a `.env` file in the project root:

```env
DB_HOST=your_host
DB_PORT=your_port
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=your_database
```

### 3. Run Locally

```bash
streamlit run cash_transaction.py
```

### 4. Deploy on Streamlit Cloud

- Connect this repo on [share.streamlit.io](https://share.streamlit.io)
- Set **Main file path** to `cash_transaction.py`
- Add secrets under **Settings → Secrets**:

```toml
DB_HOST = "your_host"
DB_PORT = "your_port"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_NAME = "your_database"
```

## Database Tables

| Table | Purpose |
|---|---|
| `FnV_CashCollection_Base` | Source data — orders with Invoice, UPI, Cash amounts per driver |
| `FnV_CashCollection_TransactionBase` | Records each payment transaction made against an order |
