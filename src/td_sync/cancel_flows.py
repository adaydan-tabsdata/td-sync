from td_sync.sync_v2 import load_server


def main():
    server = load_server()
    transactions = server.list_transactions()
    stalled_transactions = [
        i for i in transactions if i.status in ["Stalled", "Running"]
    ]
    if len(stalled_transactions) > 0:
        canceled_transactions = [i.cancel() for i in stalled_transactions]
        print(f"Canceled {len(canceled_transactions)} stalled transactions")
        return canceled_transactions
    print("No stalled transactions found")
    return []
