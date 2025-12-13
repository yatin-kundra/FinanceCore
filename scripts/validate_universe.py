import yfinance as yf
from fincore.data.universe import NIFTY_500

valid = []
invalid = []

for ticker in NIFTY_500:
    try:
        df = yf.download(
            ticker,
            period="5d",
            progress=False,
        )
        if df.empty:
            invalid.append(ticker)
        else:
            valid.append(ticker)
    except Exception:
        invalid.append(ticker)

print(f"VALID ({len(valid)}):")
print(valid)

print("\nINVALID:")
print(invalid)
