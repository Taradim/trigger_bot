import requests

def main():
    print("Hello from trigger-bot!")
    
    symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'DOTUSDT', 'LINKUSDT', 'BCHUSDT', 'LTCUSDT', 'XLMUSDT', 'XMRUSDT']

    for symbol in symbols:
        url = f'https://api.binance.com/api/v3/ticker/price?symbol={symbol}'
        response = requests.get(url)
        data = response.json()
        print(symbol, data['price'])


if __name__ == "__main__":
    main()
