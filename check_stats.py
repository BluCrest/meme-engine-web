import sqlite3
c=sqlite3.connect('C:/Users/EMMANUEL/meme-engine/data/trades.db')
r=c.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('Tables:', list(r))
r=c.execute("SELECT * FROM portfolio_stats")
print('Portfolio stats:', list(r))
r=c.execute("SELECT * FROM equity_curve")
print('Equity curve rows:', r.fetchall())
c.close()