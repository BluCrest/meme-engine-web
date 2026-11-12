"""Self-test for the demo wallet's honest math (x returns + percentages).
Run: python check_demo.py
Verifies fills against hand-computed values to 1e-9 tolerance.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from paper.demo_wallet import DemoWallet

TOL = 1e-9
fails = []

def check(name, got, want, tol=TOL):
    ok = abs(got - want) <= tol
    print(f"{'PASS' if ok else 'FAIL'} {name}: got {got:.10f} want {want:.10f}")
    if not ok:
        fails.append(name)

cfg = {'paper': {'initial_sol': 2.0, 'slippage_pct': 2.0, 'fee_pct': 0.30,
                 'fallback_sol_usd': 200.0, 'reset_on_boot': True}}
w = DemoWallet(cfg, state_path='data/_demo_selftest.json')

# --- Case 1: single round trip, buy $100 @ 0.001, sell all @ 0.002 (mid 2x) ---
b = w.buy('MINT1', 100.0, 0.001)
# buy math: fee=0.30, exec=0.001*1.02=0.00102, tokens=(100-0.30)/0.00102=97,843.1372549...
want_tokens = (100.0 * (1 - 0.003)) / (0.001 * 1.02)
check('buy tokens', b['tokens'], want_tokens)
check('buy exec_price', b['exec_price'], 0.00102)
check('cash reduced (SOL)', 2.0 - b['sol_in'], w.sol)  # sol_in = 100/200 = 0.5
check('sol_in usd conv', b['sol_in'], 0.5)

s = w.sell('MINT1', b['tokens'], 0.002, 'test')
# sell math: exec=0.002*0.98=0.00196, gross=97,843.137*0.00196=191.7725..., fee=0.5773..., net=...
want_gross = want_tokens * 0.002 * 0.98
want_net = want_gross * (1 - 0.003)
check('sell gross', s['gross_usd'], want_gross)
check('sell net', s['net_usd'], want_net)
check('holdings cleared', 0.0 if 'MINT1' not in w.holdings else w.holdings['MINT1']['amount'], 0.0, tol=1e-6)

# ALL-IN x: proceeds/cost. Mid-price x would be exactly 2.0; honest x < 2.0.
x1 = DemoWallet.x_return(100.0, s['net_usd'])
want_x1 = want_net / 100.0
check('x all-in roundtrip', x1, want_x1)
print(f"      mid-price x would be 2.0000 -> honest all-in x = {x1:.6f} (ROI {DemoWallet.roi_pct(100.0, s['net_usd']):.4f}%)")
if x1 >= 2.0:
    fails.append('x honesty: all-in x must be < mid-price x')

# --- Case 2: tiered exit (2x tier 25%, then full at 3x), partial bookkeeping ---
w2 = DemoWallet(cfg, state_path='data/_demo_selftest2.json')
b2 = w2.buy('MINT2', 50.0, 0.01)   # exec 0.0102, tokens=(50*0.997)/0.0102
t2 = b2['tokens']
s25 = w2.sell('MINT2', t2 * 0.25, 0.02, 'tier1')   # mid 2x
s75 = w2.sell('MINT2', t2 * 0.75, 0.03, 'tier2')   # mid 3x
proceeds = s25['net_usd'] + s75['net_usd']
x2 = DemoWallet.x_return(50.0, proceeds)
want_x2 = (s25['net_usd'] + s75['net_usd']) / 50.0
check('x tiered', x2, want_x2)
# hand-compute: tokens=4887.2549..., s25 exec=0.02*0.98, s75 exec=0.03*0.98, each net *0.997
tok = 50.0 * 0.997 / 0.0102
want_net25 = tok * 0.25 * 0.0196 * 0.997
want_net75 = tok * 0.75 * 0.0294 * 0.997
check('tier1 net hand', s25['net_usd'], want_net25)
check('tier2 net hand', s75['net_usd'], want_net75)
print(f"      tiered: mid-price blended x would be {0.25*2 + 0.75*3:.4f} -> honest all-in x = {x2:.6f}")

# --- Case 3: stop-out loss (buy, price halves) ---
w3 = DemoWallet(cfg, state_path='data/_demo_selftest3.json')
b3 = w3.buy('MINT3', 10.0, 0.001)
s3 = w3.sell('MINT3', b3['tokens'], 0.0005, 'stop')
x3 = DemoWallet.x_return(10.0, s3['net_usd'])
want_x3 = (b3['tokens'] * 0.0005 * 0.98 * 0.997) / 10.0
check('x stop-out', x3, want_x3)
print(f"      stop-out: mid-price x would be 0.5000 -> honest all-in x = {x3:.6f} (ROI {DemoWallet.roi_pct(10.0, s3['net_usd']):.2f}%)")

# --- Case 4: wallet refuses overspend ---
w4 = DemoWallet(cfg, state_path='data/_demo_selftest4.json')
r = w4.buy('MINT4', 1_000_000.0, 0.001)
check('overspend refused', 0.0 if r is None else 1.0, 0.0)

# --- Case 5: cash conservation (sol in - sol out sums to zero roundtrip) ---
w5 = DemoWallet(cfg, state_path='data/_demo_selftest5.json')
start = w5.sol
b5 = w5.buy('MINT5', 200.0, 0.002)
s5 = w5.sell('MINT5', b5['tokens'], 0.002, 'flat')
# roundtrip at same mid: lose fee*2 + slippage both legs; cash should be start - small loss
check('cash not increased', 1.0 if w5.sol < start else 0.0, 1.0)
loss = start - w5.sol
print(f"      flat roundtrip cost (slip+fees): ${loss*200:.4f} on $200 -> honest, never free")

print()
print('RESULT:', 'ALL PASS' if not fails else f'{len(fails)} FAILURES: {fails}')
sys.exit(0 if not fails else 1)
