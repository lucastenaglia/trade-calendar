"""
convert_report.py
Convierte ReportHistory-*.xlsx de MT5 a JSON liviano para Trade Calendar.

Uso:
    python convert_report.py ReportHistory-100001497.xlsx

Genera: ReportHistory-100001497.json  (~1-2 MB en lugar de 34 MB)
"""
import sys, json, re
from pathlib import Path
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def parse_report(xlsx_path: Path) -> dict:
    print(f"Leyendo {xlsx_path.name} ({xlsx_path.stat().st_size / 1e6:.1f} MB)...")

    # Leer primeras filas para metadata
    meta_df = pd.read_excel(xlsx_path, header=None, nrows=8)
    account_name = account_num = company = ""
    for _, row in meta_df.iterrows():
        vals = [str(v).strip() if pd.notna(v) else '' for v in row]
        if vals[0] in ('Name:', 'Nombre:'):
            account_name = next((v for v in vals[1:] if v), '')
        elif vals[0] in ('Account:', 'Cuenta de trading:'):
            account_num = next((v for v in vals[1:] if v), '')
        elif vals[0] in ('Company:', 'Empresa:'):
            company = next((v for v in vals[1:] if v), '')

    print(f"  Cuenta: {account_name} ({account_num})")

    # Leer todo el archivo para encontrar secciones
    print("  Buscando secciones (puede tomar 1-2 minutos)...")
    df = pd.read_excel(xlsx_path, header=None)
    print(f"  Total filas: {len(df):,}")

    # Buscar header de Deals: Time | Deal | Symbol | Type | Direction | ...
    deals_header_row = -1
    positions_header_row = -1

    for i, row in df.iterrows():
        vals = [str(v).strip() if pd.notna(v) else '' for v in row]
        if vals[0] in ('Time', 'Fecha/Hora') and vals[1] == 'Deal' and vals[4] in ('Direction', 'Dirección'):
            deals_header_row = i
            break
        if vals[0] in ('Time', 'Fecha/Hora') and vals[1] in ('Position', 'Posición') and positions_header_row == -1:
            positions_header_row = i

    trades = []
    initial_balance = 0.0

    if deals_header_row >= 0:
        print(f"  Sección Deals encontrada en fila {deals_header_row:,}")
        data = df.iloc[deals_header_row + 1:].copy()
        data.columns = range(data.shape[1])
        # Cols: 0=Time, 1=Deal, 2=Symbol, 3=Type, 4=Direction, 5=Volume,
        #       6=Price, 7=Order, 8=Commission, 9=Fee, 10=Swap, 11=Profit, 12=Balance

        for _, row in data.iterrows():
            sym  = str(row.get(2, '') or '').strip().lower()
            typ  = str(row.get(3, '') or '').strip().lower()
            dire = str(row.get(4, '') or '').strip().lower()

            # Balance inicial (deposit row): col 11 = Profit holds the balance amount
            if not initial_balance and (sym == 'balance' or typ in ('balance', 'deposit', 'credit')):
                try:
                    bal = float(row.get(11, 0) or 0)
                    if bal > 0:
                        initial_balance = bal
                except:
                    pass

            if dire != 'out':
                continue
            if typ not in ('buy', 'sell'):
                continue

            date_str = str(row.get(0, '') or '').strip()
            m = re.match(r'^(\d{4})\.(\d{2})\.(\d{2})', date_str)
            if not m:
                continue

            try:
                vol        = float(row.get(5,  0) or 0)
                profit     = float(row.get(11, 0) or 0)
                commission = float(row.get(8,  0) or 0)
                swap       = float(row.get(10, 0) or 0)
            except:
                continue

            trades.append({
                'date': f"{m[1]}-{m[2]}-{m[3]}",
                'type': typ,
                'vol': round(vol, 4),
                'profit': round(profit, 2),
                'commission': round(commission, 2),
                'swap': round(swap, 2),
                'net': round(profit + commission + swap, 2),
            })

    elif positions_header_row >= 0:
        print(f"  Sección Positions encontrada en fila {positions_header_row:,}")
        data = df.iloc[positions_header_row + 1:].copy()
        data.columns = range(data.shape[1])
        # Cols: 0=Time, 1=Position, 2=Symbol, 3=Type, 4=Volume, 5=Price,
        #       6=S/L, 7=T/P, ..., 10=Commission, 11=Swap, 12=Profit

        for _, row in data.iterrows():
            typ = str(row.get(3, '') or '').strip().lower()

            if not initial_balance and typ in ('balance', 'deposit', 'credit'):
                try:
                    bal = float(row.get(12, 0) or 0)
                    if bal > 0:
                        initial_balance = bal
                except:
                    pass

            if typ not in ('buy', 'sell'):
                continue

            date_str = str(row.get(0, '') or '').strip()
            m = re.match(r'^(\d{4})\.(\d{2})\.(\d{2})', date_str)
            if not m:
                continue

            try:
                vol        = float(row.get(4,  0) or 0)
                profit     = float(row.get(12, 0) or 0)
                commission = float(row.get(10, 0) or 0)
                swap       = float(row.get(11, 0) or 0)
            except:
                continue

            trades.append({
                'date': f"{m[1]}-{m[2]}-{m[3]}",
                'type': typ,
                'vol': round(vol, 4),
                'profit': round(profit, 2),
                'commission': round(commission, 2),
                'swap': round(swap, 2),
                'net': round(profit + commission + swap, 2),
            })
    else:
        print("ERROR: No se encontró ninguna sección de trades (Deals/Positions).")
        sys.exit(1)

    print(f"  Trades cerrados encontrados: {len(trades):,}")

    return {
        'accountName': account_name,
        'accountNum': account_num,
        'company': company,
        'initialBalance': initial_balance,
        'trades': trades,
    }


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python convert_report.py ReportHistory-XXXXXXXX.xlsx")
        sys.exit(1)

    xlsx_path = Path(sys.argv[1])
    if not xlsx_path.exists():
        print(f"Archivo no encontrado: {xlsx_path}")
        sys.exit(1)

    result = parse_report(xlsx_path)
    out_path = xlsx_path.with_suffix('.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    size_mb = out_path.stat().st_size / 1e6
    print(f"\n✓ Generado: {out_path.name} ({size_mb:.2f} MB)")
    print(f"  Abrí ese .json en el Trade Calendar.")
