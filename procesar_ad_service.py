"""
ElectroAlem - Microservicio de procesamiento de archivos AD
Extrae métricas estandarizadas del XLSX para el flujo n8n.

Deploy: Flask simple en el mismo servidor de n8n
Uso: POST /procesar-ad con multipart/form-data (file + filename)
"""

from flask import Flask, request, jsonify
import pandas as pd
import re
import json
from datetime import datetime
from io import BytesIO

app = Flask(__name__)


def extraer_fecha_desde_nombre(filename: str) -> str:
    """Extrae fecha del nombre AD_DD_MM_YYYY.xlsx → YYYY-MM-DD"""
    match = re.search(r'AD_(\d{2})_(\d{2})_(\d{4})', filename)
    if match:
        d, m, y = match.groups()
        return f"{y}-{m}-{d}"
    return datetime.now().strftime("%Y-%m-%d")


def procesar_ad(file_bytes: bytes, filename: str) -> dict:
    fecha_ad = extraer_fecha_desde_nombre(filename)

    # ── Tabla Dinam: deuda por cliente y tramos ──────────────────────────────
    tdf = pd.read_excel(BytesIO(file_bytes), sheet_name='Tabla Dinam', header=None)
    tdf.columns = tdf.iloc[3]
    tdf = tdf.iloc[4:].reset_index(drop=True)

    for col in ['>61', '46-60', '31-45', '16-30', '1-15', 'Total general']:
        tdf[col] = pd.to_numeric(tdf[col], errors='coerce').fillna(0)

    tdf['Código'] = tdf['Código'].astype(str)

    # Excluir fila de totales (contiene "Total general" como código)
    tdf = tdf[~tdf['Código'].str.contains('Total', na=False)]

    morosos = tdf[tdf['Total general'] > 0]
    total_cuentas = len(tdf[tdf['Total general'] != 0])
    cuentas_morosas = morosos['Código'].nunique()

    mora_total = morosos['Total general'].sum()
    mora_mayor_61 = morosos['>61'].sum()
    mora_46_60 = morosos['46-60'].sum()
    mora_31_45 = morosos['31-45'].sum()
    mora_16_30 = morosos['16-30'].sum()
    mora_1_15 = morosos['1-15'].sum()

    # Mora real = 15 a 61 días
    mora_real_15_61 = mora_16_30 + mora_31_45 + mora_46_60

    # Ticket promedio
    ticket_promedio = mora_total / cuentas_morosas if cuentas_morosas > 0 else 0

    # Top 5 deudores
    top5 = morosos.nlargest(5, 'Total general')[['Código', 'Razón Social', 'Total general']]
    top5_list = top5.to_dict(orient='records')

    # ── Trabajando: cobranza semanal y evolución ──────────────────────────────
    wdf = pd.read_excel(BytesIO(file_bytes), sheet_name='Trabajando', header=None)

    try:
        cob_prev = float(wdf.iloc[7, 16]) if pd.notna(wdf.iloc[7, 16]) else 0
        cob_curr = float(wdf.iloc[7, 17]) if pd.notna(wdf.iloc[7, 17]) else 0
        evo_cobranza = float(wdf.iloc[7, 18]) if pd.notna(wdf.iloc[7, 18]) else 0
    except (IndexError, ValueError, TypeError):
        cob_prev, cob_curr, evo_cobranza = 0, 0, 0

    # Performance = cobranza / mora real (%)
    performance_pct = (cob_curr / mora_real_15_61 * 100) if mora_real_15_61 > 0 else 0

    # ── Cantidades por tramo ──────────────────────────────────────────────────
    cant_mayor_61 = int((morosos['>61'] > 0).sum())
    cant_46_60 = int((morosos['46-60'] > 0).sum())
    cant_31_45 = int((morosos['31-45'] > 0).sum())
    cant_16_30 = int((morosos['16-30'] > 0).sum())
    cant_1_15 = int((morosos['1-15'] > 0).sum())

    return {
        "fecha_ad": fecha_ad,
        "filename": filename,
        "total_cuentas": total_cuentas,
        "cuentas_morosas": cuentas_morosas,
        "mora_total": round(mora_total, 2),
        "mora_real_15_61": round(mora_real_15_61, 2),
        "mora_mayor_61": round(mora_mayor_61, 2),
        "mora_46_60": round(mora_46_60, 2),
        "mora_31_45": round(mora_31_45, 2),
        "mora_16_30": round(mora_16_30, 2),
        "mora_1_15": round(mora_1_15, 2),
        "cobranza_semanal": round(cob_curr, 2),
        "cobranza_semana_anterior": round(cob_prev, 2),
        "performance_cobranza_pct": round(performance_pct, 2),
        "ticket_promedio": round(ticket_promedio, 2),
        "evolucion_cobranza": round(evo_cobranza, 6),
        "cant_mayor_61": cant_mayor_61,
        "cant_46_60": cant_46_60,
        "cant_31_45": cant_31_45,
        "cant_16_30": cant_16_30,
        "cant_1_15": cant_1_15,
        "top5_deudores_json": json.dumps(top5_list, ensure_ascii=False),
        "codigos_morosos": list(morosos['Código'].unique()),
    }


def calcular_reincidentes(codigos_semana_actual: list, historial_rows: list) -> dict:
    """
    Calcula clientes que se repiten y los que pagaron.
    historial_rows: últimas 4 filas del Google Sheet historial
    """
    if len(historial_rows) < 3:
        return {"reincidentes_count": 0, "pagaron_count": 0}

    # Reconstruir sets de morosos por semana desde historial
    # Nota: en el sheet guardamos codigos_morosos como JSON string
    sets_historicos = []
    for row in historial_rows[-4:-1]:  # las 3 semanas anteriores
        codigos_str = row.get('codigos_morosos', '[]')
        try:
            sets_historicos.append(set(json.loads(codigos_str)))
        except Exception:
            sets_historicos.append(set())

    set_actual = set(str(c) for c in codigos_semana_actual)

    if sets_historicos:
        # Reincidentes: en el set actual Y en TODAS las semanas anteriores disponibles
        reincidentes = set_actual.copy()
        for s in sets_historicos:
            reincidentes &= s

        # Los que pagaron: estaban en la primera semana registrada y NO están ahora
        primer_set = sets_historicos[0] if sets_historicos else set()
        pagaron = primer_set - set_actual
    else:
        reincidentes, pagaron = set(), set()

    return {
        "reincidentes_count": len(reincidentes),
        "pagaron_count": len(pagaron),
    }


@app.route('/procesar-ad', methods=['POST'])
def endpoint_procesar():
    if 'file' not in request.files:
        return jsonify({"error": "No se recibió archivo"}), 400

    f = request.files['file']
    filename = request.form.get('filename', f.filename)
    file_bytes = f.read()

    try:
        resultado = procesar_ad(file_bytes, filename)

        # Calcular reincidentes si vienen datos del historial
        historial_json = request.form.get('historial', '[]')
        historial = json.loads(historial_json)
        reincidentes = calcular_reincidentes(resultado['codigos_morosos'], historial)
        resultado.update(reincidentes)

        return jsonify(resultado)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    # Test local con los archivos de ejemplo
    import sys
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'rb') as fh:
            data = procesar_ad(fh.read(), sys.argv[1].split('/')[-1])
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    else:
        app.run(host='0.0.0.0', port=5679, debug=True)
