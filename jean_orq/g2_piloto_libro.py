#!/usr/bin/env python3
"""g2_piloto_libro - mide el TIEMPO CON LIBRO VALIDO de una particion, por venue.

QUE MIDE Y QUE NO
  Mide la unica magnitud que dice si un corte sirve: que porcentaje del tiempo el libro
  de cada simbolo esta sincronizado y, por tanto, puede producir features. Contar ficheros
  no lo dice; "parquet == manifest" tampoco, porque eso compara la fuente consigo misma.

  NO reconstruye los niveles de precio ni sustituye a fs2_libro: no mantiene el libro,
  mantiene su ESTADO (sincronizado o no) segun la regla de continuidad del venue. Es lo
  que hace falta para decidir el alcance del corte antes de gastar dias de CPU en G2.

  Usa fs2_dialectos_libro, de modo que cada venue se juzga con SU regla:
    binance  pu == u_anterior        foto book_snapshot
    bybit    u  == u_anterior + 1    foto book_snapshot
    okx      pu == u_anterior        foto book_deep_snapshot
  Aplicar la de binance a los otros dos no da error: da 0 por ciento de tiempo valido y
  ni una queja. Ese es justamente el fallo que este piloto existe para no repetir.

SOLO LECTURA. No escribe en el derivado, no toca procesos, no conecta con exchanges.
orders = 0, execution_authority = NONE.

Uso:
  python g2_piloto_libro.py --dir <particion>/normalized --exchange bybit [--ficheros 12] [--desde 0.4]
"""
import argparse
import json
import os
import sys

import numpy as np
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fs2_dialectos_libro import MaquinaLibro, dialecto_de, VERSION as VERSION_DIALECTOS

VERSION = "g2_piloto_libro/1.0.0"
COLS = ["symbol", "event_type", "book_first_update_id", "book_update_id",
        "book_prev_update_id", "event_ts_utc", "receive_ts_utc", "sequence_id"]
NULO = -(1 << 62)


def col_np(t, nombre):
    return t.column(nombre).fill_null(NULO).to_numpy(zero_copy_only=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--exchange", required=True)
    ap.add_argument("--ficheros", type=int, default=12)
    ap.add_argument("--desde", type=float, default=0.4)
    a = ap.parse_args()

    dial = dialecto_de(a.exchange)
    todos = sorted(fn for fn in os.listdir(a.dir) if fn.endswith(".parquet"))
    if not todos:
        print(json.dumps({"error": "sin parquet en %s" % a.dir}))
        return 2
    ini = max(0, min(len(todos) - a.ficheros, int(len(todos) * a.desde)))
    muestra = todos[ini:ini + a.ficheros]

    maquinas = {}          # symbol -> MaquinaLibro
    t_ini = {}             # symbol -> primer ts visto
    t_fin = {}             # symbol -> ultimo ts visto
    t_valido = {}          # symbol -> ms acumulados en SYNCED
    t_ultimo = {}          # symbol -> ts del ultimo mensaje
    usaron_receive = 0
    usaron_evento = 0

    for fn in muestra:
        t = pq.read_table(os.path.join(a.dir, fn), columns=COLS)
        et = t.column("event_type").to_pylist()
        sym = t.column("symbol").to_pylist()
        U = col_np(t, "book_first_update_id")
        u = col_np(t, "book_update_id")
        pu = col_np(t, "book_prev_update_id")
        ev = col_np(t, "event_ts_utc")
        rx = col_np(t, "receive_ts_utc")
        sq = col_np(t, "sequence_id")

        es_libro = np.array([e == "book_delta" or dial.es_foto(e) for e in et], dtype=bool)
        idx = np.nonzero(es_libro)[0]
        if idx.size == 0:
            continue

        # un mensaje se expande en una fila por nivel: quedarse con la primera de cada uno
        anterior = None
        for i in idx:
            clave = (sym[i], u[i], U[i], pu[i], sq[i])
            if clave == anterior:
                continue
            anterior = clave

            s = sym[i]
            m = maquinas.get(s)
            if m is None:
                m = maquinas[s] = MaquinaLibro(a.exchange)

            # reloj: la hora del evento si la hay; si no, la de recepcion, y se declara
            ts = ev[i]
            if ts == NULO or ts <= 0:
                ts = rx[i]
                if ts != NULO and ts > 0:
                    ts = ts // 1000000 if ts > 10 ** 15 else ts
                    usaron_receive += 1
                else:
                    ts = None
            else:
                usaron_evento += 1

            if ts is not None:
                if s not in t_ini:
                    t_ini[s] = ts
                    t_valido[s] = 0
                else:
                    # el tramo que acaba de pasar contaba como valido si el libro lo estaba
                    if m.usable and t_ultimo.get(s) is not None and ts >= t_ultimo[s]:
                        t_valido[s] += ts - t_ultimo[s]
                t_fin[s] = ts
                t_ultimo[s] = ts

            if dial.es_foto(et[i]):
                m.foto(None if u[i] == NULO else int(u[i]))
            else:
                m.delta(None if U[i] == NULO else int(U[i]),
                        None if u[i] == NULO else int(u[i]),
                        None if pu[i] == NULO else int(pu[i]))

    pct = []
    for s, m in maquinas.items():
        dur = (t_fin.get(s, 0) - t_ini.get(s, 0))
        if dur > 0:
            pct.append(100.0 * t_valido.get(s, 0) / dur)
    pct.sort()

    def q(p):
        return round(pct[int(len(pct) * p)], 2) if pct else None

    aplicados = sum(m.aplicados for m in maquinas.values())
    rechazados = sum(m.rechazados for m in maquinas.values())
    resyncs = sum(m.resyncs for m in maquinas.values())
    sin_foto = sum(1 for m in maquinas.values() if m.generacion == 0)
    total_msg = aplicados + rechazados + resyncs

    salida = {
        "version": VERSION,
        "dialectos": VERSION_DIALECTOS,
        "exchange": a.exchange,
        "regla_aplicada": dial.nombre_regla,
        "fotos_reconocidas": sorted(dial.fotos),
        "dir": a.dir,
        "ficheros_totales_en_particion": len(todos),
        "ficheros_medidos": len(muestra),
        "ventana": [muestra[0], muestra[-1]],
        "simbolos": len(maquinas),
        "simbolos_sin_ninguna_foto": sin_foto,
        "mensajes_de_libro": total_msg,
        "deltas_aplicados": aplicados,
        "deltas_rechazados": rechazados,
        "pct_deltas_aplicados": round(100.0 * aplicados / max(1, aplicados + rechazados), 4),
        "resyncs": resyncs,
        "horas_cubiertas": round((max(t_fin.values()) - min(t_ini.values())) / 3600000.0, 3) if t_fin else 0,
        "PCT_TIEMPO_LIBRO_VALIDO": {
            "mediana": q(0.5), "p25": q(0.25), "p75": q(0.75),
            "min": round(pct[0], 2) if pct else None,
            "max": round(pct[-1], 2) if pct else None,
            "simbolos_medidos": len(pct),
        },
        "reloj": {
            "mensajes_con_hora_de_evento": usaron_evento,
            "mensajes_que_cayeron_a_hora_de_recepcion": usaron_receive,
            "nota": "si cayeron muchos a recepcion, la particion se normalizo con una version "
                    "que no recuperaba la hora de ese venue y el porcentaje lleva ese sesgo",
        },
        "orders": 0,
        "execution_authority": "NONE",
    }
    print(json.dumps(salida, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
