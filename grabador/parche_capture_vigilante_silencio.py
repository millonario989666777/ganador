#!/usr/bin/env python3
"""
Parche del GRABADOR (PC2): vigilante de silencio de datos.

PROBLEMA MEDIDO
---------------
En jean200/capture.py, dentro del bucle de cada socket:

    if now - last_response > 60:
        raise StreamError("Conexion sin respuesta durante 60 segundos")
    ...
    message = await asyncio.wait_for(ws.receive(), timeout=5)
    if message.type in (CLOSED, CLOSE, ERROR): raise ...
    last_response = time.monotonic()          # <-- se actualiza ANTES de filtrar el pong
    if message.type != TEXT: continue
    if message.data == "pong": continue

`last_response` se refresca con el PONG. Un socket que contesta al ping pero no
trae ni un dato parece vivo PARA SIEMPRE: el temporizador de 60 s nunca salta.

Eso explica lo medido contra el archivo oficial de los exchanges:
  binance 2026-09-13  corte de 142,4 s con el libro al 0,1 %  y SIN reconexion
  bybit   2026-09-14  corte de  66,0 s con el libro al 1,2 %  y SIN reconexion
Y el temporizador de 60 s explica los dos cortes de 61,1 s y 62,2 s, esos si
con reconexion.

ARREGLO
-------
Se separa "el socket responde" de "el socket trae datos":
  - last_response  sigue como esta (cualquier trama, incluido el pong). Es el respaldo.
  - last_data      NUEVO: solo se actualiza cuando llega un mensaje de mercado ruteado.
Si last_data se pasa del umbral, se fuerza reconexion.

UMBRALES: medidos sobre nuestra propia grabacion, no elegidos a ojo.
Silencio maximo real observado en sockets SANOS (PC3, 30-40 ficheros repartidos
por todo el dia, huecos medidos solo DENTRO de cada fichero):

    clase de socket                p99.9   p99.99     MAX
    binance LIBRO   (depth@100ms)  0,25 s   0,55 s   5,11 s
    binance OPERAC. (aggTrade...)  0,99 s   1,08 s   5,40 s
    bybit   (todo junto)           0,22 s   0,48 s   1,86 s
    okx     LIBRO   (books)        0,18 s   0,27 s   0,90 s
    okx     OPERAC. (trades-all)   6,49 s  13,25 s  39,74 s

Evidencia: PC3 /home/jean/JEAN_FIVEDAY_CLAUDE/silencio_socket.log
           PC3 /home/jean/JEAN_FIVEDAY_CLAUDE/silencio.log

Las liquidaciones de okx callan minutos con toda normalidad: ese socket se queda
SIN vigilante a proposito.

No opera, no usa credenciales, no envia ordenes.
"""
import argparse
import ast
import hashlib
import shutil
import sys
import time
from pathlib import Path

DESTINO = "/home/jean/jean-grabador-200/jean200/capture.py"
DESTINO_INSTALADO = "/home/jean/jean-grabador-200/.venv/lib/python3.12/site-packages/jean200/capture.py"

ANCLA_CONSTANTE = 'WS_HANDSHAKE_TIMEOUT_SECONDS = 45.0\n'
ANCLA_CLASE = 'class Capture:\n'

BLOQUE_NUEVO = '''
# Silencio de DATOS tolerado por socket, en segundos.
# El temporizador viejo miraba last_response, que el PONG refresca: un socket que
# contesta al ping pero no trae datos parecia vivo para siempre. De ahi los cortes
# de 142 s (binance) y 66 s (bybit) SIN reconexion, medidos contra el archivo
# oficial de los exchanges.
# Umbrales medidos sobre nuestra propia grabacion (silencio MAXIMO real en sockets
# sanos): binance libro 5,11 s | binance operaciones 5,40 s | bybit 1,86 s |
# okx libro 0,90 s | okx operaciones 39,74 s.
# Evidencia: PC3 JEAN_FIVEDAY_CLAUDE/silencio_socket.log
SILENCIO_DATOS_SEGUNDOS = {
    ("binance", "public"): 15.0,
    ("binance", "market"): 15.0,
    ("bybit", ""): 10.0,
    ("okx", "public"): 10.0,
    ("okx", "trades"): 180.0,
}


def silencio_datos_permitido(spec):
    """Segundos de silencio de DATOS tras los que se fuerza reconexion.

    Devuelve None cuando ese socket no debe vigilarse: las liquidaciones de okx
    callan minutos enteros con toda normalidad y un vigilante ahi solo provocaria
    reconexiones falsas, que es justo lo contrario de lo que se busca.
    """
    if spec.name == "okx-liquidations":
        return None
    sufijo = spec.name.rpartition("-")[2] if "-" in spec.name else ""
    if sufijo.isdigit():
        sufijo = ""
    return SILENCIO_DATOS_SEGUNDOS.get((spec.exchange, sufijo))
'''

BLOQUE_CONSTANTE = ANCLA_CONSTANTE + BLOQUE_NUEVO

ANCLA_INIT = '                    last_ping = last_response = time.monotonic()\n'
BLOQUE_INIT = ('                    last_ping = last_response = last_data = time.monotonic()\n'
               '                    silencio_max = silencio_datos_permitido(spec)\n')

ANCLA_CHECK = ('                        if now - last_response > 60:\n'
               '                            raise StreamError("Conexión sin respuesta durante 60 segundos")\n')
BLOQUE_CHECK = (ANCLA_CHECK +
                '                        if silencio_max is not None and now - last_data > silencio_max:\n'
                '                            # El socket contesta al ping pero no trae datos: esta muerto por dentro.\n'
                '                            raise StreamError(\n'
                '                                f"Socket sin datos durante {silencio_max:.0f} segundos")\n')

ANCLA_DATA = '                        for symbol, kind, exchange_ms in routed:\n'
BLOQUE_DATA = ('                        last_data = time.monotonic()\n' + ANCLA_DATA)

CAMBIOS = ((ANCLA_INIT, BLOQUE_INIT),
           (ANCLA_CHECK, BLOQUE_CHECK),
           (ANCLA_DATA, BLOQUE_DATA))

MARCA = "SILENCIO_DATOS_SEGUNDOS"


def sha(texto):
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def aplicar(texto):
    if MARCA in texto:
        return texto, False
    # El paquete instalado en el .venv es de 2026-09-07 y el fuente de 2026-09-14:
    # el instalado NO tiene WS_HANDSHAKE_TIMEOUT_SECONDS. Cuando falta esa ancla el
    # bloque se inserta antes de "class Capture:", para no arrastrar de rondon el
    # otro cambio que el fuente lleva pendiente. Un solo cambio en lo que corre.
    if ANCLA_CONSTANTE in texto:
        texto = texto.replace(ANCLA_CONSTANTE, BLOQUE_CONSTANTE, 1)
    elif texto.count(ANCLA_CLASE) == 1:
        texto = texto.replace(ANCLA_CLASE, BLOQUE_NUEVO.lstrip("\n") + "\n\n" + ANCLA_CLASE, 1)
    else:
        raise SystemExit("No se encontro donde insertar el bloque de constantes")
    for ancla, bloque in CAMBIOS:
        if texto.count(ancla) != 1:
            raise SystemExit("ANCLA no encontrada exactamente una vez: %r (aparece %d veces)"
                             % (ancla[:60], texto.count(ancla)))
        texto = texto.replace(ancla, bloque, 1)
    return texto, True


# ---------------------------------------------------------------- pruebas

class _Spec:
    def __init__(self, exchange, name):
        self.exchange, self.name = exchange, name


def _simular(umbral, eventos):
    """Reproduce la logica de los dos temporizadores del bucle.

    eventos: lista de (t, clase) con clase en {"pong", "dato"}.
    Devuelve el instante en que saltaria la reconexion, o None.
    ANTES del parche solo existia last_response, que el pong refresca.
    """
    last_response = last_data = 0.0
    for t, clase in eventos:
        if t - last_response > 60:
            return ("viejo", t)
        if umbral is not None and t - last_data > umbral:
            return ("nuevo", t)
        last_response = t                 # el pong tambien cuenta, como en el codigo
        if clase == "dato":
            last_data = t
    return None


def autotest():
    fallos = []

    def ok(cond, msg):
        if not cond:
            fallos.append(msg)

    ns = {}
    exec(compile(BLOQUE_CONSTANTE, "<parche>", "exec"), ns)
    perm = ns["silencio_datos_permitido"]

    # 1..6 los nombres reales que fabrica exchanges.py:socket_specs
    ok(perm(_Spec("binance", "binance-000-public")) == 15.0, "1 binance libro deberia ser 15 s")
    ok(perm(_Spec("binance", "binance-012-market")) == 15.0, "2 binance operaciones deberia ser 15 s")
    ok(perm(_Spec("bybit", "bybit-007")) == 10.0, "3 bybit deberia ser 10 s")
    ok(perm(_Spec("okx", "okx-003-public")) == 10.0, "4 okx libro deberia ser 10 s")
    ok(perm(_Spec("okx", "okx-003-trades")) == 180.0, "5 okx operaciones deberia ser 180 s")
    ok(perm(_Spec("okx", "okx-liquidations")) is None, "6 las liquidaciones de okx NO se vigilan")

    # 7 cada umbral deja margen sobre el silencio maximo REAL medido
    maximos = {"binance-000-public": 5.11, "binance-000-market": 5.40,
               "bybit-000": 1.86, "okx-000-public": 0.90, "okx-000-trades": 39.74}
    for nombre, maxreal in maximos.items():
        ex = nombre.split("-")[0]
        u = perm(_Spec(ex, nombre))
        ok(u is not None and u > maxreal * 1.5,
           "7 umbral de %s (%s) no deja 1,5x sobre el maximo real %.2f s" % (nombre, u, maxreal))

    # 8 EL FALLO: socket que solo contesta pongs. Antes no saltaba nunca.
    solo_pongs = [(t, "pong") for t in range(1, 601, 5)]
    ok(_simular(None, solo_pongs) is None, "8 antes del parche un socket de solo pongs no saltaba")

    # 9 con el vigilante, ese mismo socket salta cerca del umbral
    r = _simular(15.0, solo_pongs)
    ok(r is not None and r[0] == "nuevo" and r[1] <= 25,
       "9 con el vigilante deberia saltar antes de 25 s, salio %r" % (r,))

    # 10 un socket sano con datos cada 5 s NO salta (falso positivo)
    sano = [(t, "dato") for t in range(1, 3601, 5)]
    ok(_simular(15.0, sano) is None, "10 un socket sano no debe reconectar")

    # 11 okx operaciones con un silencio real de 39,74 s no da falso positivo
    okx = [(0.0, "dato"), (39.74, "dato"), (79.0, "dato"), (120.0, "dato")]
    ok(_simular(180.0, okx) is None, "11 el silencio real de okx no debe disparar el vigilante")

    # 12 el respaldo de 60 s sigue vivo para los sockets sin vigilante
    mudo = [(t, "pong") for t in (10, 20, 30)] + [(200.0, "pong")]
    r = _simular(None, mudo)
    ok(r is not None and r[0] == "viejo", "12 el respaldo de 60 s debe seguir funcionando")

    # 13..15 sobre los DOS ficheros reales: el fuente y el que de verdad se ejecuta
    vistos = 0
    for etiqueta, ruta in (("fuente", DESTINO), ("instalado", DESTINO_INSTALADO)):
        origen = Path(ruta)
        if not origen.exists():
            continue
        vistos += 1
        texto = origen.read_text(encoding="utf-8")
        nuevo, cambiado = aplicar(texto)
        try:
            ast.parse(nuevo)
        except SyntaxError as exc:
            fallos.append("13 %s: el fichero parcheado no compila: %s" % (etiqueta, exc))
        otra, cambiado2 = aplicar(nuevo)
        ok(otra == nuevo and not cambiado2, "14 %s: aplicarlo dos veces deberia no cambiar nada" % etiqueta)
        ok(nuevo.count("last_data = time.monotonic()") == 2,
           "15 %s: last_data deberia asignarse exactamente 2 veces" % etiqueta)
        ok(nuevo.count("SILENCIO_DATOS_SEGUNDOS = {") == 1,
           "16 %s: el bloque de umbrales deberia aparecer una sola vez" % etiqueta)
        # 17 no se cuela ningun otro cambio. La UNICA linea que el parche sustituye
        # es la de los temporizadores; todo lo demas solo se anade.
        quitadas = [l for l in texto.splitlines() if l not in nuevo.splitlines()]
        ok(quitadas == [ANCLA_INIT.rstrip("\n")],
           "17 %s: el parche solo debe sustituir la linea de temporizadores, quito %r"
           % (etiqueta, quitadas))
    if not vistos:
        print("   (13-17 no se prueban aqui: capture.py solo existe en PC2)")

    for f in fallos:
        print("FALLO:", f)
    print("AUTOTEST_PASS" if not fallos else "AUTOTEST_FAIL (%d)" % len(fallos))
    return 0 if not fallos else 1


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--autotest", action="store_true")
    p.add_argument("--aplicar", action="store_true")
    p.add_argument("--destino", default=DESTINO)
    a = p.parse_args()
    if a.autotest or not a.aplicar:
        return autotest()
    origen = Path(a.destino)
    texto = origen.read_text(encoding="utf-8")
    print("sha antes : %s" % sha(texto)[:16])
    nuevo, cambiado = aplicar(texto)
    if not cambiado:
        print("ya estaba aplicado, no se toca nada")
        return 0
    ast.parse(nuevo)
    copia = "%s.bak_sin_vigilante_%s" % (a.destino, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    shutil.copy2(a.destino, copia)
    origen.write_text(nuevo, encoding="utf-8")
    print("copia de seguridad: %s" % copia)
    print("sha despues: %s" % sha(nuevo)[:16])
    print("APLICADO")
    return 0


if __name__ == "__main__":
    sys.exit(main())
