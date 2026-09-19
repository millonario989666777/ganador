#!/usr/bin/env python3
"""parche_fs2_1_1_foto_vieja - sube fs2_libro de 1.0 a 1.1: una foto vieja no retrocede el libro.

QUE ARREGLA

  La foto REST se pide por un canal distinto al del stream. Su contenido es de un instante
  T0, pero se graba en T0 + lo que tarde. En ese hueco el stream avanza.

  MEDIDO el 19-sep sobre binance_2026-09-13 (6.134 fotos, 60 ficheros):

      desfase entre el primer delta posterior y el lastUpdateId de la foto
        mediana +1.762   p10 +405   p90 +8.122   min +4   max +293.042

  SIEMPRE positivo: cuando la foto se graba, el libro que ya teniamos iba por delante.

  Hasta la 1.0, `aplicar_foto` vaciaba el libro y se ponia a esperar el delta que cubriera
  lastUpdateId. Ese delta ya habia pasado, asi que el siguiente llegaba descolgado y el
  simbolo se quedaba ciego hasta la foto siguiente, unos 5 minutos despues:

      fotos que no enganchaban ......... 2.416 de 56.687        = 4,3 %
      filas GAP_AFTER_SNAPSHOT ......... 709.534 de 17.279.875  = 4,1 %
      709.534 / 2.416 = 294 s = 4,9 min por fallo, que es justo la cadencia de foto

  Y de esos 2.416, el 96,9 % no hacian falta: el libro que se tiraba estaba mas al dia
  que la foto (246 de 254 en la ventana medida; solo 8 eran perdida real de datos).

LA REGLA NUEVA, en una linea: si el libro vale y la foto no adelanta nada, se ignora y se
cuenta. No es descartar informacion, es no retroceder. Si la foto SI adelanta (u > last_u)
o el libro esta roto, se aplica igual que antes.

Esto solo se puede hacer EN DIFERIDO. Un sistema en vivo, cuando le llega la foto vieja,
ya ha aplicado y tirado los deltas que la cubrian. Nosotros tenemos el fichero entero.

SOLO TOCA CODIGO DE LECTURA. fs2_libro no modifica el crudo ni el normalizado: lee y
escribe su propio derivado. orders = 0, execution_authority = NONE.

Uso:
  python3 parche_fs2_1_1_foto_vieja.py --autotest
  python3 parche_fs2_1_1_foto_vieja.py --dry-run
  python3 parche_fs2_1_1_foto_vieja.py --aplicar
"""
import argparse
import ast
import hashlib
import io
import os
import sys
import time

FS2 = "/home/jean/JEAN_FEATURE_STORE/bin/fs2_libro.py"
VERSION = "parche_fs2_1_1_foto_vieja/1.0.0"

VIEJO_FOTO = '''    def aplicar_foto(self, niveles, ts, u):
        self.bids, self.asks = {}, {}
'''

NUEVO_FOTO = '''    def aplicar_foto(self, niveles, ts, u):
        # La foto REST llega VIEJA. Se pide por otro canal, y cuando se graba el stream
        # ya ha avanzado. MEDIDO el 19-sep sobre binance_2026-09-13 (6.134 fotos): el
        # desfase entre el primer delta posterior y el lastUpdateId de la foto es de
        # +1.762 actualizaciones de mediana, p90 +8.122, y SIEMPRE positivo.
        #
        # Hasta la 1.0 se aplicaba igual: se vaciaba un libro sano, se quedaba esperando
        # un delta que cubriera lastUpdateId, ese delta YA HABIA PASADO, y el simbolo se
        # quedaba ciego hasta la foto siguiente, unos 5 minutos despues. Eso era el 4,1 %
        # del tiempo total (709.534 filas de 17.279.875), y el 96,9 % de esos huecos no
        # hacian falta: el libro que se tiraba estaba mas al dia que la foto.
        #
        # Regla: si el libro vale y la foto no adelanta nada, se ignora y se cuenta. No
        # es descartar informacion: es no retroceder. Si la foto SI adelanta (u > last_u)
        # o el libro esta roto, se aplica como siempre.
        if (self.valid and u is not None and self.last_u is not None
                and u <= self.last_u):
            self.last_event_ts = ts     # el feed esta vivo, aunque no rebasemos el libro
            return "foto_vieja_ignorada"
        self.bids, self.asks = {}, {}
'''

VIEJO_FIN = '''        self.last_snap_ts = ts
        self.resyncs += 1
'''
NUEVO_FIN = '''        self.last_snap_ts = ts
        self.resyncs += 1
        return "fotos"
'''

VIEJO_CALL = '''                lib.aplicar_foto(niveles, ts, col["book_update_id"][i])
                cnt["fotos"] += 1
'''
NUEVO_CALL = '''                cnt[lib.aplicar_foto(niveles, ts, col["book_update_id"][i])] += 1
'''

VIEJO_VER = 'CODE_VERSION = "fs2_libro/1.0"'
NUEVO_VER = 'CODE_VERSION = "fs2_libro/1.1"  # 1.1: la foto vieja no retrocede el libro'

CAMBIOS = ((VIEJO_FOTO, NUEVO_FOTO, "guardia de foto vieja"),
           (VIEJO_FIN, NUEVO_FIN, "aplicar_foto devuelve su resultado"),
           (VIEJO_CALL, NUEVO_CALL, "el que llama cuenta las dos salidas"),
           (VIEJO_VER, NUEVO_VER, "version"))


def sha(t):
    return hashlib.sha256(t.encode("utf-8")).hexdigest()


def transformar(texto):
    """Aplica los cuatro cambios o levanta ValueError diciendo cual no encaja."""
    if "foto_vieja_ignorada" in texto:
        raise ValueError("YA_APLICADO: fs2_libro ya ignora la foto vieja")
    for viejo, nuevo, nombre in CAMBIOS:
        n = texto.count(viejo)
        if n != 1:
            raise ValueError("NO_ENCAJA: '%s' aparece %d veces" % (nombre, n))
        texto = texto.replace(viejo, nuevo)
    ast.parse(texto)
    return texto


# ---------------------------------------------------------------------------
FIXTURE = '''import x
CODE_VERSION = "fs2_libro/1.0"


class Libro(object):
    def __init__(self):
        self.bids, self.asks = {}, {}
        self.last_u = None
        self.valid = False
        self.esperando_sync = False
        self.motivo = None
        self.last_event_ts = None
        self.last_snap_ts = None
        self.resyncs = 0

    def aplicar_foto(self, niveles, ts, u):
        self.bids, self.asks = {}, {}
        for side, price, qty in niveles:
            if price is None or price <= 0 or qty is None:
                continue
            if qty > 0:
                (self.bids if side == "BID" else self.asks)[price] = qty
        self.last_u = u
        self.esperando_sync = True
        self.valid = True
        self.motivo = None
        self.last_event_ts = ts
        self.last_snap_ts = ts
        self.resyncs += 1


def bucle(col, i, lib, niveles, ts, cnt):
            if col["event_type"][i] == "book_snapshot":
                lib.aplicar_foto(niveles, ts, col["book_update_id"][i])
                cnt["fotos"] += 1
            else:
                pass
'''


def autotest():
    fallos = []

    def check(n, c, d=""):
        print("  %-58s %s %s" % (n, "ok" if c else "FALLA", d if not c else ""))
        if not c:
            fallos.append(n)

    nuevo = transformar(FIXTURE)
    check("la version sube a 1.1", 'fs2_libro/1.1' in nuevo)
    check("no queda rastro de la 1.0", 'fs2_libro/1.0"' not in nuevo)
    check("el que llama ya no da por hecha la salida", 'cnt["fotos"] += 1' not in nuevo)

    ns = {}
    exec(compile(nuevo.replace("import x\n", ""), "<fixture>", "exec"), ns)
    Libro = ns["Libro"]

    # el caso medido: libro sano por delante, llega una foto vieja
    L = Libro()
    L.aplicar_foto([("BID", 100.0, 5.0)], 1, 1000)
    L.esperando_sync = False            # ya enganchado
    L.last_u = 1011
    antes = dict(L.bids)
    r = L.aplicar_foto([("BID", 100.0, 1.0)], 2, 1005)
    check("la foto vieja se ignora", r == "foto_vieja_ignorada", r)
    check("el libro no se vacia", L.bids == antes, (L.bids, antes))
    check("last_u no retrocede", L.last_u == 1011, L.last_u)
    check("no cuenta como resync", L.resyncs == 1, L.resyncs)

    # una foto que adelanta si se aplica
    L2 = Libro()
    L2.aplicar_foto([("BID", 1.0, 1.0)], 1, 100)
    L2.esperando_sync = False
    r = L2.aplicar_foto([("BID", 2.0, 2.0)], 2, 500)
    check("una foto mas nueva se aplica", r == "fotos", r)
    check("el libro pasa a ser el de la foto nueva", L2.bids == {2.0: 2.0}, L2.bids)

    # con el libro roto se aplica aunque sea vieja
    L3 = Libro()
    L3.aplicar_foto([("BID", 1.0, 1.0)], 1, 100)
    L3.valid = False
    r = L3.aplicar_foto([("BID", 3.0, 3.0)], 2, 50)
    check("con el libro roto la foto vieja SI se aplica", r == "fotos", r)

    # la primera foto de todas siempre entra
    L4 = Libro()
    r = L4.aplicar_foto([("BID", 1.0, 1.0)], 1, 10)
    check("la primera foto siempre se aplica", r == "fotos", r)

    try:
        transformar(nuevo)
        check("aplicarlo dos veces se niega", False, "no levanto ValueError")
    except ValueError as e:
        check("aplicarlo dos veces se niega", "YA_APLICADO" in str(e), str(e))
    try:
        transformar("nada que ver")
        check("con texto inesperado se niega", False, "no levanto ValueError")
    except ValueError as e:
        check("con texto inesperado se niega", "NO_ENCAJA" in str(e), str(e))

    print("\n%s" % ("AUTOTEST_PASS" if not fallos else "AUTOTEST_FAIL: " + ", ".join(fallos)))
    return 0 if not fallos else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fs2", default=FS2)
    ap.add_argument("--autotest", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--aplicar", action="store_true")
    a = ap.parse_args()

    if a.autotest:
        return autotest()
    if not os.path.isfile(a.fs2):
        print("NO_ESTA: %s" % a.fs2)
        return 2

    texto = io.open(a.fs2, encoding="utf-8").read()
    print("ANTES   sha256=%s" % sha(texto))
    try:
        nuevo = transformar(texto)
    except ValueError as e:
        print("NO_APLICA: %s" % e)
        return 4
    print("DESPUES sha256=%s" % sha(nuevo))
    if not a.aplicar:
        print("\n(dry-run: no se ha escrito nada. Anade --aplicar)")
        return 0

    copia = "%s.bak_1.0_%s" % (a.fs2, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    io.open(copia, "w", encoding="utf-8").write(texto)
    tmp = a.fs2 + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        f.write(nuevo)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, a.fs2)
    print("escrito %s (copia: %s)" % (a.fs2, os.path.basename(copia)))
    print("orders=0 execution_authority=NONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
