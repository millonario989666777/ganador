#!/usr/bin/env python3
"""parche_fs1_2_2_latido - sube fs1_normalizar de 2.1 a 2.2. Tres cosas que se tiraban.

QUE ARREGLA

  Un `book_delta` de bybit puede llegar sin ningun nivel: `b` y `a` vacios, pero con
  `u` y `seq` nuevos. Hasta 2.1 esos mensajes iban a cuarentena con el codigo
  BOOK_DELTA_SIN_NIVELES y un comentario que decia "No se pierde nada".

  Medido el 18-sep sobre bybit_2026-09-13 (3 ficheros crudos, 320.657 book_delta, de
  los cuales 1.456 vacios), aplicando la regla de bybit `u == u_anterior + 1`:

      CON los latidos dentro   320.467 encadenan   0 rompen    100,0000 %
      SIN los latidos          317.836 encadenan   1.175 rompen 99,6317 %

  El latido GASTA un numero de la cadena. Tirarlo abre un agujero que no existia, y
  cada agujero deja el libro de ese simbolo invalido hasta la siguiente foto. Ese es
  el motivo real de que bybit midiera 69,25 % de tiempo con libro valido.

  A partir de 2.2 el latido se emite como una fila SIN niveles: lleva los
  identificadores y nada mas. fs2_libro ya salta los niveles con precio nulo
  (fs2_libro.py:433), asi que esa fila encadena el libro sin tocarlo.

  (2) okx tira su FOTO EN BANDA. El grabador etiqueta action=="snapshot" como kind
      `book_snapshot` (exchanges.py:216), y el lector de okx solo conoce `book_delta` y
      `book_deep_snapshot`: la foto cae en UNKNOWN_EVENT_TYPE y se descarta.
      MEDIDO en okx_2026-09-13: 606 fotos al dia a la basura. Son las MEJORES que hay,
      porque llegan por el mismo socket que los deltas y no hay que adivinar donde
      encajan. Traen bids, asks, seqId y prevSeqId = -1, que es como okx marca una foto.
      Por culpa de esto el gate dio FAIL 10/12 en esa particion.

  (3) okx tambien tiene latidos: 105 al dia salen como EMPTY_RESULT, con el motivo
      "el lector de okx atendio book_delta y no emitio nada".

ALCANCE MEDIDO: bybit y okx. binance no tiene ni un delta vacio (0 de 236.214) y su
stream no lleva foto en banda por protocolo, asi que su salida con 2.2 es identica a la
de 2.1 y sus particiones ya hechas siguen valiendo.

POR QUE SE NIEGA A CORRER EN CALIENTE

  g1_particion.py:167 calcula `sha_file(FS1)` al construir la MEDICION, o sea DESPUES
  de que el normalizador haya terminado. Si se cambia fs1_normalizar.py con trabajos
  en vuelo, esos trabajos sellan su `code_version` real (2.1) junto al sha del binario
  NUEVO (2.2). La evidencia quedaria mintiendo y ninguna comprobacion lo veria. Por eso
  este parche aborta si hay un fs1_normalizar.py vivo, salvo --forzar explicito.

SOLO TOCA CODIGO. No borra datos, no toca la captura, no conecta con exchanges.
orders = 0, execution_authority = NONE.

Uso:
  python3 parche_fs1_2_2_latido.py --autotest          # prueba la transformacion, no toca nada
  python3 parche_fs1_2_2_latido.py --dry-run           # dice que cambiaria
  python3 parche_fs1_2_2_latido.py --aplicar           # aplica, con copia de seguridad
"""
import argparse
import ast
import hashlib
import io
import os
import re
import subprocess
import sys
import time

BIN = "/home/jean/JEAN_FEATURE_STORE/bin"
VERSION = "parche_fs1_2_2_latido/1.0.0"

VIEJO_DIALECTOS = '''        if not b and not a:
            # Latido: el canal manda u y seq nuevos sin ningun nivel que haya
            # cambiado. No se pierde nada, pero se deja contado para que nadie
            # lo confunda con un mensaje que se cayo por el camino.
            return malo("BOOK_DELTA_SIN_NIVELES", "u=%s seq=%s" % (data.get("u"), data.get("seq")))
'''

NUEVO_DIALECTOS = '''        if not b and not a:
            # Latido: el canal manda u y seq nuevos sin ningun nivel que haya
            # cambiado. Hasta 2.1 esto iba a cuarentena con el comentario "no se
            # pierde nada". MEDIDO el 18-sep sobre bybit_2026-09-13: era falso.
            # El latido GASTA un numero de la cadena, y la cadena de bybit es
            # u == u_anterior + 1 exacta. Con los latidos dentro, 320.467 pares
            # encadenan y 0 rompen; sin ellos aparecen 1.175 agujeros en solo 3
            # ficheros, y cada agujero deja el libro invalido hasta la siguiente
            # foto. Asi que se emite: una fila SIN niveles, con los
            # identificadores y nada mas. fs2_libro salta los niveles de precio
            # nulo, de modo que esta fila encadena el libro sin tocarlo.
            if data.get("u") is None:
                # sin u no encadena nada: eso si es un mensaje inservible
                return malo("BOOK_DELTA_SIN_NIVELES_NI_U", "seq=%s" % (data.get("seq"),))
            emit({"side": None, "price": None, "qty": None, "level": None,
                  "action": accion, "book_first_update_id": None,
                  "book_update_id": data.get("u"), "book_prev_update_id": None},
                 ("BOOK_DELTA_SIN_NIVELES",))
            return True
'''

# --- okx: la foto en banda llega como kind "book_snapshot" y el lector no la conocia
VIEJO_OKX_RAMA = """    if kind == \"book_delta\":
        if not items:
            return malo(\"BAD_SHAPE\", \"book_delta de okx sin lista data\")
        accion = \"SNAPSHOT\" if d.get(\"action\") == \"snapshot\" else \"UPDATE\"
"""

NUEVO_OKX_RAMA = """    if kind in (\"book_delta\", \"book_snapshot\"):
        # El grabador manda la foto EN BANDA de okx como kind \"book_snapshot\"
        # (exchanges.py:216, action==\"snapshot\"). Hasta 2.1 este lector solo conocia
        # book_delta y book_deep_snapshot, asi que la foto caia en UNKNOWN_EVENT_TYPE:
        # 606 al dia medidas en okx_2026-09-13, y el gate en FAIL 10/12 por eso.
        # La forma del payload es la misma que la de un delta; lo unico que cambia es
        # que sustituye el libro en vez de sumarse, y que okx la marca con prevSeqId=-1.
        if not items:
            return malo(\"BAD_SHAPE\", \"%s de okx sin lista data\" % kind)
        accion = \"SNAPSHOT\" if (kind == \"book_snapshot\" or d.get(\"action\") == \"snapshot\") else \"UPDATE\"
"""

VIEJO_OKX_NIV = """            ids = {\"book_update_id\": it.get(\"seqId\"), \"book_prev_update_id\": it.get(\"prevSeqId\")}
            niveles(it.get(\"bids\"), \"BID\", accion, ids)
"""

NUEVO_OKX_NIV = """            ids = {\"book_update_id\": it.get(\"seqId\"), \"book_prev_update_id\": it.get(\"prevSeqId\")}
            if not it.get(\"bids\") and not it.get(\"asks\"):
                # Latido: seqId nuevo sin ningun nivel que haya cambiado. Mismo caso que
                # en bybit. Hasta 2.1 salia como EMPTY_RESULT (105 al dia medidos) y se
                # perdia el eslabon de la cadena. Se emite una fila sin niveles.
                if it.get(\"seqId\") is None:
                    malo(\"BOOK_DELTA_SIN_NIVELES_NI_SEQ\", \"ts=%s\" % it.get(\"ts\"))
                    continue
                emit({\"side\": None, \"price\": None, \"qty\": None, \"level\": None,
                      \"action\": accion, \"book_first_update_id\": None,
                      \"book_update_id\": it.get(\"seqId\"),
                      \"book_prev_update_id\": it.get(\"prevSeqId\")},
                     (\"BOOK_DELTA_SIN_NIVELES\",))
                continue
            niveles(it.get(\"bids\"), \"BID\", accion, ids)
"""

VIEJO_VERSION = 'CODE_VERSION = "fs1_normalizar/2.1"  # 2.1: hora de evento por venue (bybit/okx)'
NUEVO_VERSION = ('CODE_VERSION = "fs1_normalizar/2.2"  # 2.2: latidos de bybit y okx emitidos, '
                 'y la foto en banda de okx deja de tirarse')


def sha(texto):
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def leer(ruta):
    with io.open(ruta, encoding="utf-8") as f:
        return f.read()


def transformar(dialectos, normalizador):
    """Devuelve (dialectos_nuevo, normalizador_nuevo) o levanta ValueError con el motivo.

    Separada del disco a proposito: asi el autotest prueba la transformacion de verdad
    y no una imitacion.
    """
    if NUEVO_DIALECTOS.strip() in dialectos or 'kind in ("book_delta", "book_snapshot")' in dialectos:
        raise ValueError("YA_APLICADO: fs1_dialectos.py ya trae los cambios de 2.2")
    if dialectos.count(VIEJO_DIALECTOS) != 1:
        raise ValueError("NO_ENCAJA: el bloque del latido aparece %d veces en fs1_dialectos.py"
                         % dialectos.count(VIEJO_DIALECTOS))
    if normalizador.count(VIEJO_VERSION) != 1:
        raise ValueError("NO_ENCAJA: CODE_VERSION 2.1 aparece %d veces en fs1_normalizar.py"
                         % normalizador.count(VIEJO_VERSION))
    for viejo, nombre in ((VIEJO_OKX_RAMA, "rama de libro de okx"),
                          (VIEJO_OKX_NIV, "niveles de okx")):
        if dialectos.count(viejo) != 1:
            raise ValueError("NO_ENCAJA: %s aparece %d veces en fs1_dialectos.py"
                             % (nombre, dialectos.count(viejo)))
    d2 = dialectos.replace(VIEJO_DIALECTOS, NUEVO_DIALECTOS)
    d2 = d2.replace(VIEJO_OKX_RAMA, NUEVO_OKX_RAMA)
    d2 = d2.replace(VIEJO_OKX_NIV, NUEVO_OKX_NIV)
    n2 = normalizador.replace(VIEJO_VERSION, NUEVO_VERSION)
    for nombre, txt in (("fs1_dialectos.py", d2), ("fs1_normalizar.py", n2)):
        ast.parse(txt)   # si el resultado no compila, no sale de aqui
    return d2, n2


def normalizadores_vivos():
    """PIDs de fs1_normalizar.py vivos, leyendo argv. Nunca el texto de la orden:
    un comando que solo MENCIONA fs1_normalizar.py no es un normalizador, y confundir
    las dos cosas ya hizo que un guardia se detectara a si mismo."""
    vivos = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open("/proc/%s/cmdline" % pid, "rb") as f:
                av = f.read().split(b"\0")
        except OSError:
            continue
        av = [x.decode("utf-8", "replace") for x in av if x]
        if len(av) >= 2 and os.path.basename(av[1]) == "fs1_normalizar.py":
            vivos.append(int(pid))
    return sorted(vivos)


# ---------------------------------------------------------------------------
def _fixture():
    """Una copia reducida pero VALIDA de la funcion real, para poder ejecutarla."""
    return ('def bybit(kind, d, data, ctx):\n'
            '    emit, niveles, fnum, malo = ctx["emit"], ctx["niveles"], ctx["fnum"], ctx["malo"]\n'
            '    if kind == "book_delta":\n'
            '        accion = "SNAPSHOT" if d.get("type") == "snapshot" else "UPDATE"\n'
            '        ids = {"book_update_id": data.get("u")}\n'
            '        b, a = data.get("b"), data.get("a")\n'
            + VIEJO_DIALECTOS +
            '        niveles(b, "BID", accion, ids)\n'
            '        niveles(a, "ASK", accion, ids)\n'
            '        return True\n'
            '\n\n'
            'def okx(kind, d, data, ctx):\n'
            '    emit, niveles, fnum, malo = ctx["emit"], ctx["niveles"], ctx["fnum"], ctx["malo"]\n'
            '    items = data if isinstance(data, list) else []\n'
            + VIEJO_OKX_RAMA +
            '        for it in items:\n'
            '            if not isinstance(it, dict):\n'
            '                malo("BAD_SHAPE", "elemento de book_delta no es dict")\n'
            '                continue\n'
            + VIEJO_OKX_NIV +
            '            niveles(it.get("asks"), "ASK", accion, ids)\n'
            '        return True\n')


def _correr(codigo, data, fn="bybit", kind="book_delta", d=None):
    """Ejecuta el lector resultante contra un ctx de mentira y devuelve lo que hizo."""
    filas, cuar, nivs = [], [], []
    ctx = {"emit": lambda extra, flags=(): filas.append((dict(extra), tuple(flags))),
           "niveles": lambda pares, side, accion, ids=None: nivs.append((side, accion, list(pares or []))),
           "fnum": float,
           "malo": lambda code, detail="": (cuar.append(code), True)[1]}
    ns = {}
    exec(compile(codigo, "<fixture>", "exec"), ns)
    ns[fn](kind, d if d is not None else {"type": "delta"}, data, ctx)
    return filas, cuar, nivs


def autotest():
    fallos = []

    def check(nombre, cond, detalle=""):
        print("  %-58s %s %s" % (nombre, "ok" if cond else "FALLA", detalle if not cond else ""))
        if not cond:
            fallos.append(nombre)

    dial = _fixture()
    norm = "x = 1\n" + VIEJO_VERSION + "\ny = 2\n"
    d2, n2 = transformar(dial, norm)

    check("la version sube a 2.2", "fs1_normalizar/2.2" in n2)
    check("no queda rastro de la 2.1", "fs1_normalizar/2.1" not in n2)
    check("el latido ya no va a cuarentena", 'return malo("BOOK_DELTA_SIN_NIVELES"' not in d2)
    check("no se toca la rama con niveles", 'niveles(b, "BID", accion, ids)' in d2)

    # --- comportamiento, no texto ---
    LATIDO = {"s": "OPNUSDT", "b": [], "a": [], "u": 2519458, "seq": 170977139697}
    f, c, nv = _correr(dial, LATIDO)          # como estaba en 2.1
    check("2.1: el latido iba a cuarentena", c == ["BOOK_DELTA_SIN_NIVELES"] and not f, (f, c))
    f, c, nv = _correr(d2, LATIDO)            # como queda en 2.2
    check("2.2: el latido sale como fila", len(f) == 1 and not c, (f, c))
    if f:
        fila, flags = f[0]
        check("2.2: la fila lleva el u de la cadena", fila["book_update_id"] == 2519458, fila)
        check("2.2: la fila NO lleva precio ni cantidad",
              fila["price"] is None and fila["qty"] is None and fila["side"] is None, fila)
        check("2.2: queda marcada y contada", flags == ("BOOK_DELTA_SIN_NIVELES",), flags)
    check("2.2: el latido no toca el libro", nv == [], nv)

    CON_NIVELES = {"s": "CHIPUSDT", "b": [["0.04783", "12550"]], "a": [], "u": 5452256}
    f0, c0, nv0 = _correr(dial, CON_NIVELES)
    f1, c1, nv1 = _correr(d2, CON_NIVELES)
    check("2.2: un delta con niveles se comporta igual que en 2.1",
          (f0, c0, nv0) == (f1, c1, nv1) and nv1 and not c1, (nv0, nv1))

    SIN_U = {"s": "X", "b": [], "a": [], "seq": 1}
    f, c, nv = _correr(d2, SIN_U)
    check("2.2: sin u sigue yendo a cuarentena", c == ["BOOK_DELTA_SIN_NIVELES_NI_U"] and not f, (f, c))

    # --- okx: la foto en banda ---
    FOTO_OKX = [{"bids": [["0.03731", "427", "0", "4"]], "asks": [["0.03740", "100", "0", "2"]],
                 "ts": 1789261477207, "checksum": 0, "seqId": 729624411, "prevSeqId": -1}]
    f, c, nv = _correr(dial, FOTO_OKX, fn="okx", kind="book_snapshot", d={"action": "snapshot"})
    check("2.1: la foto en banda de okx no se atendia", (f, c, nv) == ([], [], []), (f, c, nv))
    f, c, nv = _correr(d2, FOTO_OKX, fn="okx", kind="book_snapshot", d={"action": "snapshot"})
    check("2.2: la foto en banda de okx se atiende", len(nv) == 2 and not c, (nv, c))
    check("2.2: y se marca como SNAPSHOT, no como UPDATE",
          bool(nv) and all(x[1] == "SNAPSHOT" for x in nv), nv)

    # --- okx: el latido ---
    LATIDO_OKX = [{"bids": [], "asks": [], "ts": 1, "seqId": 500, "prevSeqId": 499}]
    f, c, nv = _correr(dial, LATIDO_OKX, fn="okx")
    # en 2.1 se llamaba a niveles() con las listas vacias: cero filas emitidas. Eso es
    # exactamente lo que el normalizador contaba luego como EMPTY_RESULT.
    check("2.1: el latido de okx no emitia ni una fila",
          f == [] and c == [] and all(x[2] == [] for x in nv), (f, c, nv))
    f, c, nv = _correr(d2, LATIDO_OKX, fn="okx")
    check("2.2: el latido de okx sale como fila", len(f) == 1 and not c and not nv, (f, c, nv))
    if f:
        fila, flags = f[0]
        check("2.2: el latido de okx lleva seqId y prevSeqId",
              fila["book_update_id"] == 500 and fila["book_prev_update_id"] == 499, fila)

    # --- okx: un delta normal no cambia ---
    DELTA_OKX = [{"bids": [["1.0", "2", "0", "1"]], "asks": [], "seqId": 10, "prevSeqId": 9}]
    a0 = _correr(dial, DELTA_OKX, fn="okx")
    a1 = _correr(d2, DELTA_OKX, fn="okx")
    check("2.2: un delta de okx con niveles se comporta igual", a0 == a1 and a1[2], (a0, a1))

    # --- seguridad del propio parche ---
    try:
        transformar(d2, n2)
        check("aplicarlo dos veces se niega", False, "no levanto ValueError")
    except ValueError as e:
        check("aplicarlo dos veces se niega", "YA_APLICADO" in str(e), str(e))
    try:
        transformar("nada que ver", norm)
        check("con texto inesperado se niega", False, "no levanto ValueError")
    except ValueError as e:
        check("con texto inesperado se niega", "NO_ENCAJA" in str(e), str(e))

    check("el guardia no se detecta a si mismo", os.getpid() not in normalizadores_vivos())

    print("\n%s" % ("AUTOTEST_PASS" if not fallos else "AUTOTEST_FAIL: " + ", ".join(fallos)))
    return 0 if not fallos else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=BIN)
    ap.add_argument("--autotest", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--aplicar", action="store_true")
    ap.add_argument("--forzar", action="store_true",
                    help="aplica aunque haya normalizadores vivos. Sella evidencia falsa: "
                         "solo con los trabajos parados")
    a = ap.parse_args()

    if a.autotest:
        return autotest()

    p_d = os.path.join(a.bin, "fs1_dialectos.py")
    p_n = os.path.join(a.bin, "fs1_normalizar.py")
    for p in (p_d, p_n):
        if not os.path.isfile(p):
            print("NO_ESTA: %s" % p)
            return 2

    # El candado solo aplica a --aplicar: un ensayo no escribe nada y conviene poder
    # comprobar que los anclajes siguen encajando aunque haya trabajo en curso.
    vivos = normalizadores_vivos() if a.aplicar else []
    if vivos and not a.forzar:
        print("ABORTA: hay %d normalizador(es) vivo(s): %s" % (len(vivos), vivos))
        print("g1_particion sella el sha DESPUES de correr, asi que cambiar el fichero ahora")
        print("haria que esos trabajos firmaran su code_version real con el sha del binario nuevo.")
        print("Espera a que terminen, o pasa --forzar si sabes lo que eso implica.")
        return 3

    d, n = leer(p_d), leer(p_n)
    print("ANTES  fs1_dialectos.py   sha256=%s" % sha(d))
    print("ANTES  fs1_normalizar.py  sha256=%s" % sha(n))
    try:
        d2, n2 = transformar(d, n)
    except ValueError as e:
        print("NO_APLICA: %s" % e)
        return 4
    print("DESPUES fs1_dialectos.py  sha256=%s" % sha(d2))
    print("DESPUES fs1_normalizar.py sha256=%s" % sha(n2))

    if not a.aplicar:
        print("\n(dry-run: no se ha escrito nada. Anade --aplicar)")
        return 0

    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    for p, txt in ((p_d, d2), (p_n, n2)):
        copia = "%s.bak_2.1_%s" % (p, ts)
        with io.open(copia, "w", encoding="utf-8") as f:
            f.write(leer(p))
        tmp = p + ".tmp"
        with io.open(tmp, "w", encoding="utf-8") as f:
            f.write(txt)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
        print("escrito %s (copia: %s)" % (p, os.path.basename(copia)))

    r = subprocess.run([sys.executable, "-c",
                        "import ast,io;[ast.parse(io.open(p,encoding='utf-8').read()) for p in %r]"
                        % [p_d, p_n]])
    print("SINTAXIS_OK" if r.returncode == 0 else "SINTAXIS_ROTA")
    print("orders=0 execution_authority=NONE")
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
