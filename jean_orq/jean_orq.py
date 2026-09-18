#!/usr/bin/env python3
"""JEAN_NORMALIZADOR_ORQUESTADOR_V1 - gobierna fs1_normalizar 2.0 sobre las particiones del corte.

QUE HACE
  Convierte la normalizacion en un trabajo autonomo y durable: una cola de particiones
  (exchange x dia) con estado persistente, reanudacion, candados, verificacion real y
  recibos sellados. Corre como servicio: no depende de que una sesion de IA siga abierta.

QUE NO HACE, POR DISENO
  - No normaliza por su cuenta: ejecuta g1_particion.py, que a su vez ejecuta
    fs1_normalizar.py TAL CUAL. No hay un segundo normalizador. Dos normalizadores
    compitiendo producen dos datasets que no cuadran y nadie sabe cual creer.
  - No toca el crudo: RAW es inmutable. No borra, no mueve, no reescribe.
  - No toca la captura de mercado ni ningun servicio del grabador.
  - No conecta con exchanges, no usa credenciales, no envia ordenes.
    orders=0 y execution_authority=NONE en todos los recibos que emite.

POR QUE NO BASTA CON MIRAR SI EL PROCESO VIVE
  Un PID vivo no demuestra que el trabajo este sano, y una salida que existe no es un PASS.
  Para declarar PASS hay que recorrer la cadena entera:
    RAW sellado en G0 -> normalizador termina con 0 -> manifiesto coherente -> hashes
    correctos -> recuentos reconciliados -> cuarentena integra -> gate de 12 checks en PASS
  Cualquier eslabon que falle deja la particion en FAIL con su motivo, y las demas siguen.

CONVIVENCIA CON LO QUE YA ESTA CORRIENDO
  Si hay cadenas g1_cadena.sh vivas, el orquestador las detecta leyendo su linea de
  comandos, marca las particiones que cubren como EXTERNO y no las toca. Puede arrancar
  sin parar nada. Cuando una cadena externa termina, sus particiones vuelven a la cola.

ESTADOS
  NEW      descubierta, sin comprobar
  READY    lista para ejecutar (RAW presente y sellado en G0)
  BLOCK    no ejecutable por una causa declarada (sin G0, sin RAW, sin espacio)
  RUNNING  la esta ejecutando este orquestador
  EXTERNO  la esta ejecutando otro proceso (cadena lanzada a mano); no se toca
  VERIFY   termino de normalizar, se esta verificando
  PASS     verificada entera y sellada con recibo
  FAIL     fallo con motivo; no bloquea a las demas (fail-closed por particion)

USO (en PC3)
  python3 jean_orq.py autotest                 # pruebas sin datos reales ni red
  python3 jean_orq.py preflight                # comprueba entorno antes de nada
  python3 jean_orq.py plan                     # descubre particiones y adopta lo ya hecho
  python3 jean_orq.py estado [--json]          # informe legible o para maquina
  python3 jean_orq.py run --workers 2          # bucle de trabajo (esto es el servicio)
  python3 jean_orq.py run --una-vez --dry-run  # ensayo: dice que haria y no lo hace
  python3 jean_orq.py verificar --job okx_2026-09-13
  python3 jean_orq.py reintentar --todos-fail
"""
import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time

VERSION = "jean_orq/1.0.0"
ESQUEMA_ESTADO = "jean_orq_estado_v1"
ESQUEMA_RECIBO = "jean_orq_recibo_v1"

# ---------------------------------------------------------------------------
# Configuracion. Todo se puede sobreescribir con --config fichero.json
# ---------------------------------------------------------------------------
CONFIG_DEF = {
    "raw_root": "/home/jean/PC3_DISCO/JEAN_200_PC2/events",
    "market": "USDT_PERPETUAL",
    "derived": "/home/jean/PC3_DISCO/DERIVED_V1_fs1v20_20260918T0930Z",
    "base": "/home/jean/JEAN_FIVEDAY_CLAUDE",
    "g0_manifest": "/home/jean/JEAN_G0_FIVEDAY_CHATGPTPRO1/G0_OUTPUT/raw_manifest.jsonl",
    "g0_receipt": "/home/jean/JEAN_G0_FIVEDAY_CHATGPTPRO1/G0_OUTPUT/G0_RECEIPT.json",
    "py_gate": "/home/jean/jean-quant-lab/venv/bin/python",
    "py_driver": "python3",
    "orq_dir": "/home/jean/JEAN_ORQUESTADOR",
    "out_name": "normalized",
    "exchanges": ["binance", "bybit", "okx"],
    "dias": [],                      # vacio = todos los que haya en RAW
    "workers": 2,
    # Techo GLOBAL de normalizadores pesados a la vez en la maquina, contando los que
    # no ha lanzado este orquestador. Medido en PC3: cada fs1_normalizar pica en ~4,3 GB
    # de RSS; con 19 GB de RAM, 3 caben (12,9 GB) y 4 empiezan a tirar de swap.
    "max_normalizadores": 3,
    "margen_gib": 25.0,              # margen de disco que se deja libre siempre
    "factor_salida": 1.1,            # la salida pesa ~1x la entrada; se pide 1.1 por seguridad
    "ciclo_s": 60,                   # cada cuanto revisa el bucle
    "estancado_min": 45,             # sin avance en normalized.tmp => sospecha de cuelgue
    "matar_estancados": False,       # por defecto NO mata: marca y avisa
    "sha_completo": False,           # el gate (C2) ya verifica sha256 de toda la salida
    "umbral_connection_start": 200,  # norma del proyecto: por encima, investigar la particion
    "umbral_missing_event_ts": 0.05, # ratio de filas sin hora de evento que merece alerta
    # Contrato de version POR EXCHANGE. No es capricho: fs1_normalizar/2.0 recupera la hora
    # de evento solo con las claves de binance, asi que en bybit deja sin hora el 37,51 % de
    # las filas y en okx el 22,1 %. Para binance, 2.0 y 2.1 producen salida identica (medido:
    # 1959 = 1959 recuperaciones sobre los mismos ficheros), por eso ahi valen las dos y no
    # hay que rehacer 30 h de trabajo correcto.
    # 2.2 anade el latido de libro de bybit: un book_delta sin niveles GASTA un numero de
    # la cadena, y hasta 2.1 se tiraba a cuarentena. Medido: con el latido dentro bybit
    # encadena 320.467 pares sin una sola rotura; sin el, 1.175 agujeros en 3 ficheros.
    # Por eso bybit pasa a exigir 2.2 y punto. binance y okx no tienen ni un delta vacio
    # (0 de 236.214 y 0 de 136.286 medidos), asi que para ellos 2.2 produce exactamente lo
    # mismo que 2.1 y sus particiones ya hechas siguen valiendo.
    "code_versions_ok": {
        "binance": ["fs1_normalizar/2.0", "fs1_normalizar/2.1", "fs1_normalizar/2.2"],
        "bybit": ["fs1_normalizar/2.2"],
        "okx": ["fs1_normalizar/2.1", "fs1_normalizar/2.2"],
    },
    # sha256 esperado de CADA version. Dos versiones distintas son legitimas si estan
    # declaradas arriba; dos binarios distintos bajo la MISMA version, nunca.
    "sha_por_version": {},
    "normalizador_sha256": "",       # pin de version; vacio = se fija con la primera particion
    "max_intentos": 2,
}

ESTADOS_FINALES = ("PASS",)
ESTADOS_OCUPADOS = ("RUNNING", "EXTERNO", "VERIFY")
RE_DIA = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def utc():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def sha256_file(ruta, trozo=8 << 20):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for b in iter(lambda: f.read(trozo), b""):
            h.update(b)
    return h.hexdigest()


def sha256_obj(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def escribir_json_atomico(ruta, obj):
    """Escribe y renombra. Un corte de luz no deja un json a medias que luego se lea como bueno."""
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    tmp = ruta + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, ruta)


def leer_json(ruta, por_defecto=None):
    try:
        with open(ruta, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return por_defecto


def contar_lineas(ruta):
    n = 0
    with open(ruta, "rb") as f:
        for _ in f:
            n += 1
    return n


def libre_bytes(ruta):
    st = os.statvfs(ruta)
    return st.f_bavail * st.f_frsize


def log(cfg, msg):
    linea = "[%s] %s" % (utc(), msg)
    print(linea, flush=True)
    try:
        d = os.path.join(cfg["orq_dir"], "logs")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "orquestador.log"), "a", encoding="utf-8") as f:
            f.write(linea + "\n")
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Candado global: un solo orquestador a la vez
# ---------------------------------------------------------------------------
class CandadoGlobal:
    def __init__(self, ruta):
        self.ruta = ruta
        self.f = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.ruta), exist_ok=True)
        self.f = open(self.ruta, "w")
        try:
            fcntl.flock(self.f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.f.close()
            raise SystemExit("ABORTA: ya hay otro orquestador con el candado %s" % self.ruta)
        self.f.write("%d\n" % os.getpid())
        self.f.flush()
        return self

    def __exit__(self, *a):
        try:
            fcntl.flock(self.f.fileno(), fcntl.LOCK_UN)
            self.f.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Estado persistente
# ---------------------------------------------------------------------------
def ruta_estado(cfg):
    return os.path.join(cfg["orq_dir"], "estado.json")


def estado_nuevo(cfg):
    return {
        "schema_version": ESQUEMA_ESTADO,
        "orquestador_version": VERSION,
        "creado_utc": utc(),
        "actualizado_utc": utc(),
        "orders": 0,
        "execution_authority": "NONE",
        "config": {k: cfg[k] for k in ("raw_root", "derived", "out_name", "g0_manifest")},
        "normalizador_sha256": cfg.get("normalizador_sha256", ""),
        "jobs": {},
    }


def cargar_estado(cfg):
    e = leer_json(ruta_estado(cfg))
    if not e or e.get("schema_version") != ESQUEMA_ESTADO:
        return estado_nuevo(cfg)
    return e


def guardar_estado(cfg, est):
    est["actualizado_utc"] = utc()
    escribir_json_atomico(ruta_estado(cfg), est)


def job_id(exchange, dia):
    return "%s_%s" % (exchange, dia)


def set_estado(cfg, est, jid, nuevo, motivo=None, **extra):
    j = est["jobs"].setdefault(jid, {})
    viejo = j.get("estado")
    j["estado"] = nuevo
    j["motivo"] = motivo
    j["actualizado_utc"] = utc()
    j.update(extra)
    if viejo != nuevo:
        log(cfg, "ESTADO %s: %s -> %s%s" % (jid, viejo or "-", nuevo, (" (%s)" % motivo) if motivo else ""))
    return j


# ---------------------------------------------------------------------------
# Procesos externos: cadenas y normalizadores lanzados fuera del orquestador
# ---------------------------------------------------------------------------
def argv_de(pid):
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as f:
            return [a for a in f.read().decode("utf-8", "replace").split("\0") if a]
    except OSError:
        return []


def pids_vivos():
    for n in os.listdir("/proc"):
        if n.isdigit():
            yield int(n)


def particiones_externas(mi_pid=None):
    """Devuelve {job_id: pid} de particiones que esta procesando OTRO proceso.

    Se mira argv, nunca el texto completo de la linea: un comando que MENCIONA
    'g1_cadena.sh okx' (por ejemplo este mismo script, o un grep) no es una cadena.
    Esa confusion ya ha costado un susto; aqui no se repite.
    """
    mi_pid = mi_pid or os.getpid()
    fuera = {}
    for pid in pids_vivos():
        if pid == mi_pid:
            continue
        av = argv_de(pid)
        if len(av) < 3:
            continue
        # cadena secuencial:  bash <ruta>/g1_cadena.sh <exchange> <derived> [--wait-pid N] <dia>...
        if os.path.basename(av[1]) == "g1_cadena.sh":
            ex = av[2]
            for a in av[3:]:
                if RE_DIA.match(a):
                    fuera[job_id(ex, a)] = pid
            continue
        # driver de una particion suelta:  python3 g1_particion.py --exchange X --dia Y
        if os.path.basename(av[1]) == "g1_particion.py":
            ex = dia = None
            for i, a in enumerate(av):
                if a == "--exchange" and i + 1 < len(av):
                    ex = av[i + 1]
                elif a == "--dia" and i + 1 < len(av):
                    dia = av[i + 1]
            if ex and dia:
                fuera[job_id(ex, dia)] = pid
    return fuera


def normalizadores_vivos():
    """Cuantos fs1_normalizar.py corren en la maquina, los haya lanzado quien los haya lanzado.

    Hace falta contar tambien los ajenos: si tres cadenas a mano ya ocupan la RAM y el
    orquestador anade dos suyos, la maquina se va a swap y se arrastran todos, incluidos
    los que llevaban horas de trabajo hecho.
    """
    n = 0
    for pid in pids_vivos():
        av = argv_de(pid)
        if len(av) >= 2 and os.path.basename(av[1]) == "fs1_normalizar.py":
            n += 1
    return n


# ---------------------------------------------------------------------------
# Descubrimiento de particiones
# ---------------------------------------------------------------------------
def cargar_g0(cfg):
    """Devuelve el conjunto de rutas relativas selladas en G0 con status OK."""
    ok = set()
    total = 0
    with open(cfg["g0_manifest"], encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            r = json.loads(linea)
            total += 1
            if r.get("status") == "OK":
                ok.add(r.get("relative_path"))
    return ok, total


def dir_particion(cfg, exchange, dia):
    return os.path.join(cfg["derived"], "particiones", "%s_%s" % (exchange, dia))


def dir_raw(cfg, exchange, dia):
    return os.path.join(cfg["raw_root"], exchange, cfg["market"], dia)


def descubrir(cfg):
    """Lista de (exchange, dia, n_ficheros, bytes) a partir del crudo presente."""
    res = []
    for ex in cfg["exchanges"]:
        base = os.path.join(cfg["raw_root"], ex, cfg["market"])
        if not os.path.isdir(base):
            continue
        for dia in sorted(os.listdir(base)):
            if not RE_DIA.match(dia):
                continue
            if cfg["dias"] and dia not in cfg["dias"]:
                continue
            d = os.path.join(base, dia)
            ficheros = [fn for fn in os.listdir(d) if fn.endswith(".parquet")]
            if not ficheros:
                continue
            tam = sum(os.path.getsize(os.path.join(d, fn)) for fn in ficheros)
            res.append((ex, dia, len(ficheros), tam))
    return res


# ---------------------------------------------------------------------------
# Verificacion: lo que convierte "termino" en PASS
# ---------------------------------------------------------------------------
def verificar_particion(cfg, exchange, dia, correr_gate=True):
    """Devuelve (ok, motivo, evidencia, alertas). No modifica nada."""
    part = dir_particion(cfg, exchange, dia)
    out = cfg["out_name"]
    ev = {"particion": part}
    alertas = []

    if not os.path.isdir(part):
        return False, "SIN_PARTICION", ev, alertas

    med = leer_json(os.path.join(part, "MEDICION_%s.json" % out.upper()))
    man = leer_json(os.path.join(part, "MANIFEST_%s.json" % out.upper()))
    if med is None:
        return False, "SIN_MEDICION", ev, alertas
    if man is None:
        return False, "SIN_MANIFIESTO", ev, alertas

    # 1. el normalizador termino bien
    if med.get("returncode") != 0:
        ev["returncode"] = med.get("returncode")
        return False, "RETURNCODE_NO_CERO", ev, alertas

    # 2. version del codigo, fijada y estable en todo el corte
    ev["code_version"] = man.get("code_version")
    ev["normalizador_sha256"] = med.get("normalizador_sha256")
    versiones_ok = (cfg.get("code_versions_ok") or {}).get(exchange)
    if versiones_ok and man.get("code_version") not in versiones_ok:
        ev["code_versions_ok"] = versiones_ok
        return False, "CODE_VERSION_NO_ADMITIDA_PARA_ESTE_VENUE", ev, alertas
    if man.get("schema_version") != "market_event_v1":
        return False, "SCHEMA_INESPERADO", ev, alertas
    esperado = (cfg.get("sha_por_version") or {}).get(man.get("code_version"))
    if esperado and med.get("normalizador_sha256") and med["normalizador_sha256"] != esperado:
        ev["sha_esperado_para_esa_version"] = esperado
        return False, "NORMALIZADOR_CAMBIO_SIN_SUBIR_VERSION", ev, alertas

    # 3. entrada: tantos ficheros como enlaces hay en raw/
    raw_dir = os.path.join(part, "raw")
    n_raw = len(os.listdir(raw_dir)) if os.path.isdir(raw_dir) else 0
    ev["input_files_manifiesto"] = man.get("input_files")
    ev["input_files_raw"] = n_raw
    if man.get("input_files") != n_raw:
        return False, "INPUT_FILES_NO_CUADRA", ev, alertas

    # 4. salida: un hash por fichero, y el fichero existe
    outs = man.get("output_sha256") or {}
    ev["output_files"] = len(outs)
    if len(outs) != man.get("input_files"):
        return False, "OUTPUT_FILES_NO_CUADRA", ev, alertas
    out_dir = os.path.join(part, out)
    if not os.path.isdir(out_dir):
        return False, "SIN_DIRECTORIO_SALIDA", ev, alertas
    faltan = [k for k in outs if not os.path.isfile(os.path.join(out_dir, k))]
    if faltan:
        ev["faltan_ejemplo"] = faltan[:3]
        ev["faltan"] = len(faltan)
        return False, "FALTAN_FICHEROS_DE_SALIDA", ev, alertas
    if cfg.get("sha_completo"):
        malos = [k for k, v in outs.items() if sha256_file(os.path.join(out_dir, k)) != v]
        if malos:
            ev["hash_malos"] = malos[:3]
            return False, "HASH_SALIDA_NO_COINCIDE", ev, alertas

    # 5. cuarentena integra: lo que se aparta tambien se sella
    tot = man.get("totals") or {}
    cfich = man.get("cuarentena_fichero")
    n_cuar = tot.get("cuarentena")
    ev["cuarentena_filas"] = n_cuar
    if cfich:
        cruta = os.path.join(part, cfich)
        if not os.path.isfile(cruta):
            return False, "SIN_FICHERO_CUARENTENA", ev, alertas
        if sha256_file(cruta) != man.get("cuarentena_sha256"):
            return False, "HASH_CUARENTENA_NO_COINCIDE", ev, alertas
        if n_cuar is not None and contar_lineas(cruta) != n_cuar:
            return False, "LINEAS_CUARENTENA_NO_CUADRAN", ev, alertas

    # 6. recuentos
    ev["filas_crudas"] = tot.get("filas_crudas")
    ev["filas_normalizadas"] = tot.get("filas_normalizadas")
    if not tot.get("filas_normalizadas"):
        return False, "SIN_FILAS_NORMALIZADAS", ev, alertas

    # 7. senales de calidad: no bloquean, pero quedan escritas en el recibo
    control = tot.get("control") or {}
    cs = control.get("connection_start")
    if cs is not None:
        ev["connection_start"] = cs
        if cs > cfg["umbral_connection_start"]:
            alertas.append("CONNECTION_START_ALTO: %d > %d; la norma pide investigar esta particion "
                           "antes de usarla (reconexiones invalidan el libro)" % (cs, cfg["umbral_connection_start"]))
    flags = tot.get("flags") or {}
    mets = flags.get("MISSING_EVENT_TS")
    if mets and tot.get("filas_normalizadas"):
        ratio = mets / float(tot["filas_normalizadas"])
        ev["missing_event_ts"] = mets
        ev["missing_event_ts_ratio"] = round(ratio, 6)
        if ratio > cfg["umbral_missing_event_ts"]:
            alertas.append("MISSING_EVENT_TS_ALTO: %.2f%% de las filas sin hora de evento; "
                           "cualquier feature as-of sobre ellas cae a receive_ts" % (ratio * 100))

    # 8. el gate de 12 checks, que es quien manda
    if correr_gate:
        gate = os.path.join(cfg["base"], "g1_gate_check.py")
        out_dir_gate = os.path.join(cfg["base"], "G1_VERIFICACION")
        gate_json = os.path.join(out_dir_gate, "G1_GATE_%s_%s.json" % ("%s_%s" % (exchange, dia), out.upper()))
        man_path = os.path.join(part, "MANIFEST_%s.json" % out.upper())
        # Si ya hay un veredicto sellado POSTERIOR al manifiesto, vale: repetir un gate que
        # lee decenas de GB no anade garantia, solo quita CPU a las normalizaciones en curso.
        # Si el manifiesto es mas nuevo que el veredicto, el veredicto no sirve y se rehace.
        gj_previo = leer_json(gate_json)
        if (gj_previo and gj_previo.get("verdict") == "PASS"
                and os.path.isfile(man_path)
                and os.path.getmtime(gate_json) >= os.path.getmtime(man_path)):
            ev["gate_verdict"] = gj_previo.get("verdict")
            ev["gate_pass"] = "%s/%s" % (gj_previo.get("pass"), gj_previo.get("total"))
            ev["gate_reutilizado"] = True
            return True, None, ev, alertas
        cmd = [cfg["py_gate"], gate, "--particion", part, "--out-name", out,
               "--out-dir", out_dir_gate, "--exchange", exchange]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        except (OSError, subprocess.TimeoutExpired) as exc:
            ev["gate_error"] = str(exc)
            return False, "GATE_NO_EJECUTABLE", ev, alertas
        ev["gate_returncode"] = p.returncode
        gj = leer_json(os.path.join(out_dir_gate, "G1_GATE_%s_%s.json" % ("%s_%s" % (exchange, dia), out.upper())))
        if gj:
            ev["gate_verdict"] = gj.get("verdict")
            ev["gate_pass"] = "%s/%s" % (gj.get("pass"), gj.get("total"))
        if p.returncode != 0:
            ev["gate_stdout_cola"] = (p.stdout or "")[-700:]
            return False, "GATE_G1_FAIL", ev, alertas

    return True, None, ev, alertas


def registrar_sha_version(cfg, est, ev):
    """Aprende el sha256 del normalizador para cada version que aparece.

    La primera particion de una version fija su sha. A partir de ahi, esa version tiene
    que salir siempre del mismo binario: si cambia sin subir el numero, es un cambio
    silencioso y la particion se rechaza.
    """
    ver = ev.get("code_version")
    sha = ev.get("normalizador_sha256")
    if not ver or not sha:
        return
    pines = est.setdefault("sha_por_version", {})
    if ver not in pines:
        pines[ver] = sha
        log(cfg, "version %s fijada al normalizador %s" % (ver, sha[:16]))
    cfg["sha_por_version"] = dict(pines)


def escribir_recibo(cfg, exchange, dia, ok, motivo, ev, alertas, extra=None):
    rec = {
        "schema_version": ESQUEMA_RECIBO,
        "orquestador_version": VERSION,
        "creado_utc": utc(),
        "nodo": os.uname().nodename,
        "exchange": exchange,
        "dia": dia,
        "particion": dir_particion(cfg, exchange, dia),
        "veredicto": "PASS" if ok else "FAIL",
        "motivo": motivo,
        "evidencia": ev,
        "alertas": alertas,
        "orders": 0,
        "execution_authority": "NONE",
    }
    if extra:
        rec.update(extra)
    rec["recibo_sha256"] = sha256_obj({k: v for k, v in rec.items() if k != "recibo_sha256"})
    escribir_json_atomico(os.path.join(cfg["orq_dir"], "recibos", "RECIBO_%s.json" % job_id(exchange, dia)), rec)
    return rec


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
def preflight(cfg):
    problemas, avisos, datos = [], [], {}

    for clave, ruta in (("raw_root", cfg["raw_root"]), ("derived", cfg["derived"]), ("base", cfg["base"])):
        if not os.path.isdir(ruta):
            problemas.append("no existe %s: %s" % (clave, ruta))
    for clave in ("g0_manifest", "g0_receipt"):
        if not os.path.isfile(cfg[clave]):
            problemas.append("no existe %s: %s" % (clave, cfg[clave]))
    for nombre in ("g1_particion.py", "g1_gate_check.py"):
        if not os.path.isfile(os.path.join(cfg["base"], nombre)):
            problemas.append("falta %s en %s" % (nombre, cfg["base"]))
    if not os.path.isfile(cfg["py_gate"]):
        problemas.append("no existe el interprete del gate: %s" % cfg["py_gate"])

    rec0 = leer_json(cfg["g0_receipt"], {})
    datos["g0_status"] = rec0.get("status")
    datos["g0_scope"] = rec0.get("scope")
    datos["g0_file_count"] = rec0.get("file_count")
    if rec0.get("status") != "PASS":
        problemas.append("G0 no esta en PASS (status=%s): no se normaliza sobre un sello que no paso"
                         % rec0.get("status"))

    # version del normalizador: se fija una vez y no puede cambiar a mitad del corte
    fs1 = "/home/jean/JEAN_FEATURE_STORE/bin/fs1_normalizar.py"
    if os.path.isfile(fs1):
        datos["normalizador_sha256"] = sha256_file(fs1)
        if cfg.get("normalizador_sha256") and cfg["normalizador_sha256"] != datos["normalizador_sha256"]:
            problemas.append("el normalizador no coincide con el pin de la configuracion")
    else:
        avisos.append("no encuentro %s para fijar el pin de version" % fs1)

    # copias sueltas del codigo: divergencia silenciosa ya ha mordido antes
    copias = {}
    for raiz in ("/home/jean/JEAN_FEATURE_STORE/bin", "/home/jean/JEAN_G0_FIVEDAY_CHATGPTPRO1"):
        if not os.path.isdir(raiz):
            continue
        for fn in sorted(os.listdir(raiz)):
            if fn.startswith(("fs1_dialectos", "fs1_normalizar", "fs2_libro")) and fn.endswith(".py"):
                copias[os.path.join(raiz, fn)] = sha256_file(os.path.join(raiz, fn))[:16]
    datos["copias_codigo"] = copias
    for fam in ("fs1_dialectos", "fs1_normalizar", "fs2_libro"):
        hs = {v for k, v in copias.items() if os.path.basename(k).startswith(fam)}
        if len(hs) > 1:
            avisos.append("hay %d versiones distintas de %s en disco: un renombrado accidental cambia "
                          "el comportamiento sin avisar" % (len(hs), fam))

    libre = libre_bytes(cfg["derived"]) if os.path.isdir(cfg["derived"]) else 0
    datos["libre_gb"] = round(libre / 1e9, 1)

    trabajos = descubrir(cfg) if os.path.isdir(cfg["raw_root"]) else []
    datos["particiones_descubiertas"] = len(trabajos)
    datos["bytes_entrada_total"] = sum(t[3] for t in trabajos)
    necesario_total = int(datos["bytes_entrada_total"] * cfg["factor_salida"])
    datos["necesario_total_gb"] = round(necesario_total / 1e9, 1)
    if trabajos and libre < necesario_total:
        avisos.append("el corte completo no cabe: harian falta ~%.0f GB de salida y hay %.0f GB libres. "
                      "Se procesara por tandas mientras quepa, y las que no quepan quedaran en BLOCK "
                      "en vez de llenar el disco a medias"
                      % (necesario_total / 1e9, libre / 1e9))

    externas = particiones_externas()
    datos["particiones_externas"] = externas
    if externas:
        avisos.append("hay %d particiones en manos de procesos externos; no se tocaran" % len(externas))

    return problemas, avisos, datos


# ---------------------------------------------------------------------------
# Plan: descubrir, adoptar lo hecho, dejar la cola lista
# ---------------------------------------------------------------------------
def plan(cfg, est, correr_gate=True):
    g0_ok, _ = cargar_g0(cfg)
    externas = particiones_externas()
    for ex, dia, n, tam in descubrir(cfg):
        jid = job_id(ex, dia)
        j = est["jobs"].setdefault(jid, {"exchange": ex, "dia": dia, "intentos": 0})
        j["exchange"], j["dia"] = ex, dia
        j["ficheros_crudo"], j["bytes_crudo"] = n, tam

        if j.get("estado") in ESTADOS_FINALES:
            continue

        # 1. adoptar lo que ya esta hecho, en vez de repetirlo.
        #    El manifiesto se escribe al final: si existe, esa particion ya no la esta
        #    produciendo nadie, aunque la cadena que la hizo siga viva con otros dias.
        #    Por eso se mira ANTES que los procesos externos: si no, una particion
        #    terminada se quedaria en EXTERNO hasta que muriera toda la cadena.
        man_path = os.path.join(dir_particion(cfg, ex, dia), "MANIFEST_%s.json" % cfg["out_name"].upper())
        if os.path.isfile(man_path):
            ok, motivo, ev, alertas = verificar_particion(cfg, ex, dia, correr_gate=correr_gate)
            if ok:
                escribir_recibo(cfg, ex, dia, True, None, ev, alertas, {"adoptada": True})
                set_estado(cfg, est, jid, "PASS", "adoptada: ya estaba hecha y verifica",
                           evidencia=ev, alertas=alertas)
                registrar_sha_version(cfg, est, ev)
                continue

        # 2. lo que otro proceso esta haciendo, no se toca
        if jid in externas:
            set_estado(cfg, est, jid, "EXTERNO", "pid %d" % externas[jid], pid=externas[jid])
            continue

        # 3. sin sello G0 no se procesa: normalizar lo no sellado rompe la cadena de custodia
        rd = dir_raw(cfg, ex, dia)
        rel = ["%s/%s/%s/%s" % (ex, cfg["market"], dia, fn)
               for fn in os.listdir(rd) if fn.endswith(".parquet")]
        sin_sello = [r for r in rel if r not in g0_ok]
        if sin_sello:
            set_estado(cfg, est, jid, "BLOCK", "SIN_G0: %d ficheros sin sellar (p.ej. %s)"
                       % (len(sin_sello), sin_sello[0]))
            continue

        if j.get("estado") == "FAIL" and j.get("intentos", 0) >= cfg["max_intentos"]:
            continue
        set_estado(cfg, est, jid, "READY", None)
    return est


# ---------------------------------------------------------------------------
# Ejecucion
# ---------------------------------------------------------------------------
def cabe_en_disco(cfg, bytes_entrada):
    libre = libre_bytes(cfg["derived"])
    necesario = int(bytes_entrada * cfg["factor_salida"]) + int(cfg["margen_gib"] * (1 << 30))
    return libre >= necesario, libre, necesario


def lanzar(cfg, est, jid):
    j = est["jobs"][jid]
    ex, dia = j["exchange"], j["dia"]
    cabe, libre, necesario = cabe_en_disco(cfg, j.get("bytes_crudo", 0))
    if not cabe:
        set_estado(cfg, est, jid, "BLOCK", "DISCO_INSUFICIENTE libre=%.1fGB necesario=%.1fGB"
                   % (libre / 1e9, necesario / 1e9))
        return None
    logs = os.path.join(cfg["orq_dir"], "logs")
    os.makedirs(logs, exist_ok=True)
    lp = os.path.join(logs, "job_%s.log" % jid)
    cmd = [cfg["py_driver"], os.path.join(cfg["base"], "g1_particion.py"),
           "--exchange", ex, "--dia", dia, "--derived", cfg["derived"], "--out-name", cfg["out_name"]]
    lf = open(lp, "a", encoding="utf-8")
    lf.write("\n=== %s LANZA %s ===\n%s\n" % (utc(), jid, " ".join(cmd)))
    lf.flush()
    p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    j["intentos"] = j.get("intentos", 0) + 1
    set_estado(cfg, est, jid, "RUNNING", None, pid=p.pid, inicio_utc=utc(),
               log=lp, avance=None, avance_utc=utc())
    log(cfg, "LANZADO %s pid=%d (intento %d)" % (jid, p.pid, j["intentos"]))
    return p


def avance_de(cfg, ex, dia):
    """Cuantos ficheros lleva escritos y cuando fue el ultimo. Un contador que no sube es un cuelgue."""
    tmp = os.path.join(dir_particion(cfg, ex, dia), cfg["out_name"] + ".tmp")
    if not os.path.isdir(tmp):
        return 0, 0.0
    ns = os.listdir(tmp)
    ultimo = 0.0
    for fn in ns:
        try:
            ultimo = max(ultimo, os.path.getmtime(os.path.join(tmp, fn)))
        except OSError:
            pass
    return len(ns), ultimo


def revisar_en_curso(cfg, est, procesos):
    """Cierra los que han terminado y vigila los que siguen."""
    for jid, p in list(procesos.items()):
        j = est["jobs"][jid]
        ex, dia = j["exchange"], j["dia"]
        rc = p.poll()
        if rc is None:
            n, ultimo = avance_de(cfg, ex, dia)
            if n != j.get("avance"):
                j["avance"], j["avance_utc"] = n, utc()
                j.pop("estancado_desde", None)
            else:
                quieto_s = time.time() - ultimo if ultimo else 0
                if quieto_s > cfg["estancado_min"] * 60:
                    if "estancado_desde" not in j:
                        j["estancado_desde"] = utc()
                        log(cfg, "AVISO %s sin avance desde hace %.0f min (%d ficheros)"
                            % (jid, quieto_s / 60, n))
                    if cfg["matar_estancados"]:
                        log(cfg, "MATANDO %s por estancamiento" % jid)
                        p.terminate()
            continue

        del procesos[jid]
        j["fin_utc"] = utc()
        if rc != 0:
            escribir_recibo(cfg, ex, dia, False, "DRIVER_RC_%d" % rc, {"returncode": rc}, [])
            set_estado(cfg, est, jid, "FAIL", "el driver termino con rc=%d" % rc)
            continue
        set_estado(cfg, est, jid, "VERIFY", None)
        ok, motivo, ev, alertas = verificar_particion(cfg, ex, dia)
        escribir_recibo(cfg, ex, dia, ok, motivo, ev, alertas)
        if ok:
            registrar_sha_version(cfg, est, ev)
            set_estado(cfg, est, jid, "PASS", None, evidencia=ev, alertas=alertas)
            for a in alertas:
                log(cfg, "ALERTA %s: %s" % (jid, a))
        else:
            set_estado(cfg, est, jid, "FAIL", motivo, evidencia=ev, alertas=alertas)


def siguiente_listo(cfg, est, externas):
    """Elige el siguiente trabajo. Dia mas antiguo primero: el corte se cierra por dias completos."""
    cands = [(j["dia"], j["exchange"], jid) for jid, j in est["jobs"].items()
             if j.get("estado") == "READY" and jid not in externas]
    if not cands:
        return None
    cands.sort()
    return cands[0][2]


def run(cfg, una_vez=False, dry_run=False):
    est = cargar_estado(cfg)
    procesos = {}
    log(cfg, "%s arranca | workers=%d | derived=%s" % (VERSION, cfg["workers"], cfg["derived"]))
    try:
        while True:
            externas = particiones_externas()
            # una particion que estaba en manos ajenas y ya nadie procesa, vuelve a la cola
            for jid, j in est["jobs"].items():
                if j.get("estado") == "EXTERNO" and jid not in externas:
                    set_estado(cfg, est, jid, "NEW", "el proceso externo termino")
            est = plan(cfg, est, correr_gate=True)
            revisar_en_curso(cfg, est, procesos)

            # cupo: ni mas workers propios de los configurados, ni mas normalizadores
            # pesados en la maquina de los que la RAM aguanta (contando los ajenos)
            vivos = normalizadores_vivos()
            cupo = min(cfg["workers"] - len(procesos), max(0, cfg["max_normalizadores"] - vivos))
            if cupo <= 0 and vivos >= cfg["max_normalizadores"]:
                log(cfg, "en espera: ya hay %d normalizadores vivos (techo %d)"
                    % (vivos, cfg["max_normalizadores"]))
            while cupo > 0:
                jid = siguiente_listo(cfg, est, externas)
                if not jid:
                    break
                cupo -= 1
                if dry_run:
                    log(cfg, "DRY-RUN: lanzaria %s" % jid)
                    set_estado(cfg, est, jid, "BLOCK", "dry-run")
                    continue
                p = lanzar(cfg, est, jid)
                if p:
                    procesos[jid] = p

            guardar_estado(cfg, est)
            pend = [j for j in est["jobs"].values()
                    if j.get("estado") in ("READY", "NEW", "RUNNING", "VERIFY", "EXTERNO")]
            if una_vez or (not procesos and not pend):
                break
            time.sleep(cfg["ciclo_s"])
    except KeyboardInterrupt:
        log(cfg, "interrumpido: los procesos hijos siguen; al reanudar se adoptan")
    finally:
        guardar_estado(cfg, est)
        recibo_global(cfg, est)
    return est


def recibo_global(cfg, est, escribir=True):
    cuenta = {}
    for j in est["jobs"].values():
        cuenta[j.get("estado", "NEW")] = cuenta.get(j.get("estado", "NEW"), 0) + 1
    rec = {
        "schema_version": "jean_orq_recibo_global_v1",
        "orquestador_version": VERSION,
        "creado_utc": utc(),
        "nodo": os.uname().nodename,
        "derived": cfg["derived"],
        "normalizador_sha256": est.get("normalizador_sha256"),
        "total": len(est["jobs"]),
        "por_estado": cuenta,
        "jobs": {k: {"estado": v.get("estado"), "motivo": v.get("motivo"),
                     "alertas": v.get("alertas") or []} for k, v in sorted(est["jobs"].items())},
        "orders": 0,
        "execution_authority": "NONE",
    }
    rec["recibo_sha256"] = sha256_obj({k: v for k, v in rec.items() if k != "recibo_sha256"})
    if escribir:
        escribir_json_atomico(os.path.join(cfg["orq_dir"], "RECIBO_ORQUESTADOR.json"), rec)
    return rec


# ---------------------------------------------------------------------------
# Informe
# ---------------------------------------------------------------------------
def informe(cfg, est, como_json=False, escribir_recibo_global=True):
    if como_json:
        print(json.dumps({"estado": est, "recibo": recibo_global(cfg, est, escribir=escribir_recibo_global)},
                         indent=1, sort_keys=True))
        return
    jobs = est["jobs"]
    cuenta = {}
    for j in jobs.values():
        cuenta[j.get("estado", "NEW")] = cuenta.get(j.get("estado", "NEW"), 0) + 1
    print("JEAN NORMALIZACION - %s" % cfg["derived"])
    print("TOTAL: %d" % len(jobs))
    for e in ("PASS", "RUNNING", "EXTERNO", "VERIFY", "READY", "NEW", "BLOCK", "FAIL"):
        if cuenta.get(e):
            print("  %-8s %d" % (e, cuenta[e]))
    for etiqueta, estados in (("EN CURSO", ("RUNNING", "VERIFY", "EXTERNO")),
                              ("SIGUIENTES", ("READY",)),
                              ("BLOQUEADAS", ("BLOCK",)),
                              ("FALLOS", ("FAIL",))):
        filas = [(k, v) for k, v in sorted(jobs.items()) if v.get("estado") in estados]
        if not filas:
            continue
        print("\n%s" % etiqueta)
        for k, v in filas[:12]:
            extra = ""
            if v.get("estado") in ("RUNNING", "EXTERNO"):
                n, _ = avance_de(cfg, v["exchange"], v["dia"])
                tot = v.get("ficheros_crudo") or 0
                extra = "  %d/%d  %.1f%%" % (n, tot, (100.0 * n / tot) if tot else 0)
            print("  %-22s %-8s%s%s" % (k, v.get("estado"), extra,
                                        ("  <- " + v["motivo"]) if v.get("motivo") else ""))
        if len(filas) > 12:
            print("  ... y %d mas" % (len(filas) - 12))
    alertas = [(k, a) for k, v in sorted(jobs.items()) for a in (v.get("alertas") or [])]
    if alertas:
        print("\nALERTAS DE CALIDAD (no bloquean, pero quedan en el recibo)")
        for k, a in alertas[:10]:
            print("  %-22s %s" % (k, a))
    libre = libre_bytes(cfg["derived"]) if os.path.isdir(cfg["derived"]) else 0
    pendiente = sum(j.get("bytes_crudo", 0) for j in jobs.values()
                    if j.get("estado") in ("READY", "NEW", "BLOCK"))
    print("\nDISCO libre=%.0f GB | salida pendiente estimada=%.0f GB" % (libre / 1e9, pendiente * cfg["factor_salida"] / 1e9))
    if pendiente * cfg["factor_salida"] > libre:
        print("  AVISO: no cabe todo lo pendiente. Se hara por tandas; lo que no quepa queda en BLOCK.")


# ---------------------------------------------------------------------------
# Autotest: sin datos reales, sin red
# ---------------------------------------------------------------------------
def autotest():
    import tempfile
    fallos = []

    def check(nombre, cond, detalle=""):
        if cond:
            print("  PASS  %s" % nombre)
        else:
            print("  FAIL  %s %s" % (nombre, detalle))
            fallos.append(nombre)

    raiz = tempfile.mkdtemp(prefix="jean_orq_test_")
    try:
        cfg = dict(CONFIG_DEF)
        cfg.update({
            "raw_root": os.path.join(raiz, "events"),
            "derived": os.path.join(raiz, "derived"),
            "base": os.path.join(raiz, "base"),
            "orq_dir": os.path.join(raiz, "orq"),
            "g0_manifest": os.path.join(raiz, "g0.jsonl"),
            "g0_receipt": os.path.join(raiz, "g0_receipt.json"),
            "exchanges": ["binance"],
            "dias": [],
            "margen_gib": 0.0,
        })
        dia = "2026-01-01"
        rd = os.path.join(cfg["raw_root"], "binance", cfg["market"], dia)
        os.makedirs(rd)
        for i in range(3):
            with open(os.path.join(rd, "events-%d.parquet" % i), "wb") as f:
                f.write(b"x" * 100)

        print("AUTOTEST %s" % VERSION)

        # 1. escritura atomica
        r = os.path.join(raiz, "a", "b.json")
        escribir_json_atomico(r, {"k": 1})
        check("escritura atomica y relectura", leer_json(r) == {"k": 1})
        check("no queda fichero .tmp", not os.path.exists(r + ".tmp"))

        # 2. descubrimiento
        trab = descubrir(cfg)
        check("descubre la particion", len(trab) == 1 and trab[0][:3] == ("binance", dia, 3), str(trab))

        # 3. G0 incompleto => BLOCK, nunca READY
        with open(cfg["g0_manifest"], "w") as f:
            f.write(json.dumps({"relative_path": "binance/%s/%s/events-0.parquet" % (cfg["market"], dia),
                                "status": "OK", "sha256": "x", "size_bytes": 100}) + "\n")
        escribir_json_atomico(cfg["g0_receipt"], {"status": "PASS"})
        est = estado_nuevo(cfg)
        est = plan(cfg, est, correr_gate=False)
        jid = job_id("binance", dia)
        check("sin sello G0 la particion queda BLOCK",
              est["jobs"][jid]["estado"] == "BLOCK", est["jobs"][jid].get("motivo", ""))

        # 4. G0 completo => READY
        with open(cfg["g0_manifest"], "w") as f:
            for i in range(3):
                f.write(json.dumps({"relative_path": "binance/%s/%s/events-%d.parquet" % (cfg["market"], dia, i),
                                    "status": "OK", "sha256": "x", "size_bytes": 100}) + "\n")
        est = plan(cfg, estado_nuevo(cfg), correr_gate=False)
        check("con sello G0 completo pasa a READY", est["jobs"][jid]["estado"] == "READY")

        # 5. verificacion: sin artefactos no puede dar PASS
        ok, motivo, _, _ = verificar_particion(cfg, "binance", dia, correr_gate=False)
        check("sin artefactos no da PASS", (not ok) and motivo in ("SIN_PARTICION", "SIN_MEDICION"), str(motivo))

        # 6. verificacion: manifiesto que miente sobre el numero de ficheros => FAIL
        part = dir_particion(cfg, "binance", dia)
        os.makedirs(os.path.join(part, "raw"))
        os.makedirs(os.path.join(part, "normalized"))
        os.makedirs(os.path.join(part, "cuarentena"))
        for i in range(3):
            os.symlink(os.path.join(rd, "events-%d.parquet" % i), os.path.join(part, "raw", "events-%d.parquet" % i))
        escribir_json_atomico(os.path.join(part, "MEDICION_NORMALIZED.json"),
                              {"returncode": 0, "normalizador_sha256": "aa"})
        escribir_json_atomico(os.path.join(part, "MANIFEST_NORMALIZED.json"), {
            "code_version": "fs1_normalizar/2.0", "schema_version": "market_event_v1",
            "input_files": 99, "output_sha256": {}, "totals": {"filas_normalizadas": 10}})
        ok, motivo, _, _ = verificar_particion(cfg, "binance", dia, correr_gate=False)
        check("manifiesto que no cuadra con raw/ da FAIL", (not ok) and motivo == "INPUT_FILES_NO_CUADRA", str(motivo))

        # 7. verificacion completa coherente => PASS, y con las alertas de calidad
        salidas = {}
        for i in range(3):
            fn = "events-%d.market_event_v1.parquet" % i
            p = os.path.join(part, "normalized", fn)
            with open(p, "wb") as f:
                f.write(b"y" * 10)
            salidas[fn] = sha256_file(p)
        cuar = os.path.join(part, "cuarentena", "c.jsonl")
        with open(cuar, "w") as f:
            f.write("{}\n{}\n")
        escribir_json_atomico(os.path.join(part, "MANIFEST_NORMALIZED.json"), {
            "code_version": "fs1_normalizar/2.0", "schema_version": "market_event_v1",
            "input_files": 3, "output_sha256": salidas,
            "cuarentena_fichero": "cuarentena/c.jsonl", "cuarentena_sha256": sha256_file(cuar),
            "totals": {"filas_crudas": 100, "filas_normalizadas": 1000, "cuarentena": 2,
                       "control": {"connection_start": 500},
                       "flags": {"MISSING_EVENT_TS": 400}}})
        cfg["sha_completo"] = True
        ok, motivo, ev, alertas = verificar_particion(cfg, "binance", dia, correr_gate=False)
        check("particion coherente da PASS", ok, str(motivo))
        check("avisa de connection_start por encima del umbral",
              any("CONNECTION_START_ALTO" in a for a in alertas), str(alertas))
        check("avisa de MISSING_EVENT_TS alto",
              any("MISSING_EVENT_TS_ALTO" in a for a in alertas), str(alertas))

        # 8. hash de salida manipulado => FAIL
        with open(os.path.join(part, "normalized", "events-0.market_event_v1.parquet"), "wb") as f:
            f.write(b"MANIPULADO")
        ok, motivo, _, _ = verificar_particion(cfg, "binance", dia, correr_gate=False)
        check("un fichero de salida alterado da FAIL", (not ok) and motivo == "HASH_SALIDA_NO_COINCIDE", str(motivo))

        # 9. cambiar el binario SIN subir el numero de version => FAIL
        with open(os.path.join(part, "normalized", "events-0.market_event_v1.parquet"), "wb") as f:
            f.write(b"y" * 10)
        cfg["sha_por_version"] = {"fs1_normalizar/2.0": "otro_binario_distinto"}
        ok, motivo, _, _ = verificar_particion(cfg, "binance", dia, correr_gate=False)
        check("cambiar el binario sin subir la version da FAIL",
              (not ok) and motivo == "NORMALIZADOR_CAMBIO_SIN_SUBIR_VERSION", str(motivo))
        cfg["sha_por_version"] = {}

        # 9b. el caso real del 20260918: una particion de bybit hecha con 2.0 NO vale,
        #     porque esa version deja sin hora de evento el 37,51 % de sus filas.
        #     La misma version en binance SI vale: ahi 2.0 y 2.1 dan salida identica.
        ok_by, motivo_by, _, _ = verificar_particion(cfg, "bybit", dia, correr_gate=False)
        check("bybit con fs1_normalizar/2.0 se rechaza",
              (not ok_by) and motivo_by in ("CODE_VERSION_NO_ADMITIDA_PARA_ESTE_VENUE", "SIN_PARTICION"),
              str(motivo_by))
        ok_bi, _, _, _ = verificar_particion(cfg, "binance", dia, correr_gate=False)
        check("binance con fs1_normalizar/2.0 se sigue aceptando", ok_bi)

        # 10. adopcion: lo ya hecho no se repite
        est = plan(cfg, estado_nuevo(cfg), correr_gate=False)
        check("adopta la particion ya hecha como PASS", est["jobs"][jid]["estado"] == "PASS",
              est["jobs"][jid].get("motivo", ""))
        check("el recibo de la adopcion existe",
              os.path.isfile(os.path.join(cfg["orq_dir"], "recibos", "RECIBO_%s.json" % jid)))

        # 11. deteccion de procesos externos: por argv, no por texto.
        # Un comando que solo MENCIONA 'g1_cadena.sh okx' en su texto no es una cadena.
        # Confundir ambas cosas ya costo un susto (un guardia que se detecto a si mismo
        # y aborto el lanzamiento); aqui se prueba con un senuelo que no se repite.
        senuelo = subprocess.Popen(["/bin/bash", "-lc", "# g1_cadena.sh okx 2026-09-13\nsleep 3"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.4)
        ext = particiones_externas()
        check("un comando que solo menciona g1_cadena.sh no cuenta como cadena",
              senuelo.pid not in ext.values(), "pid %d detectado como cadena" % senuelo.pid)
        senuelo.terminate()
        senuelo.wait()

        # 11b. techo global de normalizadores: hay que ver tambien los que no son mios
        falso = os.path.join(raiz, "fs1_normalizar.py")
        with open(falso, "w") as f:
            f.write("import time\ntime.sleep(5)\n")
        antes = normalizadores_vivos()
        ajeno = subprocess.Popen([sys.executable, falso],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)
        check("cuenta un normalizador que no ha lanzado el orquestador",
              normalizadores_vivos() >= antes + 1, "antes=%d ahora=%d" % (antes, normalizadores_vivos()))
        ajeno.terminate()
        ajeno.wait()

        # 12. guardia de disco
        cabe, _, nec = cabe_en_disco(cfg, 10)
        check("una entrada minuscula cabe", cabe)
        cabe2, _, _ = cabe_en_disco(cfg, 10 ** 18)
        check("una entrada imposible no cabe", not cabe2)

        # 13. recibo sellado
        rec = escribir_recibo(cfg, "binance", dia, True, None, {"a": 1}, [])
        copia = {k: v for k, v in rec.items() if k != "recibo_sha256"}
        check("el recibo lleva sha256 propio verificable", rec["recibo_sha256"] == sha256_obj(copia))
        check("el recibo declara orders=0 y sin autoridad de ejecucion",
              rec["orders"] == 0 and rec["execution_authority"] == "NONE")

        # 14. candado global
        c1 = CandadoGlobal(os.path.join(cfg["orq_dir"], ".lock")).__enter__()
        try:
            CandadoGlobal(os.path.join(cfg["orq_dir"], ".lock")).__enter__()
            check("dos orquestadores a la vez estan prohibidos", False, "el segundo entro")
        except SystemExit:
            check("dos orquestadores a la vez estan prohibidos", True)
        c1.__exit__()
    finally:
        shutil.rmtree(raiz, ignore_errors=True)

    print("\n%s: %d fallos" % ("AUTOTEST_FAIL" if fallos else "AUTOTEST_PASS", len(fallos)))
    return 1 if fallos else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def cargar_config(ruta):
    cfg = dict(CONFIG_DEF)
    if ruta:
        cfg.update(leer_json(ruta, {}) or {})
    return cfg


def main():
    ap = argparse.ArgumentParser(description="JEAN_NORMALIZADOR_ORQUESTADOR_V1")
    ap.add_argument("accion", choices=["autotest", "preflight", "plan", "estado", "run", "verificar", "reintentar"])
    ap.add_argument("--config")
    ap.add_argument("--workers", type=int)
    ap.add_argument("--dias", help="lista separada por comas; por defecto, todos los que haya")
    ap.add_argument("--exchanges")
    ap.add_argument("--job")
    ap.add_argument("--todos-fail", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--una-vez", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sha-completo", action="store_true")
    ap.add_argument("--matar-estancados", action="store_true")
    a = ap.parse_args()

    if a.accion == "autotest":
        return autotest()

    cfg = cargar_config(a.config)
    if a.workers:
        cfg["workers"] = a.workers
    if a.dias:
        cfg["dias"] = [d.strip() for d in a.dias.split(",") if d.strip()]
    if a.exchanges:
        cfg["exchanges"] = [e.strip() for e in a.exchanges.split(",") if e.strip()]
    if a.sha_completo:
        cfg["sha_completo"] = True
    if a.matar_estancados:
        cfg["matar_estancados"] = True
    os.makedirs(cfg["orq_dir"], exist_ok=True)

    if a.accion == "preflight":
        problemas, avisos, datos = preflight(cfg)
        print(json.dumps({"problemas": problemas, "avisos": avisos, "datos": datos},
                         indent=1, sort_keys=True, ensure_ascii=False))
        return 1 if problemas else 0

    # 'estado' es solo lectura y NO toma el candado: consultar como va el trabajo tiene
    # que funcionar mientras el servicio corre, que es justo cuando interesa preguntarlo.
    if a.accion == "estado":
        informe(cfg, cargar_estado(cfg), a.json, escribir_recibo_global=False)
        return 0

    with CandadoGlobal(os.path.join(cfg["orq_dir"], ".lock_orquestador")):
        if a.accion == "plan":
            est = plan(cfg, cargar_estado(cfg))
            guardar_estado(cfg, est)
            informe(cfg, est, a.json)
            return 0
        if a.accion == "verificar":
            if not a.job:
                print("hace falta --job exchange_YYYY-MM-DD")
                return 2
            est = cargar_estado(cfg)
            j = est["jobs"].get(a.job)
            if not j:
                print("job desconocido: %s" % a.job)
                return 2
            ok, motivo, ev, alertas = verificar_particion(cfg, j["exchange"], j["dia"])
            escribir_recibo(cfg, j["exchange"], j["dia"], ok, motivo, ev, alertas)
            set_estado(cfg, est, a.job, "PASS" if ok else "FAIL", motivo, evidencia=ev, alertas=alertas)
            guardar_estado(cfg, est)
            print(json.dumps({"job": a.job, "ok": ok, "motivo": motivo, "evidencia": ev, "alertas": alertas},
                             indent=1, sort_keys=True, ensure_ascii=False))
            return 0 if ok else 1
        if a.accion == "reintentar":
            est = cargar_estado(cfg)
            objetivo = [a.job] if a.job else [k for k, v in est["jobs"].items() if v.get("estado") == "FAIL"]
            for jid in objetivo:
                if jid in est["jobs"]:
                    est["jobs"][jid]["intentos"] = 0
                    set_estado(cfg, est, jid, "NEW", "reintento pedido")
            guardar_estado(cfg, est)
            print("reencolados: %s" % (", ".join(objetivo) or "ninguno"))
            return 0
        if a.accion == "run":
            est = run(cfg, una_vez=a.una_vez, dry_run=a.dry_run)
            informe(cfg, est)
            return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
