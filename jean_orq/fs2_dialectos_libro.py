#!/usr/bin/env python3
"""fs2_dialectos_libro - la regla de continuidad y el tipo de foto de CADA venue.

Analogo a fs1_dialectos.py, pero para la reconstruccion del libro (FS-2 / G2).

POR QUE EXISTE
  fs2_libro/1.0 aplica UNA sola regla (la de binance: pu == u_anterior) y reconoce UNA
  sola foto (book_snapshot). Con bybit y okx eso no da error: da un libro con 0 por ciento
  de tiempo usable, en silencio. Es el mismo fallo que ya mordio tres veces en este
  proyecto (T062, T063, T064): el proceso vivo, sin errores, y los datos mal.

EVIDENCIA (medida el 20260918 en pc3-grabador, no heredada de nadie)
  Herramienta: medir_dialectos_libro.py sobre ficheros CONSECUTIVOS ya normalizados,
  deduplicando a nivel de mensaje (un mensaje de libro se expande en ~9 filas, una por
  nivel; comparar fila contra fila daba 10.38 por ciento en binance en vez de 100).

    venue     regla                 acierto      pares medidos   foto
    binance   pu == u_anterior      100.0000 %   1.385.216       book_snapshot
    bybit     u  == u_anterior + 1   99.5683 %   1.295.790       book_snapshot
    okx       pu == u_anterior      100.0000 %     788.307       book_deep_snapshot

  En bybit, pu y U son NULOS en los 12.346.264 deltas medidos: bybit no manda
  prev_update_id, de modo que la regla de binance no puede acertar nunca.
  En okx la regla es la MISMA que binance; lo unico que lo bloqueaba era que su foto se
  llama book_deep_snapshot y fs2_libro no la reconocia.

  Contrastado ademas con dos fuentes independientes:
   - Documentacion oficial de cada exchange.
   - cryptofeed (bmoscon/cryptofeed, commit ba6ba83d), que en produccion hace:
       okx.py:307   sequence, previous = update.get('seqId'), update.get('prevSeqId')
       bybit.py:409 sequence_number=data.get('u')
       binance.py   U <= last+1 <= u

NO CONECTA CON NINGUN EXCHANGE. No usa credenciales. No envia ordenes.
orders = 0 y execution_authority = NONE.
"""

VERSION = "fs2_dialectos_libro/1.0.0"

# Estados de la maquina de libro. Fail-closed: mientras no haya foto valida, NO se usa.
UNINITIALIZED = "UNINITIALIZED"   # aun no ha llegado ninguna foto
SYNCED = "SYNCED"                 # foto aplicada y deltas encadenando
DESYNCED = "DESYNCED"             # se rompio la cadena; no usable hasta la proxima foto

# Motivos por los que una fila del libro no es usable. Que un motivo exista es lo que
# permite distinguir "no habia libro" de "el libro estaba mal", que no es lo mismo.
NO_SNAPSHOT_YET = "NO_SNAPSHOT_YET"
GAP_AFTER_SNAPSHOT = "GAP_AFTER_SNAPSHOT"
STALE_BOOK = "STALE_BOOK"
EMPTY_SIDE = "EMPTY_SIDE"


class Dialecto(object):
    """Como encadena un venue sus actualizaciones de libro y como se llama su foto."""

    def __init__(self, exchange, nombre_regla, fotos, campo_secuencia, evidencia):
        self.exchange = exchange
        self.nombre_regla = nombre_regla
        self.fotos = frozenset(fotos)
        self.campo_secuencia = campo_secuencia
        self.evidencia = evidencia

    def es_foto(self, event_type):
        return event_type in self.fotos

    def secuencia_de(self, U, u, pu):
        """El id que queda como 'ultimo aplicado' tras este mensaje."""
        return u

    def continua(self, ultimo_u, U, u, pu):
        """True si este delta encadena con el ultimo aplicado. Ante la duda, False.

        Fail-closed a proposito: equivocarse diciendo que NO encadena cuesta un resync;
        equivocarse diciendo que SI encadena produce un libro incorrecto que ninguna
        comprobacion posterior detecta.
        """
        raise NotImplementedError

    def continua_tras_foto(self, ultimo_u, U, u, pu):
        """El PRIMER delta despues de una foto no encadena por igualdad.

        La foto se toma por REST en un instante cualquiera, y el stream sigue su propio
        ritmo: el delta que hay que aceptar es el que ABARCA ese instante, no el que
        empieza justo donde acabo la foto. Medido en este mismo proyecto: solo 265 de 403
        fotos encadenaban directo. Exigir igualdad aqui deja el libro desincronizado
        hasta la foto siguiente y hunde el tiempo valido sin que nada de error.
        """
        return self.continua(ultimo_u, U, u, pu)


class DialectoPu(Dialecto):
    """binance USDT-M y okx: el delta trae el id del anterior y tiene que cuadrar.

    Los ids NO tienen que subir de uno en uno: lo que manda es que el 'anterior'
    declarado por el mensaje sea exactamente el ultimo que aplicamos.
    """

    def continua(self, ultimo_u, U, u, pu):
        if ultimo_u is None or pu is None:
            return False
        return pu == ultimo_u

    def continua_tras_foto(self, ultimo_u, U, u, pu):
        if ultimo_u is None or u is None:
            return False
        # el delta que abarca el instante de la foto
        if pu is not None and pu <= ultimo_u < u:
            return True
        if U is not None and U <= ultimo_u + 1 <= u:
            return True
        return pu is not None and pu == ultimo_u


class DialectoIncremental(Dialecto):
    """bybit: no manda prev_update_id, encadena porque u sube exactamente de uno en uno."""

    def continua(self, ultimo_u, U, u, pu):
        if ultimo_u is None or u is None:
            return False
        return u == ultimo_u + 1

    def continua_tras_foto(self, ultimo_u, U, u, pu):
        """bybit pide la foto por REST y el stream sigue su propio ritmo.

        MEDIDO el 20260918 sobre 410 fotos de bybit_2026-09-13, saltos del primer delta
        respecto a la foto:  +1 56,3 %   +2 18,8 %   +3 10,7 %   +4 5,4 %   +5 2,7 %   >5 6,1 %
        Es decir, exigir u+1 rechaza el 43,7 por ciento de los arranques y desincroniza el
        libro tras casi la mitad de las fotos, sin que nada de error.

        Y a diferencia de binance u okx, bybit no manda U ni pu, asi que NO hay forma de
        comprobar si un delta abarca el instante de la foto. Se acepta el primer delta
        POSTERIOR a la foto bajo un SUPUESTO DECLARADO: que la foto contiene todo lo
        anterior a ella. Si de verdad se perdieron deltas en ese hueco, el libro arranca
        con ese error dentro. Por eso los arranques con salto se cuentan aparte
        (arranques_con_salto), para que la magnitud del supuesto sea auditable y no
        quede escondida en un porcentaje bonito.
        """
        if ultimo_u is None or u is None:
            return False
        return u > ultimo_u


DIALECTOS = {
    "binance": DialectoPu(
        "binance", "pu == u_anterior", {"book_snapshot"}, "book_update_id",
        "medido 20260918: 100.0000 % de 1.385.216 pares, 200 simbolos, 12 ficheros consecutivos"),
    "okx": DialectoPu(
        "okx", "pu == u_anterior", {"book_deep_snapshot", "book_snapshot"}, "book_update_id",
        "medido 20260918: 100.0000 % de 788.307 pares, 200 simbolos; foto = book_deep_snapshot"),
    "bybit": DialectoIncremental(
        "bybit", "u == u_anterior + 1", {"book_snapshot"}, "book_update_id",
        "medido 20260918: 99.5683 % de 1.295.790 pares; pu y U nulos en los 12.346.264 deltas"),
}


def dialecto_de(exchange):
    """Devuelve el dialecto del venue. Un venue desconocido NO cae en el de binance.

    Adivinar el dialecto es exactamente como se produce un libro plausible y falso.
    """
    d = DIALECTOS.get(exchange)
    if d is None:
        raise KeyError(
            "sin dialecto de libro para el venue '%s'. Anadirlo a DIALECTOS con su regla "
            "MEDIDA sobre datos reales; no reutilizar la de binance por defecto." % exchange)
    return d


class MaquinaLibro(object):
    """Maquina de estados de un simbolo. Fail-closed y con motivo siempre que no sea usable.

    Uso:
        m = MaquinaLibro("bybit")
        m.foto(u)                 -> al llegar una foto
        m.delta(U, u, pu)         -> por cada delta; devuelve True si se aplica
        m.usable, m.motivo, m.generacion
    """

    def __init__(self, exchange):
        self.dialecto = dialecto_de(exchange)
        self.estado = UNINITIALIZED
        self.ultimo_u = None
        self.motivo = NO_SNAPSHOT_YET
        self.generacion = 0      # sube con cada foto: permite distinguir tramos
        self.aplicados = 0
        self.rechazados = 0
        self.resyncs = 0
        self.previos_descartados = 0      # deltas anteriores a la foto: no son huecos
        self.arranques_con_salto = 0      # fotos cuyo primer delta no encadeno por +1
        self.esperando_primer_delta = False

    @property
    def usable(self):
        return self.estado == SYNCED

    def foto(self, u):
        """Una foto siempre resincroniza: es la unica forma de salir de DESYNCED."""
        if u is None:
            # una foto sin id no sirve para encadenar nada: no se acepta
            self.estado = DESYNCED
            self.motivo = GAP_AFTER_SNAPSHOT
            return False
        self.estado = SYNCED
        self.ultimo_u = u
        self.motivo = None
        self.generacion += 1
        self.resyncs += 1
        self.esperando_primer_delta = True
        return True

    def delta(self, U, u, pu):
        if self.estado == UNINITIALIZED:
            self.motivo = NO_SNAPSHOT_YET
            self.rechazados += 1
            return False
        if self.estado == DESYNCED:
            # Un delta NO repara un desync. Solo la proxima foto. Aplicarlo "a ver si cuela"
            # es lo que produce un libro que parece bueno y no lo es.
            self.motivo = GAP_AFTER_SNAPSHOT
            self.rechazados += 1
            return False

        # Un delta ANTERIOR a la foto no es un hueco: es cola del stream que ya venia en
        # camino. Se descarta sin romper nada, porque su contenido ya esta en la foto.
        if u is not None and self.ultimo_u is not None and u <= self.ultimo_u:
            self.previos_descartados += 1
            return False

        if self.esperando_primer_delta:
            if self.dialecto.continua_tras_foto(self.ultimo_u, U, u, pu):
                self.esperando_primer_delta = False
                if u is not None and self.ultimo_u is not None and u != self.ultimo_u + 1:
                    self.arranques_con_salto += 1
            else:
                self.estado = DESYNCED
                self.motivo = GAP_AFTER_SNAPSHOT
                self.rechazados += 1
                return False
        elif not self.dialecto.continua(self.ultimo_u, U, u, pu):
            self.estado = DESYNCED
            self.motivo = GAP_AFTER_SNAPSHOT
            self.rechazados += 1
            return False
        nuevo = self.dialecto.secuencia_de(U, u, pu)
        if nuevo is not None:
            self.ultimo_u = nuevo
        self.aplicados += 1
        return True

    def resumen(self):
        return {
            "exchange": self.dialecto.exchange,
            "regla": self.dialecto.nombre_regla,
            "estado": self.estado,
            "generacion": self.generacion,
            "aplicados": self.aplicados,
            "rechazados": self.rechazados,
            "resyncs": self.resyncs,
            "previos_descartados": self.previos_descartados,
            "arranques_con_salto": self.arranques_con_salto,
            "motivo": self.motivo,
        }


# ---------------------------------------------------------------------------
# Pruebas: sin datos reales, sin red
# ---------------------------------------------------------------------------
def autotest():
    fallos = []

    def check(nombre, cond, detalle=""):
        if cond:
            print("  PASS  %s" % nombre)
        else:
            print("  FAIL  %s %s" % (nombre, detalle))
            fallos.append(nombre)

    print("AUTOTEST %s" % VERSION)

    # 1. cada venue con su regla y su foto
    check("binance encadena por pu", DIALECTOS["binance"].continua(100, 101, 105, 100))
    check("binance rechaza si pu no cuadra", not DIALECTOS["binance"].continua(100, 101, 105, 99))
    check("okx encadena por pu igual que binance", DIALECTOS["okx"].continua(500, None, 700, 500))
    check("okx reconoce book_deep_snapshot como foto", DIALECTOS["okx"].es_foto("book_deep_snapshot"))
    check("binance NO reconoce book_deep_snapshot", not DIALECTOS["binance"].es_foto("book_deep_snapshot"))
    check("bybit encadena por u+1", DIALECTOS["bybit"].continua(41, None, 42, None))
    check("bybit rechaza un salto", not DIALECTOS["bybit"].continua(41, None, 44, None))

    # 2. el error que motiva todo esto: aplicar la regla de binance a bybit
    check("la regla de binance sobre datos de bybit (pu nulo) nunca encadena",
          not DIALECTOS["binance"].continua(41, None, 42, None))

    # 3. un venue desconocido NO hereda la regla de binance
    try:
        dialecto_de("kraken")
        check("un venue desconocido no cae en binance por defecto", False, "no lanzo")
    except KeyError:
        check("un venue desconocido no cae en binance por defecto", True)

    # 4. maquina fail-closed
    m = MaquinaLibro("bybit")
    check("sin foto no es usable", (not m.usable) and m.motivo == NO_SNAPSHOT_YET)
    check("un delta antes de la foto no se aplica", not m.delta(None, 10, None))
    m.foto(10)
    check("tras la foto es usable", m.usable and m.generacion == 1)
    check("un delta anterior a la foto se descarta sin romper nada",
          (not m.delta(None, 9, None)) and m.usable and m.previos_descartados == 1)
    check("el delta que encadena se aplica", m.delta(None, 11, None))
    check("un salto rompe la cadena", not m.delta(None, 13, None))
    check("tras el salto queda DESYNCED", m.estado == DESYNCED and m.motivo == GAP_AFTER_SNAPSHOT)
    check("un delta NO repara el desync", not m.delta(None, 14, None))
    check("sigue sin ser usable", not m.usable)
    m.foto(20)
    check("solo la foto resincroniza", m.usable and m.generacion == 2)
    check("y sigue la cuenta desde el nuevo id", m.delta(None, 21, None))

    # 4b. bybit: la foto viene por REST y el primer delta puede llegar con salto
    bb = MaquinaLibro("bybit")
    bb.foto(100)
    check("bybit acepta el primer delta con salto tras la foto", bb.delta(None, 104, None))
    check("y lo cuenta como arranque con salto, no lo esconde", bb.arranques_con_salto == 1)
    check("despues ya exige u+1 estricto", bb.delta(None, 105, None) and not bb.delta(None, 110, None))

    # 5. okx completo
    o = MaquinaLibro("okx")
    o.foto(1000)
    check("okx: delta con pu correcto se aplica", o.delta(None, 1500, 1000))
    check("okx: los ids NO tienen que subir de uno en uno", o.ultimo_u == 1500)
    check("okx: pu que no cuadra rompe", not o.delta(None, 1600, 1234))

    # 6. una foto sin id no vale como foto
    b = MaquinaLibro("binance")
    check("foto sin id no sincroniza", not b.foto(None))
    check("y deja el libro no usable", not b.usable)

    # 7. contadores. La cuenta exacta de la secuencia de arriba:
    #    delta(10) antes de la foto -> rechazado
    #    foto(10); delta(11) -> aplicado
    #    delta(13) salto -> rechazado (y DESYNCED)
    #    delta(14) -> rechazado (un delta no repara un desync)
    #    foto(20); delta(21) -> aplicado
    r = m.resumen()
    check("el resumen lleva regla y contadores",
          r["regla"] == "u == u_anterior + 1" and r["aplicados"] == 2
          and r["rechazados"] == 3 and r["resyncs"] == 2, str(r))

    print("\n%s: %d fallos" % ("AUTOTEST_FAIL" if fallos else "AUTOTEST_PASS", len(fallos)))
    return 1 if fallos else 0


if __name__ == "__main__":
    import sys
    sys.exit(autotest())
