# 03 · VERSIONES Y CADENA DE EVIDENCIA

## fs1_normalizar

| versión | qué arregla | sha256 (16) |
|---|---|---|
| **2.0** | base | `3a80bc4cc697d251` |
| **2.1** | hora de evento por venue | `ba47288146f927b0` |
| **2.2** | latidos de bybit y okx + foto en banda de okx | *(al aplicarse)* |

### 2.1 — la hora de evento

`fs1 2.0` recuperaba la hora solo con las claves de binance. Resultado: **37,51 %** de las
filas de bybit y **22,1 %** de las de okx sin hora de evento, con el dato **dentro del
payload**.

Sitio donde lo pone cada uno:

```
binance   response.T / response.E
bybit     response.result.ts
okx       response.data[0].ts     ← es una CADENA, hay que convertirla
```

Medido tras el arreglo en `okx_2026-09-13`: `MISSING_EVENT_TS` **4.674 de 775.328.795 =
0,0006 %**.

Para binance, 2.0 y 2.1 producen salida **idéntica** (medido: 1959 = 1959 recuperaciones
sobre los mismos ficheros). Por eso sus particiones ya hechas siguen valiendo.

### 2.2 — tres pérdidas

| | al día | dónde estaba |
|---|---|---|
| bybit: el latido iba a cuarentena | 196.647 | `fs1_dialectos.py:65` |
| okx: la foto en banda caía en `UNKNOWN_EVENT_TYPE` | 606 | lector de okx |
| okx: el latido salía como `EMPTY_RESULT` | 105 | lector de okx |

binance no se toca.

## fs2_libro

| versión | qué arregla | sha256 (16) |
|---|---|---|
| **1.0** | base | `12759c23023d2a58` |
| **1.1** | una foto vieja no retrocede un libro sano | `7d252fb9dfc1d80b` |

Resultado medido sobre los **mismos 60 ficheros**:

| | 1.0 | 1.1 |
|---|---|---|
| `pct_usable` | 93,8664 % | **97,6850 %** |
| `GAP_AFTER_SNAPSHOT` | 75.774 | **4.266** |
| `hueco_tras_foto` | 254 | **14** |
| `invalido` | 484.523 | **25.750** |
| coste | 1.156 s | 1.196 s (+3,5 %) |

Lo que **no** cambió, y no tenía que cambiar: `rotura` 18, `previo_a_foto` 701,
`sin_foto` 128.939, `STALE_BOOK` 8.860, `CHAIN_BREAK` 134. Idénticos.

## Contrato de versión POR VENUE

No es capricho. Cada venue admite lo que produce datos correctos **para él**:

```python
"code_versions_ok": {
    "binance": ["fs1_normalizar/2.0", "fs1_normalizar/2.1", "fs1_normalizar/2.2"],
    "bybit":   ["fs1_normalizar/2.2"],
    "okx":     ["fs1_normalizar/2.2"],
}
```

## La cadena de evidencia

Cada partición sella:

```
MANIFEST_NORMALIZED.json    code_version, schema_version, sha de cada salida, totales
MEDICION_NORMALIZED.json    sha del normalizador, sha de dialectos, tiempos, memoria
cuarentena/*.jsonl          lo descartado, con código y puntero al crudo (artifact_id + row)
G1_GATE_*.json              los 12 checks
recibos/RECIBO_*.json       veredicto del orquestador, con sha propio
```

### Regla del sha

Dos versiones distintas son legítimas si están declaradas. **Dos binarios distintos bajo
la misma versión, nunca.**

Desde el 19-sep, `g1_particion` sella el sha **al ARRANCAR**, no al terminar, y sella
también `fs1_dialectos.py`. Además deja escrito
`normalizador_cambiado_durante_la_corrida`.

**Por qué**: `binance_2026-09-14` corrió de `20260918T160508Z` a `20260919T002950Z` con el
código 2.0 en memoria mientras el fichero se cambiaba a 2.1 a las `22:09:25Z`. Selló
`code_version 2.0` con el sha del binario **2.1**. El recibo pasó en PASS y el check
`NORMALIZADOR_CAMBIO_SIN_SUBIR_VERSION` **no saltó**.

La MEDICION no se ha reescrito — una evidencia sellada no se toca. Lleva al lado una
`NOTA_SHA_CONTRADICTORIO.md` con la prueba, y el pin de `estado.json` se corrigió a
`3a80bc4c...`.
