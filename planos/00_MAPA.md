# 00 · MAPA

## Las tres máquinas

| | qué es | qué corre | disco |
|---|---|---|---|
| **PC1** | Windows (`AS40569324`) | memoria JEAN (SQLite, `jean_memory.py`), contexto en MD, material TOKIO | `D:\DATOS TOKIO REAL1\PARQUET` 57,66 GB |
| **PC2** | `pc2-grabador` | **el grabador** `jean200 capture`, 24/7, nunca ha parado | `/home/jean/JEAN_200_DATOS_TRES_EXCHANGES/events` |
| **PC3** | `pc3-grabador`, 192.168.100.4 | derivación G1/G2, orquestador, gates | 468 GB, ~185 GB libres |

**PC3 no alcanza a PC1 por red** (100 % de pérdida a 192.168.100.2). Responden .1 y .3.
No hay rclone ni montajes. Para cruzar datos entre las dos hay que pasar por el puente.

## El flujo

```
    exchanges  ──ws──►  PC2: jean200 capture        CRUDO, payload íntegro
                             (nunca para)                │
                                                         ▼
                        PC3: G0  inventario y sello ──► manifiesto
                                                         │
                             G1  fs1_normalizar     ──► market_event_v1
                                 + fs1_dialectos         (una fila por nivel)
                                                         │
                             gate de 12 checks      ──► PASS / FAIL
                                                         │
                             G2  fs2_libro          ──► book_state_v1
                                 reconstruye el libro     (rejilla de 1 s)
                                                         │
                             G3  features + etiquetas  ──► NO SE HA CORRIDO NUNCA
                             G4  dataset congelado     ──► NO SE HA CORRIDO NUNCA
```

Cada puerta exige la anterior en PASS. No se salta ninguna.

## Quién manda sobre qué en PC3

- `jean-normalizador-orquestador.service` → `jean_orq.py run --workers 2`.
  Gobierna `g1_particion.py`, que a su vez lanza `fs1_normalizar.py`. **No es un
  normalizador nuevo**: hay uno solo, y esto lo gobierna.
- Hay además **cadenas lanzadas a mano** desde sesiones de login (`session-NNNN.scope`).
  Esas no son del orquestador: las marca `EXTERNO` y no las toca. **Si se cierra la
  sesión que las lanzó, mueren.** Es el fallo que costó 4 h en PC2 el 18-sep.

## Los ficheros que importan

```
PC3 /home/jean/JEAN_FEATURE_STORE/bin/
      fs1_normalizar.py      el normalizador. Una sola versión viva.
      fs1_dialectos.py       QUÉ se normaliza y QUÉ se tira, por venue.
      fs2_libro.py           reconstruye el libro. Produce book_state_v1.
PC3 /home/jean/JEAN_FIVEDAY_CLAUDE/
      g1_particion.py        lanza el normalizador y sella la MEDICION
      g1_gate_check.py       los 12 checks de G1
PC3 /home/jean/JEAN_ORQUESTADOR/
      jean_orq.py            el orquestador
      estado.json            estado persistente y pines de sha
      recibos/               un recibo sellado por partición
PC2 /home/jean/jean-grabador-200/jean200/
      capture.py             el bucle de captura y las reconexiones
      exchanges.py           la traducción de cada venue a `kind`
```

## Cobertura del corte

Origen en PC3: **10.509 ficheros parquet**, 5 días (13→17 sep), 3 venues, 200 símbolos.

| venue | 13 | 14 | 15 | 16 | 17 |
|---|---|---|---|---|---|
| binance | 582 | 669 | 675 | 687 | 665 |
| bybit | 393 | 503 | 519 | 523 | 485 |
| okx | 864 | 986 | 1003 | 1010 | 945 |

Los días 07→12 **no están en PC3**. Y los 11 días completos no caben: ~770 GB en un disco
de 468 GB. Eso es una decisión, no un cálculo.
