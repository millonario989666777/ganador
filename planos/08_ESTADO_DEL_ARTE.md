# 08 · ESTADO DEL ARTE: ¿existe ya esto en cripto?

Buscado el 19-sep-2026. La respuesta corta: **sí existe, pero no es una norma — es un
consenso disperso y reciente.** No hay un ITCH ni un MDP 3.0 para cripto. Cada proyecto
vuelve a deducir las reglas de cada venue por su cuenta.

Lo importante: **proyectos independientes han llegado a las mismas conclusiones que
nosotros.** Eso es confirmación cruzada, no coincidencia.

## market-tape (Bybit + Binance)

Se describe a sí mismo como *"a market data recorder that accounts for what it missed"*.
Su tesis, palabra por palabra:

> *"The rest of the line is the venue's own frame, unchanged: an interpretation can be
> recomputed from a faithful recording and cannot be recovered from a lossy one."*

Es literalmente el argumento de por qué no se graba normalizado. Guarda el frame nativo
del exchange más un `lt` (local timestamp), en JSON por líneas, gzip, particionado por día
UTC.

Sus reglas de continuidad:

| canal | regla | ¿nosotros? |
|---|---|---|
| bybit `orderbook.50` | `u == previous + 1` | **la misma**, medida al 100,0000 % |
| binance `@depth@100ms` | `pu == previous u` | **la misma**, medida al 100,0000 % |
| binance `@trade` | `t == previous + 1` | no lo usábamos |
| bybit `publicTrade` | **no se puede verificar** | **no lo sabíamos** |

Lo de bybit trades es conocimiento nuevo y útil: su `seq` *"numbers matching events rather
than individual trades"*, así que no sirve para detectar pérdida.

Publica un contador, `tape_missing_total{venue,symbol,channel}` = números de secuencia que
nunca llegaron. Y un CLI, `tape verify`, que **re-deriva la completitud desde los ficheros
solos**, sin fiarse de lo que el proceso vivo creyó.

**Diferencia con nosotros: no guarda fotos.** Sin foto no hay libro absoluto, solo cambios
relativos. Nosotros sí las guardamos.

## k2-market-data-platform (Rust · Binance, Kraken, Coinbase)

Archivo crudo `raw.messages · verbatim · forever`. Sella `recv_ts_ns` **antes de parsear**,
igual que nosotros con `receive_utc_ns`.

Sí captura fotos: *"Top-20 snapshots at 1 Hz; per-venue sequencing and resync."*

Resultados publicados sobre una ventana de 6,5 h:

```
Binance y Kraken   0 huecos de secuencia en 14,1 M de mensajes
Kraken CRC32       validado en cada update, 0 fallos
```

Auditoría nocturna por capa y paridad OHLCV a tres bandas.

## cryptofeed

Graba a **pcap con compresión zstd y metadatos en `.meta.json`** — el estándar de archivo
institucional, aplicado a cripto. Y cae a REST automáticamente si el websocket falla.

## Qué confirma esto y qué nos falta

**Confirma**: nuestras dos reglas principales (bybit `+1`, binance `pu`) las han deducido
otros por separado. Sumado a la documentación de los exchanges y a nuestras propias
medidas, son tres fuentes independientes.

**Nos falta de ellos:**

1. **Un contador publicado de pérdida.** Sabemos detectar huecos, pero no publicamos un
   `missing_total` por venue/símbolo/canal. Deberíamos.
2. **Una verificación que re-derive la completitud desde los ficheros solos**, sin fiarse
   de lo que el proceso vivo anotó. Nuestro gate se acerca, pero parte del manifiesto.
3. **La regla de trades**: `t == previous + 1` en binance. No la usábamos.
4. **El aviso de bybit trades**: no se pueden verificar. Hay que dejarlo declarado, no
   asumir que sí.

**Tenemos y ellos no:**

- market-tape no guarda fotos; nosotros sí, y sin ellas no hay libro absoluto.
- Nuestra cadena de evidencia (manifiesto sellado + cuarentena con puntero al crudo + gate
  de 12 checks + recibo con sha propio) es más estricta que lo que describen.

## Lo que NO existe en cripto

- Ninguna especificación tipo ITCH/MDP 3.0 que diga "así se graba".
- Ningún feed A/B, canal de recuperación ni retransmisión (ver plano 07).
- Ninguna certificación ni referencia contra la que validar.

Por eso hemos tenido que medir las tres reglas nosotros. No era desconfianza: **no había
dónde mirarlo.**

## Fuentes

- market-tape — github.com/sanzhar-zh/market-tape
- k2-market-data-platform — github.com/rjdscott/k2-market-data-platform
- cryptofeed — github.com/bmoscon/cryptofeed
- Tardis.dev — docs.tardis.dev (mensaje nativo + localTimestamp de 100 ns)
- Y para el lado institucional, el plano 07.
