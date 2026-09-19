# 09 · EL ORÁCULO EXTERNO

*Qué hay grabado en internet, gratis, que sirva para juzgar nuestra grabación desde fuera.*

Hasta ahora todo lo que sabíamos de nuestra captura salía **de nuestra propia captura**: la
cadena `pu == u_anterior` dice si perdimos un mensaje **entre dos que sí tenemos**, pero no
puede decir nada de un tramo en el que no tenemos ninguno. Un contador que se audita a sí
mismo tiene ese punto ciego por construcción.

Este plano es la salida de ese punto ciego: **fuentes externas, oficiales y gratuitas, del
mismo día que nosotros grabamos**, contra las que medir.

---

## 1. Binance publica su propio archivo, gratis

`https://data.binance.vision/` — ficheros estáticos, diarios, **sin credenciales, sin clave,
sin cuenta**. Comprobado desde PC3 el 19-sep-2026.

| conjunto | existe para 2026-09 | qué trae |
|---|---|---|
| `aggTrades` | sí, 09-01 … 09-18 | `agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker` |
| `trades` | sí, 09-01 … 09-18 | operaciones individuales |
| `bookDepth` | sí, 09-01 … 09-18 | profundidad a ±1 %…±5 % del medio, **cada minuto** |
| `bookTicker` | **no**, descatalogado | — |

Descargado a PC3 en `JEAN_FIVEDAY_CLAUDE/ORACULO_BINANCE/`:

```
293a2edf87230709  BTCUSDT-aggTrades-2026-09-13.zip   6.390.912 B
65dd38b023f519cf  BTCUSDT-bookDepth-2026-09-13.zip     558.324 B
631428d13b8af8ca  BTCUSDT-trades-2026-09-13.zip     10.510.466 B
428e26a494237bf3  BTCUSDT-bookTicker-2026-09-13.zip        365 B  (vacío: no existe)
```

**Corrección de método registrada:** la primera comparación que hice fue contra `trades`
(1.409.705 operaciones individuales) y dio un desfase enorme. Estaba mal la comparación, no
el dato: nosotros grabamos `aggTrade` (`exchanges.py:200` mapea `"aggTrade" → "trade"`), así
que el oráculo correcto es `aggTrades`, 512.737.

---

## 2. LA MEDIDA · cuánto perdimos de verdad, dicho por Binance

`ORACULO_BINANCE/cruzar_trades.py` (sha `5aca69e638ca5c7c`) lee el zip oficial, recorre los
**582 parquets normalizados** de `binance_2026-09-13` y cruza los identificadores uno a uno.
La numeración oficial de Binance es **contigua** — 0 huecos en su propia numeración — así que
un identificador que falta es un mensaje que **no nos llegó**, y sabemos exactamente cuál y
a qué hora.

```
Binance OFICIAL aggTrades ... 512.737   ids 3.448.030.365 → 3.448.543.101
                              huecos en su numeración: 0  (contigua)
NUESTROS .................... 511.952
FALTAN ...................... 785   (0,1531 %)
SOBRAN (no están en el oficial) ... 0
```

**Cobertura de operaciones: 99,8469 %.**

**`SOBRAN = 0` es tan importante como el 99,84 %:** en 511.952 operaciones no hay ni una
inventada, ni una duplicada, ni una con identificador que Binance no reconozca. Lo que
grabamos, es. Lo único que falla es lo que *no* grabamos.

### Dónde se perdieron — 4 cortes, no 785 pérdidas sueltas

Agrupando los identificadores que faltan en **rachas** (cada racha de números consecutivos =
un corte del feed):

| ids perdidos | desde | hasta | duración |
|---:|---|---|---:|
| 441 | 16:15:53 | 16:18:15 | **142,4 s** |
| 180 | 18:45:04 | 18:46:05 | **61,1 s** |
| 163 | 16:33:44 | 16:34:46 | **62,2 s** |
| 1 | 03:17:29 | 03:17:29 | 0,0 s |

```
tiempo total con el feed cortado: 265,8 s de 86.400 = 0,3076 %
```

**El feed estuvo vivo el 99,6924 % del día.**

### Lo que esto enseña del grabador

1. **No son pérdidas sueltas, son cortes.** 784 de los 785 identificadores caen en tres
   ventanas. El grabador no "gotea": se cae y vuelve.
2. **Dos de los tres cortes duran 61,1 s y 62,2 s.** Un corte de ~61 segundos repetido no es
   azar: huele a **temporizador fijo** — un ping/pong o un *read timeout* de 60 s que tarda
   ese minuto en detectar la caída y reconectar. Es la primera pista dura de *por qué* se
   cae, y es accionable sobre PC2.
3. **Los dos primeros cortes están a 18 minutos uno de otro** (16:15 y 16:33) y el tercero
   aparte (18:45). Sugiere un mal rato de red concreto, no un fallo uniforme.
4. **Una sola pérdida aislada en todo el día** (03:17:29, 1 mensaje). El resto del día, el
   socket no pierde nada.

> `orders = 0` · `execution_authority = NONE`

---

## 3. Bybit también publica, y de nuestros días

`https://public.bybit.com/trading/BTCUSDT/` lista ficheros diarios hasta
**BTCUSDT2026-09-18.csv.gz** — es decir, **cubre los cinco días que grabamos**. Da un segundo
oráculo externo, para bybit, sin credenciales.

`https://public.bybit.com/orderbook/BTCUSDT/` devolvió listado vacío desde PC3: **NO MEDIDO**
si existe bajo otra ruta.

---

## 4. Tardis.dev · un DÍA ENTERO del libro, crudo, gratis

Esto es lo que se buscaba: no un resumen, no una foto por minuto, sino **la cinta entera**.

`tardis.dev` es un proveedor comercial de datos de cripto. Publica **el día 1 de cada mes
gratis, sin clave de API**, para todos los exchanges que cubre. Comprobado desde PC3:

### 4.a. El producto normalizado (CSV) — y por qué no nos vale para verificar

```
https://datasets.tardis.dev/v1/binance-futures/incremental_book_L2/2026/09/01/BTCUSDT.csv.gz
   HTTP 200 · 748.374.645 B   (un día entero, un símbolo)
https://datasets.tardis.dev/v1/bybit/incremental_book_L2/2026/09/01/BTCUSDT.csv.gz
   HTTP 200 · 124.072.307 B
https://datasets.tardis.dev/v1/okex-swap/incremental_book_L2/2026/09/01/BTC-USDT-SWAP.csv.gz
   HTTP 200 · 253.423.064 B
```

Cabecera:

```
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
binance-futures,BTCUSDT,1788220800464000,1788220800991078,true,ask,78549.6,5.687
```

**No hay `u`. No hay `pu`. No hay `seqId`.** Un proveedor comercial, de pago, entrega su
producto normalizado **sin los números de cadena del exchange** — con lo cual, con nuestra
propia regla, *no se puede verificar*. Es exactamente el mismo defecto que encontramos en el
material de TOKIO (plano 04) y la misma tesis que sostiene `market-tape` (plano 08):

> **normalizado ≠ grabado.** En cuanto normalizas y tiras el número de secuencia, nadie
> —ni tú, ni el que te lo compra— puede volver a demostrar que no falta nada.

Dos cosas sí hacen bien, y coinciden con lo que arreglamos en fs2 1.1:
- guardan **los dos relojes**: `timestamp` (del exchange) y `local_timestamp` (de recepción);
- marcan la foto **en banda** con `is_snapshot`, alineada con el flujo, no pedida por REST.

### 4.b. El producto crudo — este sí sirve

```
https://api.tardis.dev/v1/data-feeds/binance-futures?from=...&to=...&filters=[{"channel":"depth","symbols":["btcusdt"]}]
   HTTP 200, sin clave
```

Y lo que devuelve es la cinta literal, con el reloj de recepción delante:

```
2026-09-01T00:00:00.0259231Z {"stream":"btcusdt@depth@0ms","data":{"e":"depthUpdate",
  "E":1788220800022,"T":1788220800021,"s":"BTCUSDT","ps":"BTCUSDT",
  "U":11440786972435,"u":11440786986594,"pu":11440786972390,"b":[...],"a":[...]}}
```

**`U`, `u` y `pu` están.** Esto sí se puede auditar con nuestra regla.

**Límite medido del acceso gratuito:** sin clave, el servidor entrega **60 segundos por
petición**, pidas el rango que pidas (comprobado: `to=01:00:00` devuelve
`00:00:00.025 → 00:00:59.985`, 2.239 líneas, 1.922.223 B). Se sortea barriendo los **1.440
minutos** del día: `from=T HH:MM:00 → to=T HH:MM:59.999`, un fichero por minuto.

**Detalle técnico que costó un intento:** `urllib` de Python recibe **HTTP 406** de ese
servidor pase las cabeceras que pase; `curl` con la misma URL devuelve 200. El bajador llama
a `curl` por `subprocess`.

**Primer intento fallido, anotado:** en la primera pasada encadené los `pu` **a través de los
saltos de una hora a otra** sin darme cuenta de que cada petición sólo traía el primer minuto
de cada hora. Salían ~13 roturas, una por frontera. **No eran roturas de Tardis: eran huecos
de mi propio muestreo.** El número se descartó. *(Fallo de método nº 7 — ver plano 05.)*

### Estado

```
EN CURSO · bajada de los 1.440 minutos de 2026-09-01, binance-futures BTCUSDT depth@0ms
           destino  PC3:/home/jean/PC3_DISCO/ORACULO_TARDIS/
           tamaño estimado ~2,9 GB crudos · ~3,2 s por minuto · ~77 min
PENDIENTE · auditar esa cinta con NUESTRA regla (pu == u_anterior), sin resetear en las
            fronteras de minuto, y publicar su exactitud junto a la nuestra
```

**NO MEDIDO todavía:** la exactitud de cadena de la grabación de Tardis. No se adelanta una
cifra.

---

## 5. Lo que cambia con esto

| antes | ahora |
|---|---|
| "creemos que la captura es buena porque la cadena encaja" | **99,8469 % de las operaciones, dicho por Binance** |
| "no sabemos si se cayó el grabador" | **4 cortes, 265,8 s, con hora de inicio y fin** |
| "no sabemos si perdemos o inventamos" | **inventamos 0. Sólo perdemos.** |
| "no hay con qué compararse" | **hay un día entero, crudo, con números de cadena, gratis** |

Y una consecuencia incómoda que conviene decir en voz alta: **el punto ciego sigue existiendo
para el libro.** El oráculo de Binance cubre las *operaciones*, no las *actualizaciones del
libro*. Para el libro, el único oráculo externo disponible es la cinta de Tardis — y sólo del
día 1 de cada mes, que **no es ninguno de los días que grabamos**. Sirve para saber *cuánto
pierde un profesional*, que es la vara de medir; no sirve para auditar nuestros días.
