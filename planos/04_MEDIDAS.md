# 04 · MEDIDAS

Todo lo medido, con su fuente. Nada estimado salvo donde diga **PROYECCIÓN**.

## Calidad del libro — binance_2026-09-13

| | 60 ficheros (2h33) | día completo (582 ficheros) |
|---|---|---|
| símbolos | 200 | 200 |
| filas de libro (rejilla 1 s) | 1.872.075 | 17.279.875 |
| **usables con fs2 1.0** | 93,8664 % | **94,624 %** |
| **usables con fs2 1.1** | **97,6850 %** | **98,537 %** |

Mensajes del día completo (fs2 1.0):

```
ok 63.524.852 | fotos 56.687 | invalido 4.672.704 | sin_foto 128.939
previo_a_foto 7.568 | hueco_tras_foto 2.416 | rotura 679
```

**Roturas de cadena: 679 sobre 63.524.852 deltas = 0,00107 %.** El stream de binance es
casi perfecto; lo que se perdía era el enganche de la foto.

Motivos de no usable, día completo:

```
GAP_AFTER_SNAPSHOT 709.534 | STALE_BOOK 135.038 | CHAIN_BREAK 57.011
NO_SNAPSHOT_YET 30.081 | EMPTY_SIDE 30.081
```

## Coste real de operar — binance_2026-09-13, día completo

Sobre **16.350.859 filas usables**, 200 símbolos con ≥1 h usable cada uno.

### Medio spread (puntos básicos, mediana por símbolo)

| p10 | p25 | **mediana** | p75 | p90 | máximo |
|---|---|---|---|---|---|
| 0,4385 | 0,7441 | **1,3656** | 2,4137 | 3,5249 | 6,1614 (BBUSDT) |

`tarifas.json` supone **1,0 pb** como `ASSUMED_DECLARED` para los tres venues.
Solo **74 de 200** símbolos están por debajo. Para el p90 el coste real es **3,5 veces**
lo supuesto.

Más baratos: `USDCUSDT` 0,005 · `BTCUSDT` 0,006 · `ETHUSDT` 0,020 · `ZECUSDT` 0,045
Más caros: `BBUSDT` 6,161 · `BEATUSDT` 5,900 · `ROBOUSDT` 5,764 · `ARKMUSDT` 4,885

### Fondo disponible en dólares (mediana por símbolo)

| profundidad | p10 | p25 | **mediana** | p75 | p90 |
|---|---|---|---|---|---|
| 1 nivel | 31 | 58 | **158** | 375 | 2.124 |
| 5 niveles | 449 | 1.196 | **4.495** | 13.330 | 44.414 |
| 20 niveles | 6.335 | 16.210 | **40.549** | 114.441 | 279.215 |

### Cuántos de los 200 símbolos absorben una orden

| tamaño | en 1 nivel | en 5 niveles | en 20 niveles |
|---|---|---|---|
| 500 USD | 40 | 176 | **200** |
| 1.000 USD | 26 | 153 | **200** |
| 5.000 USD | 13 | 95 | 184 |
| 10.000 USD | 8 | 67 | 163 |
| 25.000 USD | 5 | 30 | 128 |
| 100.000 USD | 3 | 12 | 55 |

Con cuenta declarada por debajo de 5.000 USD y órdenes de 500–1.000, **los 200 símbolos
tienen fondo dentro de 20 niveles**. La liquidez no es el muro.

**NO MEDIDO**: a qué precio medio se llena esa orden. `book_state` guarda la cantidad
acumulada por nivel pero **no el precio de cada nivel**. Para eso hay que añadir columnas
a `fs2_libro` y volver a pasar el día (2 h 37 min medidas).

`fee_version` **sigue sin subirse**.

## Velocidades medidas

| tarea | ritmo | día completo |
|---|---|---|
| G1 binance | 37,9 s/fichero | 5,9–8,4 h |
| G1 bybit | 59,3 s/fichero | — |
| G1 okx | 30,3 s/fichero | — |
| G2 binance | 19,3 s/fichero | **2 h 37 min**, 632 MB |

Los de G1 son con **3 normalizadores en paralelo** (techo de RAM: cada uno pica en
~4,3 GB, con 19 GB caben 3).

## Particiones cerradas

| partición | veredicto | versión | filas |
|---|---|---|---|
| `binance_2026-09-13` | PASS | 2.0 | 764.883.478 |
| `binance_2026-09-14` | PASS *(sha contradictorio, ver nota)* | 2.0 | 1.036.560.620 |
| `bybit_2026-09-14` | PASS | 2.1 | 1.009.323.659 |
| `bybit_2026-09-13` | FAIL `CODE_VERSION_NO_ADMITIDA` | 2.0 | — |
| `okx_2026-09-13` | FAIL `GATE_G1_FAIL` 10/12 | 2.1 | 775.328.795 |

## Material TOKIO (PC1)

57,66 GB, 12.094 parquet, **9 días** (11→19 sep), **15 símbolos**, ya normalizado.
Más `bitstamp-l3` (2.262), `deribit-snap` (1.632), `deribit-trades` (1.444).

Prueba de continuidad, por venue:

| venue | pares | `+1` exacto | salto ≥100 | ¿comprobable? |
|---|---|---|---|---|
| bybit | 25.870 | **100,00 %** | 0 | **SÍ** |
| okx | 26.242 | 6,14 % | 12,69 % | NO |
| binance | 30.412 | 0 | **99,98 %** | NO |

**binance y okx no sirven como respaldo del libro**: guardan el id actual pero no el
anterior (`pu` / `prevSeqId`). La columna `extra` de binance es de tipo `null`: no puede
contener nada. Sin eso, un hueco es indistinguible de un salto normal.

Tampoco hay fotos (`kind` solo vale 1 = nivel de libro, 2 = operación) ni payload crudo.

**Sí sirve** como segunda opinión para los 15 símbolos, para los días 11, 12, 18 y 19 que
no tenemos derivados, y bitstamp/deribit son mercados nuevos.

---

## Pérdida real de captura, medida desde fuera — binance_2026-09-13 BTCUSDT

Oráculo: archivo oficial de Binance, `data.binance.vision`, gratis y sin credenciales.
Detalle completo en [09_ORACULO_EXTERNO.md](09_ORACULO_EXTERNO.md).

| | |
|---|---:|
| aggTrades oficiales (numeración contigua, 0 huecos suyos) | **512.737** |
| aggTrades nuestros | 511.952 |
| faltan | **785 = 0,1531 %** |
| sobran / inventados / duplicados | **0** |
| cobertura de operaciones | **99,8469 %** |
| cortes del feed | **4** |
| tiempo con el feed cortado | **265,8 s de 86.400 = 0,3076 %** |
| tiempo con el feed vivo | **99,6924 %** |

Los cuatro tramos: 142,4 s (16:15:53), 62,2 s (16:33:44), 61,1 s (18:45:04) y una pérdida
suelta de 1 mensaje (03:17:29).

**CORREGIDO tras verificar (ver plano 09 §6):** no son cuatro cortes del feed. Midiendo el
ritmo del crudo contra un control del mismo tamaño, sólo el de 142 s es un apagón (libro al
0,1 %, **y sin reconexión**). Los de 61 y 62 s son **reconexiones** — aparece conexión nueva,
el libro sigue al 23 % y al 6 %, y lo que se pierde entero es la cinta de operaciones. El de
03:17:29 **no es un corte**: el feed va al 102 % y se pierde un solo mensaje.

Los 265,8 s valen para **la cinta de operaciones**. El **libro** sólo murió unos 142 s =
0,1644 % del día.

Y lo decisivo: los 785 identificadores **no están en el crudo** (`raw/`). No los perdió fs1
— no llegaron nunca.

## Pérdida real de captura — bybit_2026-09-14 BTCUSDT

Oráculo: `public.bybit.com/trading/`, gratis y sin credenciales.

| | |
|---|---:|
| operaciones oficiales | **2.232.223** |
| nuestras | 2.223.842 |
| faltan | **8.381 = 0,3755 %** |
| sobran | **0** |
| cobertura | **99,6245 %** |

Dos apagones reales: 66,0 s (14:10:40, libro al 1,2 %) y 127,0 s (05:32:45, libro al 0 %),
**ninguno con reconexión**. Más una pérdida suelta a las 23:59:59 que cae en el cambio de
día y **se declara dudosa, no confirmada**.

Esta es la primera cifra de pérdida que **no sale de nuestro propio contador**. La cadena
`pu == u_anterior` da 100,0000 % porque sólo puede juzgar los mensajes que sí tenemos; este
cruce mide justamente lo que a esa regla se le escapa: los tramos en los que no hay nada.

`orders = 0` · `execution_authority = NONE`


---

## fs2 1.1 · día completo terminado (19-sep 11:31Z)

`binance_2026-09-13`, 582 ficheros, 200 símbolos, rejilla 1 s. 2 h 47 min (10.034 s).

| | fs2 1.0 | fs2 1.1 | |
|---|---:|---:|---|
| filas de libro | 17.279.875 | 17.279.875 | = |
| **usables** | 16.350.859 | **17.027.144** | **+676.285** |
| **% usable** | 94,624 % | **98,537 %** | **+3,91 pt** |
| `GAP_AFTER_SNAPSHOT` | 709.534 | **31.019** | −95,6 % |
| `hueco_tras_foto` | 2.416 | **175** | −92,8 % |
| `invalido` | 4.672.704 | **380.297** | −91,9 % |
| `ok` | 63.524.852 | **67.819.499** | +4.294.647 |
| `fotos` aplicadas | 56.687 | 17.522 | |
| `foto_vieja_ignorada` | — | **39.165** | |
| `sin_foto` | 128.939 | 128.939 | = |
| `previo_a_foto` | 7.568 | 7.568 | = |
| `rotura` | 679 | **680** | **+1, sin explicar** |

**La cuenta cuadra exacta:** 17.522 + 39.165 = 56.687. Son las mismas fotos; ahora 39.165 se
descartan por viejas en lugar de tirar el libro al suelo.

Motivos de no usable con 1.1: `NO_SNAPSHOT_YET` 30.081 · `EMPTY_SIDE` 30.081 ·
`GAP_AFTER_SNAPSHOT` 31.019 · `STALE_BOOK` 135.038 · `CHAIN_BREAK` 57.017.

**ABIERTO:** `rotura` pasó de 679 a 680. Uno más. Es determinista, no es ruido. La hipótesis
es que con 1.1 hay un delta que antes se tragaba un reinicio de libro y ahora sí llega a
comprobarse — **pero no está demostrado y no se da por bueno**.

---

## El coste real, recalculado sobre el libro recuperado

Los mismos dos guiones, ahora sobre `book_state_v11` (17.027.144 segundos usables en vez de
16.350.859).

| medio spread (pb) | con 1.0 | **con 1.1** |
|---|---:|---:|
| p10 | 0,4385 | 0,4385 |
| p25 | — | 0,7434 |
| **mediana** | **1,3656** | **1,391** |
| p75 | 2,4137 | 2,4149 |
| p90 | 3,5249 | 3,5273 |
| máx | 6,1614 | 6,1614 |
| **símbolos por debajo de 1 pb** | **74** | **73** |

### Lo incómodo: recuperar datos hizo el coste PEOR, no mejor

Los 676.285 segundos que antes se tiraban son justo los de **después de una foto o una
reconexión**, que es cuando el libro está más fino y el spread más ancho. Al recuperarlos,
la mediana sube de 1,3656 a 1,391 pb y un símbolo más se cae de la lista de "baratos".

> **El número viejo era optimista por supervivencia.** No porque estuviera mal calculado,
> sino porque los peores momentos del día no llegaban a la cuenta: se descartaban como no
> usables. Es exactamente el sesgo que convierte un backtest bonito en una pérdida real.

Dirección confirmada; la magnitud exacta del sesgo **NO MEDIDA** (haría falta marcar qué
segundos son los recuperados y medirlos aparte).

### Fondo por tamaño de orden (sin cambios materiales)

| | L1 | L5 | L20 |
|---|---:|---:|---:|
| mediana USD | 158 | 4.502 | 40.549 |
| p10 | 32 | 449 | 6.332 |
| p90 | 2.123 | 44.364 | 279.142 |

**Cuántos de los 200 símbolos aguantan una orden, dentro de 20 niveles:**

| orden | en 1 nivel | en 5 niveles | **en 20 niveles** |
|---|---:|---:|---:|
| 500 USD | 40 | 176 | **200** |
| 1.000 USD | 26 | 153 | **200** |
| 5.000 USD | 13 | 95 | 184 |
| 10.000 USD | 8 | 67 | 163 |
| 25.000 USD | 5 | 30 | 128 |
| 100.000 USD | 3 | 12 | 55 |

Con 500–1.000 USD **caben los 200**. A partir de 5.000 USD empieza a caerse la cola.

`orders = 0` · `execution_authority = NONE`
