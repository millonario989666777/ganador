# 04 · MEDIDAS

Todo lo medido, con su fuente. Nada estimado salvo donde diga **PROYECCIÓN**.

## Calidad del libro — binance_2026-09-13

| | 60 ficheros (2h33) | día completo (582 ficheros) |
|---|---|---|
| símbolos | 200 | 200 |
| filas de libro (rejilla 1 s) | 1.872.075 | 17.279.875 |
| **usables con fs2 1.0** | 93,8664 % | **94,624 %** |
| **usables con fs2 1.1** | **97,6850 %** | *corriendo* |

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
