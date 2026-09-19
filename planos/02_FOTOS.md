# 02 · LAS FOTOS DEL LIBRO

Los cambios (`deltas`) reconstruyen el libro exactamente, pero hace falta **una foto**
para saber desde dónde se empieza, y otra cada vez que la cadena se rompe.

Hay dos clases de foto, y no valen lo mismo.

## Foto EN BANDA — la buena

Llega **por el mismo socket que los deltas**, en el mismo orden. No hay que pegarla a
nada: ya viene pegada. Es gratis (no gasta llamada REST ni cupo) y no puede desalinearse.

Ejemplo real de okx, `MEGA-USDT-SWAP`:

```json
{"arg":{"channel":"books","instId":"MEGA-USDT-SWAP"}, "action":"snapshot",
 "data":[{"bids":[346 niveles],"asks":[400 niveles],
          "ts":1789261477207,"checksum":0,
          "seqId":729624411,"prevSeqId":-1}]}
```

`prevSeqId = -1` es como okx marca una foto.

## Foto REST — la que llega vieja

Se pide por otro canal. Su contenido es de un instante T0, pero se graba en T0 + lo que
tarde. En ese hueco el stream avanza.

**Medido** sobre `binance_2026-09-13`, 6.134 fotos, desfase entre el primer delta
posterior y el `lastUpdateId` de la foto:

```
mediana +1.762   p10 +405   p90 +8.122   min +4   max +293.042
```

**Todos positivos.** Cuando la foto se graba, el libro que ya teníamos va por delante.

## Quién manda qué

| venue | foto en banda | ¿la guardábamos? | foto REST |
|---|---|---|---|
| binance | **no existe** (el stream `depth@100ms` no la lleva, por protocolo) | — | obligatoria |
| bybit | la documentación dice que sí, tras cada suscripción | **0 en los 5 días** ← sin resolver | 800–1.077/día |
| okx | sí, `action=snapshot` | **se tiraban a cuarentena** ← arreglado en fs1 2.2 | `book_deep_snapshot` |

### bybit: cero fotos en banda, los cinco días

| día | deltas | foto en banda |
|---|---|---|
| 09-13 | 835.441 | 0 |
| 09-14 | 755.764 | 0 |
| 09-15 | 725.633 | 0 |
| 09-16 | 733.877 | 0 |
| 09-17 | 751.908 | 0 |

Comprobado además a lo bruto: en el fichero que contiene un `connection_start` hay 8.797
mensajes con el texto `"type":"snapshot"`, pero al abrirlos son **trades (8.782), tickers
(10) y liquidaciones (5)**. Del libro, ninguna. Y en los tres ficheros que rodean esa
reconexión, 332.006 deltas seguidos sin una sola foto.

El grabador pasa el payload tal cual (`exchanges.py:209` etiqueta todo `orderbook.full`
como `book_delta`), así que **si bybit la mandara, la tendríamos**. Falta leer
`capture.py` líneas 120–190, la parte de suscripción y reconexión. **NO HECHO.**

### okx: 606 fotos al día a la basura

El grabador etiqueta `action=="snapshot"` como kind `book_snapshot`
(`exchanges.py:216`). El lector de okx en `fs1_dialectos.py` solo conocía `book_delta` y
`book_deep_snapshot`: la foto caía en `UNKNOWN_EVENT_TYPE` y se descartaba.

Por eso `okx_2026-09-13` dio **GATE_G1 FAIL 10/12**.

Detalle que lo remata: la rama de `book_delta` de okx **ya tenía** la línea
`accion = "SNAPSHOT" if d.get("action") == "snapshot"`. Estaba escrita esperando la foto
por ahí. Nunca llega por ahí. Era código muerto desde el primer día.

Arreglado en **fs1 2.2**.

## El fallo que costaba el 4,1 % del tiempo

`fs2_libro` 1.0 aplicaba **toda** foto en el sitio donde se grabó, vaciando el libro y
poniéndose a esperar el delta que cubriera `lastUpdateId`. Como la foto REST llega vieja,
ese delta ya había pasado: el siguiente llegaba descolgado y el símbolo se quedaba ciego
**hasta la foto siguiente, unos 5 minutos después**.

```
fotos procesadas ................. 56.687
fotos que NO engancharon .......... 2.416   (4,3 %)
filas GAP_AFTER_SNAPSHOT ......... 709.534  (4,1 % del total)
709.534 / 2.416 = 294 s = 4,9 min de castigo por fallo
cadencia real de foto: 56.687 / 200 símbolos = una cada 5,1 min
```

Los 4,9 minutos de castigo coinciden con los 5,1 de cadencia. Cuadra al milímetro.

Y de esos 2.416, el **96,9 %** no hacían falta: el delta que engancha la foto **estaba en
los datos**, unas líneas más arriba. Solo **8 de 6.134** eran pérdida real.

### La regla nueva (fs2_libro 1.1)

> Si el libro vale y la foto no adelanta nada, se ignora y se cuenta.
> No es descartar información: es **no retroceder**.

Si la foto sí adelanta (`u > last_u`) o el libro está roto, se aplica como siempre.

**Esto solo se puede hacer en diferido.** Un sistema en vivo, cuando le llega la foto
vieja, ya ha aplicado y tirado los deltas que la cubrían. Nosotros tenemos el fichero
entero.
