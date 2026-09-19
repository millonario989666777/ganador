# 01 · DIALECTOS DEL LIBRO

Los tres exchanges mandan los mismos hechos con formas distintas. **Aplicar la regla de
binance a los otros dos no da error: da 0 % de tiempo válido y ni una queja.**

## La regla de cada uno, medida

| venue | regla de continuidad | exactitud | pares medidos |
|---|---|---|---|
| **binance** | `pu == u_anterior` | **100,0000 %** | 1.385.216 |
| **bybit** | `u == u_anterior + 1` | **100,0000 %** | 320.467 (con latidos) |
| **okx** | `prevSeqId == seqId anterior` | **100,0000 %** | 788.307 |

Los tres son exactos. Cuando hemos visto imperfección, era nuestra.

### binance: por qué hace falta `pu`

El `u` de futuros de binance es un contador **global del exchange**, no por símbolo. Vale
del orden de **11,5 billones** (`u = 11.542.169.409.882` medido el 13-sep a las 00:00Z) y
dos mensajes seguidos del mismo símbolo saltan cientos o miles.

Lo que une un mensaje con el anterior es `pu`. Sin `pu`, **un hueco real es
indistinguible de un salto normal**. Por eso un dataset que no guarde `pu` no permite
demostrar que el libro de binance sea correcto.

### bybit: el latido

`u` avanza de uno en uno. Un `book_delta` puede llegar con `b: []` y `a: []` pero con `u`
y `seq` nuevos: es un **latido**, y **gasta un número de la cadena**.

```
con los latidos dentro   320.467 encadenan   0 rompen     100,0000 %
sin ellos                317.836 encadenan   1.175 rompen  99,6317 %
```

Medido sobre 3 ficheros de `bybit_2026-09-13`, 320.657 `book_delta` de los que 1.456
venían vacíos (0,45 %).

### okx: el checksum está muerto

okx **deprecó el campo `checksum` el 23-jun-2026**: sigue en el mensaje pero vale 0.
Confirmado en nuestros datos: **136.286 de 136.286** mensajes de libro con `checksum = 0`.

okx recomienda validar con `seqId`/`prevSeqId`, que es exactamente lo que ya hacemos.
Ahí no hay nada que cambiar.

## Deltas vacíos, por venue

| venue | `book_delta` medidos | vacíos |
|---|---|---|
| binance | 236.214 | **0** |
| okx | 136.286 | 0 en la muestra; 105/día en el día completo |
| bybit | 320.657 | 1.456 (0,45 %) |

## El módulo

`fs2_dialectos_libro.py` implementa los tres dialectos con una máquina de estado que
falla cerrada, y `dialecto_de()` **levanta `KeyError` para un venue desconocido en vez de
caer en binance por defecto**. 30 pruebas.

Todavía **NO está integrado** en `fs2_libro.py`, que lleva la regla de `pu` a mano. Para
binance y okx funciona porque ambos usan `pu`/`prevSeqId`. **Para bybit no**: bybit no
manda `pu`, así que `fs2_libro` tal cual le daría casi 0 % de libro válido.
