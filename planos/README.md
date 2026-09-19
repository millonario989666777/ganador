# PLANOS

Los planos del sistema JEAN 5D, tal como está el **19 de septiembre de 2026**.

Aquí no hay opiniones ni planes: hay lo que se ha medido, con el número, la fuente y la
fecha. Lo que no se ha medido está marcado como **NO MEDIDO** y no se rellena con una
cifra plausible.

| plano | de qué va |
|---|---|
| [00_MAPA.md](00_MAPA.md) | las tres máquinas, qué corre en cada una, cómo fluye el dato |
| [01_DIALECTOS.md](01_DIALECTOS.md) | cómo encadena el libro cada exchange, medido |
| [02_FOTOS.md](02_FOTOS.md) | las fotos del libro: en banda, REST, y por qué fallaban |
| [03_VERSIONES.md](03_VERSIONES.md) | fs1, fs2, qué arregla cada versión, y la cadena de evidencia |
| [04_MEDIDAS.md](04_MEDIDAS.md) | todos los números medidos, en un sitio |
| [05_FALLOS.md](05_FALLOS.md) | los fallos encontrados, con su prueba y su estado |
| [06_PENDIENTE.md](06_PENDIENTE.md) | lo que falta, por orden |
| [07_RELOJ_Y_REDUNDANCIA.md](07_RELOJ_Y_REDUNDANCIA.md) | los tres relojes, y lo que el mercado institucional tiene y cripto no |
| [08_ESTADO_DEL_ARTE.md](08_ESTADO_DEL_ARTE.md) | quién más ha resuelto esto, qué confirma lo nuestro y qué nos falta |

## La regla que lo gobierna todo

> Un número sin procedencia no vale. Cada afirmación sobre los datos sale de un manifiesto
> sellado. Si algo no se puede medir, se declara como no disponible con su motivo — nunca
> se rellena con un valor plausible.

## Límite permanente

No se opera. No se conectan exchanges. No se usan credenciales. No se envían órdenes.
Todos los manifiestos llevan `orders = 0` y `execution_authority = NONE`.
