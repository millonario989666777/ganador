# 07 · EL RELOJ Y LA REDUNDANCIA

Lo que el mercado institucional tiene y cripto no, y qué de eso podemos construir nosotros.

## Los tres relojes. No son lo mismo

Se confunden constantemente, así que aquí quedan separados:

| | qué es | quién lo pone | para qué sirve |
|---|---|---|---|
| **Reloj de pared** (`receive_utc_ns`) | la hora que es | nuestra máquina, ajustada por NTP | comparar con otras máquinas |
| **Reloj monótono** (`receive_mono_ns`) | un cronómetro que nunca va hacia atrás | nuestra máquina | medir **cuánto** tiempo pasó |
| **Número de secuencia** (`u`, `seqId`) | el número de página del mensaje | **el exchange** | saber si **falta** una página |

Como el correo: el cronómetro dice cuánto tardó el cartero, el reloj de pared a qué hora
llegó, y el número de página si falta una carta. Ninguno sustituye a los otros.

Los tres se graban. El número de página es el que sostiene las tres reglas de continuidad
del plano 01, las tres al 100,0000 %.

## El reloj de PC2, medido (19-sep 09:37Z)

```
System clock synchronized: yes      NTP service: active
servidor    ntp.ubuntu.com  (Stratum 2)
Jitter      1,653 ms
RootDelay   6,469 ms      RootDispersion  610 us
consulta    cada 34 min 8 s   (systemd-timesyncd, su maximo)
```

**Disciplinado a nivel de milisegundos, no de microsegundos.**

| uso | ¿llega? |
|---|---|
| rejilla de libro de 1 s | **sí, de sobra** |
| latencia contra el exchange (50–200 ms) | sí, error del 1–3 % |
| ordenar eventos **entre venues** por debajo del milisegundo | **no** |

Referencia: Tardis marca a 100 ns con reloj disciplinado. Nosotros a ~1,6 ms. Mil veces
peor. No es crítico hoy, pero queda medido.

Nota: el material TOKIO se graba en un VM de Google Cloud, cuyo reloj está disciplinado
por la propia infraestructura de Google y es mejor que esto. **No le sirve de nada**: tira
`pu`, así que su libro de binance no se puede verificar. Buen reloj con contenido
incompleto sigue siendo inservible.

## Lo que el mercado institucional tiene y cripto no

CME, Nasdaq y Eurex publican con tres niveles de recuperación en cascada:

### 1 · Feed A/B redundante
El exchange manda **cada paquete por dos canales**. El receptor arbitra: lo que falta en A
lo rellena con B. Repara la mayor parte de la pérdida y no cuesta nada al receptor.

### 2 · Canal de recuperación
Una **segunda emisora** que, en bucle, va publicando el libro entero una y otra vez. Si
pierdes el hilo, sintonizas, esperas la siguiente lectura completa, y estás al día. **No
le pides nada a nadie.**

En cripto: bybit y okx te dan **una** lectura al suscribirte, y se acabó. binance ni eso —
hay que llamar por REST.

### 3 · Retransmisión
Pides "repíteme el mensaje 6" y te lo repiten.

En cripto: **no existe**. Nadie coge el teléfono. Si se pierde un mensaje, se perdió. La
única salida es una foto nueva.

### Y el archivo
El estándar institucional guarda **PCAP crudo**: el paquete tal como llegó, para poder
reproducir hasta los límites de paquete y la propia arbitración A/B.

## Qué podemos construir nosotros

| | quién lo paga | podemos? | coste real |
|---|---|---|---|
| Canal de recuperación | el exchange | **no**, no se vende en cripto | — |
| Retransmisión | el exchange | **no** | — |
| **Doble conexión A/B** | nosotros | **sí** | **2× de red. Disco, casi igual** |

Cripto no publica un feed A y un feed B, pero **se puede fabricar**: dos conexiones
independientes al mismo canal, mejor desde dos máquinas o dos IP, fusionadas por número de
secuencia antes de escribir.

La clave del coste: si se fusiona **antes** de escribir, cada mensaje se guarda una sola
vez. El disco ocupa prácticamente lo mismo. Lo que se duplica es el **ancho de banda**, no
el almacenamiento.

```
hoy            ~33 GB/día en disco, una conexión
con A/B        ~33 GB/día en disco, dos conexiones entrando
```

Es la única de las tres que está a nuestro alcance, y es la única que reduciría de verdad
la pérdida real medida (18 roturas de cadena en 7,2 millones de deltas).

## Lo que esto NO arregla

La detección de huecos **ya es completa**: las tres reglas al 100,0000 %. Siempre sabemos
cuándo perdimos algo. Lo que no podemos es recuperarlo, y A/B solo reduce la probabilidad
de perderlo, no la elimina.

Una grabación nunca es *perfecta*. Es **medidamente completa**: no se trata de perder
cero, se trata de saber exactamente cuánto y dónde. Eso ya lo tenemos.
