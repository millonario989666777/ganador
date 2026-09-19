# 05 · FALLOS ENCONTRADOS

Cada uno con su prueba y su estado. Los que llevan **(mío)** los causé yo.

## 1 · El latido de bybit iba a la basura — ARREGLADO en 2.2

196.647 filas al día. El comentario del código decía *"No se pierde nada"*. Era falso: el
latido gasta un número de la cadena.

```
con los latidos   320.467 encadenan   0 rompen
sin ellos         317.836 encadenan   1.175 rompen
```

Corrige también un número mío: la exactitud de bybit no era 99,5683 %, es **100,0000 %**.
El 0,43 % lo ponía nuestra cuarentena.

## 2 · okx tiraba 606 fotos en banda al día — ARREGLADO en 2.2

Caían en `UNKNOWN_EVENT_TYPE` porque el lector solo conocía `book_delta` y
`book_deep_snapshot`. Por eso `okx_2026-09-13` dio GATE_G1 FAIL 10/12.

## 3 · La foto vieja tiraba un libro sano — ARREGLADO en fs2 1.1

Desfase mediano +1.762 actualizaciones, siempre positivo. Costaba el 4,1 % del tiempo.
El 96,9 % de esos huecos no hacían falta.

## 4 · El gate rechazaba la versión 2.1 — ARREGLADO

`g1_gate_check.py` comparaba con `startswith("fs1_normalizar/2.0")`, que **no casa** con
`"fs1_normalizar/2.1"`. Toda partición normalizada con 2.1 habría caído en C1 con el
motivo equivocado. Había tres cadenas en vuelo produciendo 2.1 cuando se detectó.

Sustituido por la lista explícita `VERSIONES_FS1_OK`.

## 5 · C7b tenía el punto ciego justo ahí — ARREGLADO

`C7b_NADA_RELEVANTE_DESCARTADO_EN_SILENCIO` solo vigilaba `UNKNOWN_EVENT_TYPE` y
`EMPTY_RESULT`. `BOOK_DELTA_SIN_NIVELES` no estaba, así que 196.647 eslabones tirados
pasaron como PASS. El check que existía para esto no lo vio.

Ahora está en la lista.

## 6 · El sha se sellaba DESPUÉS de correr **(mío)** — ARREGLADO

`binance_2026-09-14` corrió de `16:05:08Z` a `00:29:50Z`. Yo cambié `fs1_normalizar.py` a
las `22:09:25Z`, a mitad. Resultado:

```
recibo: code_version fs1_normalizar/2.0
        normalizador_sha256 ba472881...   ← es el binario 2.1
        (el 2.0 real es    3a80bc4c...)
```

Y el pin quedó fijando que la 2.0 la produce el binario 2.1. **Dos binarios bajo la misma
versión, y el check que existe para impedirlo no saltó.**

Los datos son correctos. Lo roto es la cadena de evidencia.

Arreglado: sello al arrancar, se sella también `fs1_dialectos.py`, y se registra
`normalizador_cambiado_durante_la_corrida`. Pin corregido. Nota junto a la partición.

## 7 · El orquestador marca como EXTERNO a sus propios hijos — ABIERTO

`particiones_externas()` los detecta por argv y no excluye los PID que ha lanzado él
mismo. Comprobado: `ppid = 1338186` = el propio orquestador, y aun así figuraban como
ajenos. No es fatal (a una partición EXTERNO no se la toca, y al terminar se readopta)
pero deja el informe mintiendo y apaga su propia vigilancia.

## 8 · El orquestador mata su trabajo si se reinicia — ABIERTO

Lanza con `subprocess.Popen`, así que los hijos viven en el cgroup del servicio y
`KillMode=control-group`. Comprobado hoy al pararlo: se llevó los dos que colgaban de él.

Es el mismo fallo que costó 4 h en PC2 el 18-sep, y el README del orquestador lo
documenta como lección. **Comprobado que `systemd-run --user` funciona para el usuario
`jean`** (`Linger=yes`), así que darle cgroup propio a cada trabajo es viable.

## 9 · bybit no guarda su foto en banda — ABIERTO, en el grabador

0 en los cinco días. El grabador pasa el payload tal cual, así que si bybit la mandara la
tendríamos. Falta leer `capture.py` 120–190. **No se toca el grabador sin decisión
explícita**: es lo único del sistema que no se puede rehacer.

## 10 · Tres días de binance los lleva una sesión de login — ABIERTO, ajeno

`binance_09-15/16/17`, pid 266344, dentro de `session-3422.scope`. **Si esa sesión se
cierra, mueren los tres.** No es nuestro, pero queda avisado.

## Errores de método que cometí, para no repetirlos

1. **Declaré PC3 caído cuando solo era inalcanzable.** `uptime` decía 1d17h y los tres
   normalizadores estaban vivos. Inalcanzable ≠ apagado.
2. **Di por cerrada una medida a mitad.** Afirmé "la cadena de binance no se rompe ni una
   vez" mirando 37 de 60 ficheros. Al terminar eran 18 roturas.
3. **Saqué una conclusión de un solo nivel.** Dije que con 158 USD al mejor precio muchos
   símbolos no darían para operar. A 5 niveles hay 4.495 y a 20 hay 40.549.
4. **Dije que medir el coste por tamaño eran "un par de horas".** El dato ya estaba en
   `book_state` desde la noche anterior. Tardó 4 minutos.
5. **Prioricé el grabador antes de medir.** La foto en banda de bybit vale ~9 fotos al
   día; el problema real eran las 56.687 REST periódicas.
