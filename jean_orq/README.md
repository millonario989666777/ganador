# JEAN_NORMALIZADOR_ORQUESTADOR_V1

Orquestador determinista que gobierna `fs1_normalizar 2.0` sobre las particiones del corte,
en PC3 (`pc3-grabador`).

**No es un normalizador nuevo.** Ejecuta `g1_particion.py`, que a su vez ejecuta
`fs1_normalizar.py` tal cual. Dos normalizadores compitiendo producen dos datasets que no
cuadran y nadie sabe cuál creer; por eso aquí solo hay uno, y este programa se limita a
gobernarlo: cola, estados, reanudación, verificación y recibos.

```
        IA (supervisa, audita, decide)
                    │
                    ▼
     JEAN_NORMALIZADOR_ORQUESTADOR_V1     <- este programa
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
     worker 1                worker 2          (techo global: 3 normalizadores)
        └──── g1_particion.py ──┘
                    │
            fs1_normalizar 2.0
                    │
                    ▼
      VERIFICACIÓN: rc + manifiesto + hashes +
      recuentos + cuarentena + gate de 12 checks
                    │
              ┌─────┴─────┐
              ▼           ▼
            PASS        FAIL        -> recibo sellado con sha256
```

## Por qué existe

Un PID vivo no demuestra que el trabajo esté sano, y una salida que existe no es un PASS.
Ya ha pasado dos veces: procesos en `active/running` que no producían datos correctos, y
una corrida de prueba que se solapó con la completa dejando el directorio de una con el
manifiesto de la otra, sin que ninguna comprobación interna lo viera.

Y hay un tercer motivo, medido el 18-sep: **una cadena lanzada a mano vive dentro del
cgroup de quien la lanzó**. Cuando ese servicio se reinició, systemd mató el grupo entero y
se llevó por delante casi 4 horas de normalización. `setsid` no protege de eso. Como
servicio propio, sí.

## Instalación

```bash
# en PC3
sudo install -o jean -g jean -m 755 jean_orq.py /home/jean/JEAN_ORQUESTADOR/jean_orq.py
sudo install -m 644 jean-normalizador-orquestador.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now jean-normalizador-orquestador
```

## Uso

```bash
python3 jean_orq.py autotest       # 22 pruebas, sin datos reales ni red
python3 jean_orq.py preflight      # comprueba el entorno; no toca nada
python3 jean_orq.py plan           # descubre particiones y adopta lo ya hecho
python3 jean_orq.py estado         # informe legible
python3 jean_orq.py estado --json  # lo mismo, para que lo lea una IA o un script
python3 jean_orq.py verificar --job okx_2026-09-13
python3 jean_orq.py reintentar --todos-fail
python3 jean_orq.py run --una-vez --dry-run   # ensayo: dice qué haría y no lo hace
```

## Estados

| estado | significado |
|---|---|
| `NEW` | descubierta, sin comprobar |
| `READY` | lista: RAW presente y sellado en G0 |
| `BLOCK` | no ejecutable por causa declarada (sin G0, sin espacio) |
| `RUNNING` | la ejecuta este orquestador |
| `EXTERNO` | la ejecuta otro proceso; **no se toca** |
| `VERIFY` | terminó, se está verificando |
| `PASS` | verificada entera y sellada con recibo |
| `FAIL` | falló con motivo; las demás siguen |

## Qué hace falta para declarar PASS

No basta con que el proceso termine. Se recorre la cadena entera:

1. `returncode == 0` del normalizador
2. `code_version == fs1_normalizar/2.0` y `schema_version == market_event_v1`
3. el sha256 del normalizador **no ha cambiado a mitad del corte** (si cambia, el corte deja
   de ser homogéneo y la partición queda en FAIL)
4. `input_files` del manifiesto == enlaces reales en `raw/`
5. un hash de salida por fichero de entrada, y cada fichero existe (con `--sha-completo`,
   se recalculan todos los sha256)
6. cuarentena: el fichero existe, su sha256 coincide y tiene tantas líneas como declara
7. `filas_normalizadas > 0`
8. el gate `g1_gate_check.py` de 12 checks en PASS

Si ya hay un veredicto de gate sellado **posterior** al manifiesto, se reutiliza en vez de
releer decenas de GB. Si el manifiesto es más nuevo que el veredicto, el veredicto no vale
y el gate se vuelve a ejecutar.

## Alertas de calidad

No bloquean, pero quedan escritas en el recibo:

- **`CONNECTION_START_ALTO`** — más de 200 reconexiones en un día y venue. La norma del
  proyecto pide investigar esa partición antes de usarla: cada reconexión invalida el libro
  hasta el siguiente snapshot, y ese tiempo no produce features.
- **`MISSING_EVENT_TS_ALTO`** — más del 5 % de filas sin hora de evento. Cualquier feature
  *as-of* sobre esas filas cae a `receive_ts`, que no es lo mismo.

## Convivencia con cadenas lanzadas a mano

Detecta `g1_cadena.sh` y `g1_particion.py` vivos **leyendo su argv**, nunca el texto de la
línea de comandos: un comando que solo *menciona* `g1_cadena.sh okx` no es una cadena. Esa
confusión ya provocó que un guardia se detectara a sí mismo y abortara un lanzamiento
legítimo; hay una prueba dedicada a que no vuelva a pasar.

Las particiones en manos ajenas quedan en `EXTERNO` y no se tocan. El orquestador puede
arrancar sin parar nada, y toma el relevo cuando la cadena externa termina o muere.

## Límites que respeta

- **Techo global de 3 normalizadores** en la máquina, contando los ajenos. Medido: cada
  `fs1_normalizar` pica en ~4,3 GB de RSS; con 19 GB de RAM, 3 caben y 4 tiran de swap.
- **Guardia de disco** antes de cada partición: `1,1 × entrada + 25 GiB` de margen. Lo que
  no quepa queda en `BLOCK`, en vez de llenar el disco a medias.
- **Nunca dos veces la misma partición**: candado del orquestador, más el candado propio del
  normalizador (`fcntl.lockf` + marca de dueño), que reclama solo las marcas huérfanas de
  procesos muertos de la misma máquina.
- **No borra RAW**, no toca la captura de mercado, no conecta con exchanges.
  `orders = 0` y `execution_authority = NONE` en todos los recibos.

## Estado medido del corte (18-sep-2026)

- G0 en **PASS**: 21.018 ficheros sellados, scope `2026-09-13..2026-09-17`, tres venues.
- **15 particiones**, no 33: los días 07→12 no están en PC3 todavía. Cuando se descarguen,
  el orquestador los descubre solo, sin tocar el programa.
- Disco: 234 GB libres frente a ~198 GB de salida estimada para las 15. Cabe, con poco
  margen. Los 11 días completos (~770 GB entre crudo y normalizado) **no caben** en el disco
  de 468 GB: harán falta tandas o más disco.
- Alerta ya detectada: `bybit_2026-09-13` con **37,51 %** de filas sin hora de evento.

## Salidas

```
/home/jean/JEAN_ORQUESTADOR/
  estado.json                  estado persistente (escritura atómica)
  RECIBO_ORQUESTADOR.json      recibo global sellado con sha256
  recibos/RECIBO_<job>.json    un recibo por partición, con evidencia y alertas
  logs/orquestador.log         traza de decisiones
  logs/job_<job>.log           salida del driver de cada partición
```

## fs1_normalizar 2.2: el latido de libro de bybit

Un `book_delta` de bybit puede llegar con `b` y `a` vacíos pero con `u` y `seq` nuevos.
Hasta 2.1 esos mensajes iban a cuarentena (`BOOK_DELTA_SIN_NIVELES`) con un comentario que
decía *"No se pierde nada"*. Medido el 18-sep sobre `bybit_2026-09-13`, 3 ficheros crudos,
320.657 `book_delta` de los que 1.456 venían vacíos:

| escenario | encadenan | rompen | `u == u_anterior + 1` |
|---|---|---|---|
| con los latidos dentro | 320.467 | **0** | **100,0000 %** |
| sin ellos (lo que veía fs2) | 317.836 | 1.175 | 99,6317 % |

El latido **gasta** un número de la cadena. Tirarlo abre un agujero que no existía, y cada
agujero deja el libro de ese símbolo inválido hasta la siguiente foto. Ese era el motivo
real de que bybit midiera 69,25 % de tiempo con libro válido, y también del 0,43 % que yo
mismo había atribuido al exchange: era nuestro.

Desde 2.2 el latido se emite como una fila **sin niveles**, con los identificadores y nada
más. `fs2_libro.py:433` ya salta los niveles de precio nulo, así que encadena el libro sin
tocarlo. Se aplica con `parche_fs1_2_2_latido.py`, que **se niega a correr si hay algún
normalizador vivo**: `g1_particion.py:167` sella el sha256 del normalizador *después* de
que el proceso termine, así que cambiar el fichero en caliente haría que los trabajos en
vuelo firmaran su `code_version` real con el sha del binario nuevo.

Alcance medido: **solo bybit**. binance (236.214 `book_delta`) y okx (136.286) no tienen ni
un delta vacío, así que su salida con 2.2 es idéntica a la de 2.1 y sus particiones ya
hechas siguen valiendo.

### Dos fallos del gate encontrados por el camino

- **C1 rechazaba la 2.1.** Comparaba con `startswith("fs1_normalizar/2.0")`, que no casa con
  `"fs1_normalizar/2.1"`. Toda partición normalizada con 2.1 habría caído con el motivo
  equivocado. Sustituido por la lista explícita `VERSIONES_FS1_OK`.
- **C7b tenía el punto ciego justo aquí.** `C7b_NADA_RELEVANTE_DESCARTADO_EN_SILENCIO` solo
  considera silenciosos `UNKNOWN_EVENT_TYPE` y `EMPTY_RESULT`, así que 196.647 eslabones de
  secuencia tirados pasaron como PASS. El check que existía para esto no lo vio.
