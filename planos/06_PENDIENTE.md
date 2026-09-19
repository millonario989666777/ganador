# 06 · PENDIENTE

Por orden de lo que desbloquea más, no de lo que apetece más.

## Armado y en vuelo (19-sep 09:20Z)

| | estado |
|---|---|
| `fs1 2.2` | **automatizado**: el servicio `aplicar22` lo aplica en cuanto muera el último normalizador, y arranca el orquestador |
| G2 día completo binance con fs2 1.1 | corriendo, acaba ~11:56Z |
| `binance_2026-09-15` | cadena ajena, 98 %, acaba ~09:27Z |

## Lo primero, cuando 2.2 esté dentro

1. **Renormalizar bybit y okx con 2.2.** Los cinco días de cada uno. Es lo que desbloquea
   su libro. Ritmo medido: bybit 59,3 s/fichero, okx 30,3 s/fichero.
2. **Medir el antes/después del latido en bybit.** `bybit_2026-09-14` está en PASS con 2.1
   y es el **control limpio**: misma partición, misma hora de evento correcta, única
   diferencia el latido.
3. **Medir cuánto sube okx con sus 606 fotos en banda.** No se estima: se mide.

## Integración que falta

4. **Meter `fs2_dialectos_libro.py` dentro de `fs2_libro.py`.** Está escrito y con 30
   pruebas. Sin eso **bybit no puede llegar a G2**: `fs2_libro` lleva la regla de `pu` a
   mano y bybit no manda `pu`.

## El coste real, la pieza que falta

5. **Añadir a `fs2_libro` el coste de llenar X dólares** recorriendo niveles, y volver a
   pasar el día (2 h 37 min). Hoy sabemos que la orden **cabe**; no a qué precio medio se
   llena. Es lo último que falta para sustituir el `slippage 1.0` inventado por un número
   real y subir `fee_version`.

## Orquestador

6. **Que no marque como EXTERNO a sus propios hijos.**
7. **Lanzar cada trabajo con `systemd-run --user --unit=... --collect`**, para que tenga
   cgroup propio y sobreviva a un reinicio del servicio. Ya comprobado que funciona para
   el usuario `jean`.

## Grabador (PC2) — NO TOCAR SIN DECISIÓN EXPLÍCITA

Es lo único del sistema que no se puede rehacer. Un cambio mal hecho deja un hueco
irrecuperable.

8. **Leer `capture.py` 120–190** y averiguar por qué no se guarda la foto en banda de
   bybit. Solo leer.
9 bis. **Doble conexión independiente arbitrada por secuencia** (A/B fabricado). Es el
   único cambio de grabación que reduciría de verdad la pérdida real. Cuesta 2× de red y
   casi nada de disco si se fusiona antes de escribir. Ver plano 07.
9. **Pedir la foto REST alineada**: abrir el stream y bufferizar ANTES de pedirla, que es
   el procedimiento que documenta binance. Reduciría el 4,3 % de enganches fallidos en
   origen, además de lo que ya arregla fs2 1.1 en diferido.

## Validación externa

10. **Cruzar contra un oráculo independiente.** Dos opciones:
    - `hftbacktest` o `NautilusTrader`: pasar el mismo día por los dos y comparar mejor
      bid/ask evento a evento.
    - TOKIO, para los 15 símbolos comunes. **Limitación medida**: para sacar el mejor
      precio de TOKIO hay que reconstruir su libro, y TOKIO no tiene fotos. Solo es
      viable vía trades (bybit/okx), no en binance.

## Validación externa — ya no es teórica (ver plano 09)

10.b. **HECHO**: cruce contra el archivo oficial de Binance, `binance_2026-09-13` BTCUSDT.
   Faltan 785 aggTrades de 512.737 = **0,1531 %**; sobran 0; **4 cortes, 265,8 s**.
10.c. **Arreglar el corte de ~61 s.** Dos de los tres cortes duran 61,1 s y 62,2 s. Un
   temporizador fijo de 60 s tarda ese minuto en detectar la caída. Bajarlo (ping/pong
   cada 5–10 s, *read timeout* corto) debería recortar ~123 s de los 265,8 s del día.
   **Toca PC2: no se hace sin decisión explícita.**
10.d. **Extender el cruce a los cinco días y a más símbolos**, y publicar `missing_total`
   junto a cada partición, como hace `market-tape`.
10.e. **Cruzar bybit** contra `public.bybit.com/trading/`, que sí cubre 09-13…09-18.
10.f. **Auditar la cinta cruda de Tardis (2026-09-01) con nuestra regla** y publicar su
   exactitud al lado de la nuestra. Es la única vara de medir externa que hay para el
   *libro*, no sólo para las operaciones.

## Abierto sin investigar

11. `okx_2026-09-13`: **211 reconexiones** frente al umbral de 200 de la norma.
12. `binance_2026-09-12`: **972 reconexiones**. Sin diagnosticar.
13. La cola de símbolos malos de bybit (p25 = 20,98 de tiempo válido).
14. Calidad de red PC2↔PC3: 33–113 ms de ping en LAN provocó un corte.
15. `corte_real_medido.md` en PC2 lleva cifras desfasadas.

## Decisiones que no son mías

- **Los días 07→12 no están en PC3.** Y los 11 días completos no caben: ~770 GB en un
  disco de 468 GB. O tandas, o más disco.
- **Qué hacer con el `MISSING_EVENT_TS` residual** (okx `liquidation`).
- **`binance_2026-09-14`**: su MEDICION se contradice. O se renormaliza, o se acepta la
  nota como excepción declarada.
- **Los tres días de binance que lleva una sesión de login.** Si se cierra, mueren.
