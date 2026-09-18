# Orquestador canonico: jean_orq

Decision del usuario, 20260918.

    CANONICO      jean_orq.py  (jean_orq/1.0.0)
                  PC3: /home/jean/JEAN_ORQUESTADOR/jean_orq.py
                  servicio jean-normalizador-orquestador.service (active, enabled)

    NO CANONICO   /home/jean/JEAN_NORMALIZADOR_ORQUESTADOR_V1_STAGING/jean_orchestrator.py
                  (linea de ChatGPT) se conserva como referencia

Dos orquestadores gobernando el mismo derivado es el mismo error que dos
normalizadores compitiendo: acaban en dos estados que no cuadran y nadie sabe
cual creer. Uno manda.

Lo aplicado en PC3, sin destruir nada del otro:
  - CANONICO.md en /home/jean/JEAN_ORQUESTADOR/
  - NO_CANONICO.md en el staging, con el motivo y como revertir
  - chmod -x sobre jean_orchestrator.py: no arranca por descuido, sigue legible

Comprobado ANTES de decidir que el staging no podia arrancar solo: launch_enabled
false, sin proceso vivo, sin unidad systemd propia, sin timer y sin cron.
