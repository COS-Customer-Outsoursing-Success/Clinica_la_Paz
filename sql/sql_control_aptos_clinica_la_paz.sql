WITH tel_duplicados AS (
    SELECT pacientetelefono
    FROM bbdd_cos_bog_clinica_de_la_paz.tb_asignacion_clinica_la_paz_ds
    WHERE fecha_asignacion >= CURDATE()
    GROUP BY pacientetelefono
    HAVING COUNT(*) > 1
)

SELECT motivo AS MOTIVO, COUNT(*) AS TOTAL
FROM (
    SELECT
        CASE
            WHEN LENGTH(pacientetelefono) = 10 
                 AND pacientetelefono IN (SELECT pacientetelefono FROM tel_duplicados)
                THEN 'Telefono duplicado'
            WHEN LENGTH(pacientetelefono) <> 10 THEN 'Telefono errado'
            WHEN pacientenombre IS NULL THEN 'No hay nombre'
            WHEN especialinom IS NULL THEN 'No tiene especialidad'
            WHEN mediconom IS NULL THEN 'No hay medico'
            WHEN consultorionom IS NULL THEN 'No tiene consultorio'
            WHEN centronom IS NULL THEN 'No tiene centro medico'
            ELSE 'Apto'
        END AS motivo
    FROM bbdd_cos_bog_clinica_de_la_paz.tb_asignacion_clinica_la_paz_ds
    WHERE fecha_asignacion >= CURDATE()
) x
GROUP BY motivo;
