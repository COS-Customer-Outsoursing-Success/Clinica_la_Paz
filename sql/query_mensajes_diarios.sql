-- Teléfonos primarios
SELECT 
    pacientenombre,
    pacientetelefono AS phone,
    especialinom,
    mediconom,
    fechacitaini,
    consultorionom,
    centronom
FROM bbdd_cos_bog_clinica_de_la_paz.tb_asignacion_clinica_la_paz_ds
WHERE 
    DATE(fechacitaini) >= CURDATE()
    AND pacientetelefono REGEXP '^3[0-9]{9}$'
    AND pacientetelefono != '-'
    AND pacientenombre IS NOT NULL
    AND especialinom IS NOT NULL
    AND mediconom IS NOT NULL
    AND consultorionom IS NOT NULL
    AND centronom IS NOT NULL

UNION ALL

-- Teléfonos secundarios (solo si existen y son válidos)
SELECT 
    pacientenombre,
    pacientetelefono_2 AS phone,
    especialinom,
    mediconom,
    fechacitaini,
    consultorionom,
    centronom
FROM bbdd_cos_bog_clinica_de_la_paz.tb_asignacion_clinica_la_paz_ds
WHERE 
    DATE(fechacitaini) >= CURDATE()
    AND pacientetelefono_2 REGEXP '^3[0-9]{9}$'
    AND pacientetelefono_2 != '-'
    AND pacientetelefono_2 IS NOT NULL
    AND pacientenombre IS NOT NULL
    AND especialinom IS NOT NULL
    AND mediconom IS NOT NULL
    AND consultorionom IS NOT NULL
    AND centronom IS NOT NULL

ORDER BY 
    fechacitaini ASC;
