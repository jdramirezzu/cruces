
if __name__ == '__main__':
    #Se importan las librerias necesarias
    import pandas as pd 
    from pandas import ExcelWriter
    import sqlalchemy as sql 


    #Se establecen los criterio de las fechas de revision
    fecha_inicio = input("Por favor inserte la fecha inicial de su revisión en formato aaaa-mm-dd ")
    fecha_fin = input("Por favor inserte la fecha final de su revisión en formato aaaa-mm-dd ")
    print("Iniciando con el proceso de conexion a base de datos")
    #Credenciales BD
    engine = sql.create_engine("mysql://mysql:reportserver@34.74.68.92:3306/BIServer") 
    #Vista de datos personales y total en aportes con especificaciones establecidas
    revision_aportes = pd.read_sql_query(f"select `cliente`.`identification` AS `cedula`,sum(`dep`.`saldo_actual`) AS `saldo`,`cliente`.`entry_date` AS `fecha_ingreso` from ((`dep_deposito` `dep` left join `prd_producto` `pro` on((`dep`.`prd_producto_id` = `pro`.`prd_producto_id`))) left join `prd_tipo_producto` `tip` on((`pro`.`prd_tipo_producto_id` = `tip`.`prd_tipo_producto_id`)))join `cu_customermaster` `cliente` on `cliente`.`cu_customermaster_id`=`dep`.`cu_customermaster_id` where (`tip`.`clasificacion` = 'AT') group by `dep`.`cu_customermaster_id`", engine)
    #Vista de prestamos desembolsados por linea de aportes
    desembolsos_aportes = pd.read_sql_query(f"select `agencias`.`name` AS `Agencia`, `cliente`.`identification` AS `Identificación_Asociado`,`cliente`.`razonsocial` AS `Nombre`, SUM(`prestamo`.`monto_financiar`) AS `Capital_Inicial`,`prestamo`.`fecha_inicio` AS `Fecha_Desembolso`,`prestamo`.`saldo` AS `Saldo_Capital` FROM (((((((((((((((((((`prm_prestamo` `prestamo` join `c_bpartner_location` `agencias` on((`agencias`.`c_bpartner_location_id` = `prestamo`.`c_bpartner_location_id`))) join `cu_customermaster` `cliente` on((`cliente`.`cu_customermaster_id` = `prestamo`.`cu_customermaster_id`))) left join `cu_address` `direccion` on(((`direccion`.`cu_customermaster_id` = `cliente`.`cu_customermaster_id`) and (`direccion`.`addressclass` = 'df') and (`direccion`.`is_mainaddress` = 'y')))) left join `adm_coddesc` `parametros` on(((`cliente`.`sex` = `parametros`.`adm_coddesc_id`) and (`parametros`.`adm_codtitle_id` = 100007)))) left join `c_city` `ciudad` on((`ciudad`.`c_city_id` = `direccion`.`c_city_id`))) left join `cu_address` `telefono` on(((`telefono`.`cu_customermaster_id` = `cliente`.`cu_customermaster_id`) and (`telefono`.`addressclass` = 'tm') and (`telefono`.`is_mainaddress` = 'y')))) left join `cu_address` `email` on(((`email`.`cu_customermaster_id` = `cliente`.`cu_customermaster_id`) and (`email`.`addressclass` = 'ce') and (`email`.`is_mainaddress` = 'y')))) join `prd_producto` `lineaproducto` on((`prestamo`.`prd_producto_id` = `lineaproducto`.`prd_producto_id`))) join `prd_tipo_producto` `producto` on((`lineaproducto`.`prd_tipo_producto_id` = `producto`.`prd_tipo_producto_id`))) join `adm_periodicidad` `periodicidad` on((`periodicidad`.`adm_periodicidad_id` = `prestamo`.`periodicidad`))) left join `prm_causacion_int_mora_hist` `causacion` on(((`causacion`.`fecha_proceso` = (curdate() - interval 1 day)) and (`causacion`.`prm_prestamo_id` = `prestamo`.`prm_prestamo_id`)))) left join `vw_rs_cartera_edades_parte01_cuotas` `cuotas` on((`cuotas`.`prm_prestamo_id` = `prestamo`.`prm_prestamo_id`))) left join `gst_mora_conf` `calificacion` on((`calificacion`.`gst_mora_conf_id` = `prestamo`.`grado_mora`))) left join `vw_rs_cartera_edades_parte02_calificacion_arrastre` `calificacion_arrastre` on((`calificacion_arrastre`.`gst_mora_conf_id` = `prestamo`.`grado_mora`))) left join `vw_rs_cartera_edades_parte03_garantia` `garantia` on((`garantia`.`cpo_cupo_id` = `prestamo`.`numero_cupo`))) left join `prv_repositorio_prestamo` `provision` on(((`provision`.`fecha_cierre` = (curdate() - interval 1 month)) and (`provision`.`prm_prestamo_id` = `prestamo`.`prm_prestamo_id`)))) left join `cu_customermaster` `asesor` on((`asesor`.`cu_customermaster_id` = `prestamo`.`cu_asesor_id`))) left join `ad_ref_list` `etapa_cobro` on(((`etapa_cobro`.`value` = `prestamo`.`estado_cobro`) and (`etapa_cobro`.`ad_reference_id` = 102026)))) left join `vw_rs_cartera_edades_parte06_producto_gestion` `gestion` on((`prestamo`.`prm_prestamo_id` = `gestion`.`prm_prestamo_id`))) where `prestamo`.`estado` = '0' AND `prestamo`.`fecha_inicio` BETWEEN '{fecha_inicio}' AND '{fecha_fin}' AND `lineaproducto`.`nombre_producto` LIKE '%%APORTES%%' GROUP BY `cliente`.`identification`", engine)
    #Merge entre las dos vistas cargadas
    merge_desembolsosaportes_aportes = revision_aportes.merge(desembolsos_aportes, how = 'inner', left_on = 'cedula', right_on = 'Identificación_Asociado')
    #Revisar si se cumple la condición de que el desembolso por linea de aportes sea como maximo 5 veces el valor que tiene en aportes
    for i in merge_desembolsosaportes_aportes.index:
        diferencia = (merge_desembolsosaportes_aportes.loc[i,'saldo']*5) - merge_desembolsosaportes_aportes.loc[i, 'Capital_Inicial']  
        merge_desembolsosaportes_aportes.loc[i,'Diferencia_saldo'] = diferencia
        if diferencia < 0:
            merge_desembolsosaportes_aportes.loc[i,'Observacion'] = (f"Favor consignar el valor de {diferencia} a aportes")
        else:
            merge_desembolsosaportes_aportes = merge_desembolsosaportes_aportes.drop([i], axis=0)
        print(f"Analizando elemento{i} de {len(merge_desembolsosaportes_aportes)}")
    
    #Se crea el archivo de excel con la revision
    writer = ExcelWriter(f'{fecha_inicio}_{fecha_fin}_Desembolsos por aportes.xlsx')
    merge_desembolsosaportes_aportes.to_excel(writer, 'Hoja de datos', index=False)
    writer.save()