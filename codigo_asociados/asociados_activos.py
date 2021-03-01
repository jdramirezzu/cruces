#importar librerias 
import pandas as pd # Manipulacion de datos
from pandas import ExcelWriter #Crear archivo de excel
import sqlalchemy as sql # Conexion con base de datos


if __name__ == '__main__':
    
    #Se establecen los criterio de las fechas de revision
    fecha_inicio = input("Por favor inserte la fecha inicial de su revisión en formato aaaa-mm-dd ")
    fecha_fin = input("Por favor inserte la fecha final de su revisión en formato aaaa-mm-dd ")
    print("Por favor espere mientras se realiza el proceso de cruce")
    #Conexion con la base de datos
    engine = sql.create_engine("mysql://mysql:reportserver@34.74.68.92:3306/BIServer") #Credenciales BD
    aportes_por_cc = pd.read_sql_query(f"select `dep`.`cu_customermaster_id` AS `cliente_id`,`cliente`.`identification` AS `cedula`, coalesce(`cliente`.`razonsocial`,'') AS `Nombre_Completo`, coalesce(`cbl2`.`name`,'') AS `Agencia`,coalesce(`cbl`.`name`,'') AS `Sucursal`, SUM(`dep`.`saldo_actual`) AS `saldo`, `cliente`.`entry_date` AS `fecha_ingreso` from ((`dep_deposito` `dep` left join `prd_producto` `pro` on((`dep`.`prd_producto_id` = `pro`.`prd_producto_id`))) left join `prd_tipo_producto` `tip` ON ((`pro`.`prd_tipo_producto_id` = `tip`.`prd_tipo_producto_id`)) left join `cu_customermaster` `cliente` ON ((`cliente`.`cu_customermaster_id`=`dep`.`cu_customermaster_id`)) left join `c_bpartner_location` `cbl` on((`cliente`.`branch` = `cbl`.`c_bpartner_location_id`))) join `c_bpartner_location` `cbl2` on((`cbl`.`ad_org_location_id` = `cbl2`.`c_bpartner_location_id`)) WHERE (`tip`.`clasificacion` = 'AT') AND (`cliente`.`entry_date` < '{fecha_fin}') group BY `dep`.`cu_customermaster_id`",engine)
    #Vista de movimientos por asociado
    movimientos_asociados = pd.read_sql_query(f"select coalesce(`movAgencia`.`name`,'') AS `Agencia_Movimiento`,coalesce(`movAgencia`.`c_bpartner_location_id`,'') AS `Agencia_Location`,coalesce(`cliAgencia`.`name`,'') AS `Agencia_Cliente`,coalesce(`prodAgencia`.`name`,'') AS `Agencia_Producto`,coalesce(`cliente`.`cu_customermaster_id`,'') AS `Cliente_id`,coalesce(`cliente`.`razonsocial`,'') AS `Cliente`, coalesce(`cliente`.`identification`,'') AS `Cedula`,coalesce(`producto`.`prd_producto_id`,'') AS `Producto_id`,coalesce(`producto`.`nombre_producto`,'') AS `Producto`,coalesce(`deposito`.`cuenta_cliente`,'') AS `Nro_Cuenta`,coalesce(`dep_movimiento`.`num_doc`,'') AS `Numero_Documento`,coalesce(`canalref`.`name`,'') AS `Canal`,coalesce(`dep_movimiento`.`fecha_movimiento`,'0001-01-01') AS `Fecha_Movimiento`,coalesce(`dep_movimiento`.`descripcion`,'') AS `Descripcion`,coalesce(`dep_movimiento`.`tipo_movimiento`,'') AS `Tipo_Movimiento`,coalesce(`dep_movimiento`.`monto_movimiento`,0) AS `Monto_Movimiento`,coalesce(`deposito`.`monto_congelado`,0) AS `Monto_Congelado`,coalesce(`dep_movimiento`.`saldo_inicial`,0) AS `Saldo_Inicial`,coalesce(`dep_movimiento`.`saldo_final`,0) AS `Saldo_Final`,coalesce(`comprobante`.`nombre`,'') AS `Comprobante`,coalesce(`tipo_doc`.`descripcion`,'') AS `Tipo_Documento` from ((((((((((`dep_movimiento` left join `dep_deposito` `deposito` on((`deposito`.`dep_deposito_id` = `dep_movimiento`.`dep_deposito_id`))) left join `prd_producto` `producto` on((`producto`.`prd_producto_id` = `deposito`.`prd_producto_id`))) left join `cu_customermaster` `cliente` on((`deposito`.`cu_customermaster_id` = `cliente`.`cu_customermaster_id`))) left join `adm_canal` `canal` on((`canal`.`adm_canal_id` = `dep_movimiento`.`adm_canal_id`))) left join `ad_ref_list` `canalref` on(((`canal`.`canal` = `canalref`.`value`) and (`canalref`.`ad_reference_id` = 101300)))) left join `c_bpartner_location` `movAgencia` on((`dep_movimiento`.`agencia_id` = `movAgencia`.`c_bpartner_location_id`))) left join `c_bpartner_location` `cliAgencia` on((`cliente`.`branch` = `cliAgencia`.`c_bpartner_location_id`))) left join `c_bpartner_location` `prodAgencia` on((`deposito`.`agencia_id` = `prodAgencia`.`c_bpartner_location_id`))) left join `ctbconf_comprobante` `comprobante` on((`dep_movimiento`.`ctbconf_comprobante_id` = `comprobante`.`ctbconf_comprobante_id`))) left join `ctbconf_tipo_doc` `tipo_doc` on((`dep_movimiento`.`ctbconf_tipo_doc_id` = `tipo_doc`.`ctbconf_tipo_doc_id`))) WHERE (`dep_movimiento`.`fecha_movimiento` BETWEEN '{fecha_inicio}' AND '{fecha_fin}') AND (`canalref`.`name` = 'Caja' OR `canalref`.`name` = 'Debito Automatico' OR `canalref`.`name` = 'Distribución de Fondos' OR `canalref`.`name` = 'Distribuciones' OR `canalref`.`name` = 'Internet Banking' OR `canalref`.`name` = 'PDA' OR `canalref`.`name` = 'Red' OR `canalref`.`name` = 'Red Reexpedicion' OR (`canalref`.`name` = 'Traslados Internos' AND NOT `tipo_doc`.`descripcion` = 'Cobro Cuota de Manejo TD' AND NOT `tipo_doc`.`descripcion` = 'Liquidacion Intereses Depositos' AND NOT `tipo_doc`.`descripcion` = 'Retiro de Clientes' AND NOT `tipo_doc`.`descripcion` = 'Reversos Depositos' AND NOT `tipo_doc`.`descripcion` = 'Reversos Taquilla'))", engine)
    #Vista reporte general de prestamo
    prestamos = pd.read_sql_query(f"select coalesce(`p`.`periodo_gracia`,0) AS `periodo_gracia`,coalesce(`p`.`numero_pagare`,0) AS `pagare`,coalesce(`p`.`plazo`,0) AS `plazo`,coalesce(`p`.`tasa_total`,0) AS `tasa`,coalesce(`p`.`saldo`,0) AS `saldo_capital`,coalesce(`p`.`fecha_ultimo_pago`,'') AS `fecha_ultimo_pago`,coalesce(`p`.`fecha_proximo_pago`,'') AS `fecha_proximo_pago`,coalesce(`p`.`dias_mora`,0) AS `dias_atraso`,coalesce(`d`.`fecha_desembolso`,'') AS `fecha_desembolso`,coalesce(`d`.`monto_desembolsar`,0) AS `monto_desembolsar`,coalesce(`pb`.`interes_causado_nocobrado`,0) AS `interes_corriente`,coalesce(`pb`.`interes_mora_causado_nocobrado`,0) AS `interes_mora`,coalesce(`pb`.`interes_causado_cuenta_orden`,0) AS `interes_dificil_cobro`,coalesce(`s`.`name`,'') AS `agencia`,coalesce(`c`.`razonsocial`,'') AS `cliente`,coalesce(`c`.`identificationtype`,'') AS `Tipo_Identificacion`,coalesce(`c`.`identification`,'') AS `Nro_Identificacion`,coalesce(`pr`.`nombre_producto`,'') AS `producto`,(case when (`p`.`tipo_prestamo` = 'c') then 'consumo' when (`p`.`tipo_prestamo` = 'l') then 'comercial' when (`p`.`tipo_prestamo` = 'h') then 'hipotecario' when (`p`.`tipo_prestamo` = 'm') then 'microcrÃ©dito' end) AS `linea`,coalesce(`pb`.`interes_mora_causado_cuenta_orden`,0) AS `interes_dificil_cobro_mora`,coalesce(`p`.`numero_operacion`,0) AS `numero_prestamo`,coalesce(`p`.`fecha_cancelacion`,'') AS `fechacancelacion`,coalesce(`garantia`.`numero_garantia`,0) AS `nrogarantia`,coalesce(`tipo_garantia`.`descripcion`,'') AS `tipogarantia`,coalesce(`garantia`.`valor_inicial`,0) AS `valorgarantia`,coalesce(`p`.`estado`,'') AS `estado`,coalesce(`pagos`.`name`,'') AS `Forma_Pago`,coalesce(`per`.`nombre`,'') AS `Periodicidad`,coalesce(`asesor`.`razonsocial`,'') AS `nombre_asesor`,coalesce(`aprobacion`.`acta_aprobacion`,'') AS `Acta_Aprobacion`,coalesce(`tipo`.`description`,'') AS `Destino_Credito`,round(coalesce(`dm`.`dias_mora_promedio`,0),2) AS `Dias_Mora_Promedio`,coalesce(`p`.`capital_inicial`,0) AS `Capital_Inicial`,coalesce(`cobros`.`capital_base`,0) AS `Valor_Capital_Cuota`,coalesce(`cobros`.`interes_corriente_base`,0) AS `Interes_Cuota`,coalesce(`cobros`.`monto_total_cobros_adicionales`,0) AS `Cobros_Adicionales`,`des`.`name` AS `Usuario_Desembolso`,`p`.`ind_reestructuracion` AS `ind_reestructuracion`,coalesce(`p`.`fecha_reestructuracion`,'') AS `Fecha_Reestructuracion`,coalesce(`p`.`prestamo_relacionado`,'') AS `Prestamo_Relacionado`,coalesce(`p`.`fecha_vencimiento_final`,'') AS `fecha_vencimiento_final` from (((((((((((((((((`prm_prestamo` `p` left join `prm_desembolso` `d` on((`p`.`prm_prestamo_id` = `d`.`prm_prestamo_id`))) left join `prm_balance` `pb` on((`p`.`prm_prestamo_id` = `pb`.`prm_prestamo_id`))) left join `c_bpartner_location` `s` on((`p`.`c_bpartner_location_id` = `s`.`c_bpartner_location_id`))) left join `cu_customermaster` `c` on((`p`.`cu_customermaster_id` = `c`.`cu_customermaster_id`))) left join `prd_producto` `pr` on((`p`.`prd_producto_id` = `pr`.`prd_producto_id`))) left join `cpo_garantia` `cupo` on(((`p`.`numero_cupo` = `cupo`.`cpo_cupo_id`) and (`cupo`.`isactive` = 'Y')))) left join `grt_garantia` `garantia` on((`cupo`.`grt_garantia_id` = `garantia`.`grt_garantia_id`))) left join `grt_tipo_garantia` `tipo_garantia` on((`garantia`.`grt_tipo_garantia_id` = `tipo_garantia`.`grt_tipo_garantia_id`))) left join `cu_customermaster` `asesor` on((`asesor`.`cu_customermaster_id` = `p`.`cu_asesor_id`))) left join `scr_credito` `credito` on((`p`.`numero_operacion` = `credito`.`numero_prestamo`))) left join `scr_aprobacion` `aprobacion` on((`credito`.`scr_credito_id` = `aprobacion`.`scr_credito_id`))) left join `prd_coddesc` `tipo` on((`p`.`destino_credito` = `tipo`.`prd_coddesc_id`))) left join `vw_rs_prestamos_dias_mora` `dm` on((`p`.`prm_prestamo_id` = `dm`.`prm_prestamo_id`))) left join `vw_rs_prestamos_cobros` `cobros` on((`p`.`prm_prestamo_id` = `cobros`.`prm_prestamo_id`))) left join `ad_user` `des` on((`d`.`createdby` = `des`.`ad_user_id`))) left join `adm_periodicidad` `per` on((`p`.`periodicidad` = `per`.`adm_periodicidad_id`))) left join `ad_ref_list` `pagos` on(((`p`.`adm_formas_pago_id` = `pagos`.`value`) and (`pagos`.`ad_reference_id` = 800722))))where (`d`.`fecha_desembolso` BETWEEN '{fecha_inicio}' AND '{fecha_fin}') AND (`p`.`estado` = '0' OR `p`.`estado`= '7')",engine)
    
    # Crea dataframe que agrupe el número de movimientos hechos por asociado clasificado por cedula
    cuenta_movimientos = pd.DataFrame(data = movimientos_asociados.groupby(['Cedula'])['Cliente_id'].count())
    movimientos_cedula = cuenta_movimientos.reset_index()
    movimientos_cedula = movimientos_cedula.rename(columns = {'Cliente_id':'Cuenta_movimientos'})
   
    # Crea dataframe que agrupe el número de prestamos por asociado utilizando como criterio la cedula
    cuenta_prestamos = pd.DataFrame(data = prestamos.groupby(['Nro_Identificacion'])['Nro_Identificacion'].count())
    cuenta_prestamos = cuenta_prestamos.rename(columns = {'Nro_Identificacion': 'Cuenta_prestamos_cedula'})
    prestamos_conf = cuenta_prestamos.reset_index()
    
    #Convierte los tipos de datos de aquellas columnas que estan como objetos y deben ser numericos
    aportes_por_cc['cedula'] = pd.to_numeric(aportes_por_cc['cedula'], errors = 'coerce')
    movimientos_asociados['Cliente_id'] = pd.to_numeric(movimientos_asociados['Cliente_id'], errors = 'coerce')
    movimientos_cedula['Cedula'] = pd.to_numeric(movimientos_cedula['Cedula'], errors = 'coerce')
    prestamos['Nro_Identificacion'] = pd.to_numeric(prestamos['Nro_Identificacion'], errors = 'coerce')
    prestamos_conf['Nro_Identificacion'] = pd.to_numeric(prestamos_conf['Nro_Identificacion'], errors = 'coerce')

    # Se efectua el merge entre las tres tablas
    merge_aportes_movimientos = aportes_por_cc.merge(movimientos_cedula, how = 'left', left_on = 'cedula', right_on = 'Cedula')
    merge_prestamos = merge_aportes_movimientos.merge(prestamos_conf, how = 'left', left_on = 'cedula', right_on = 'Nro_Identificacion', validate = 'one_to_one')
    cruce = merge_prestamos.filter(['cedula','Nombre_Completo','Agencia','saldo','fecha_ingreso', 'Cuenta_movimientos', 'Cuenta_prestamos_cedula'], axis=1)
    

    for i in cruce.index:
        # Marca la condicion determinada por el valor del saldo
        if cruce.loc[i, 'saldo'] > 52668:
            cruce.loc[i, 'condicion_saldo'] = 1
        else: 
            cruce.loc[i, 'condicion_saldo'] = 0
        # Marca la condicion determinada por el número de movimientos
        if cruce.loc[i, 'Cuenta_movimientos'] > 0:
            cruce.loc[i, 'condicion_movimientos'] = 1
        else: 
            cruce.loc[i, 'condicion_movimientos'] = 0
        # Marca la condicion determinada por prestamos desembolsados al asociado
        if cruce.loc[i, 'Cuenta_prestamos_cedula'] > 0:
            cruce.loc[i, 'condicion_prestamo'] = 1
        else: 
            cruce.loc[i, 'condicion_prestamo'] = 0
        # Suma las condiciones determinadas
        cruce.loc[i, 'suma_condiciones'] = cruce.loc[i, 'condicion_saldo'] + cruce.loc[i, 'condicion_movimientos'] + cruce.loc[i, 'condicion_prestamo']
        if cruce.loc[i, 'suma_condiciones'] > 0:
            cruce.loc[i, 'estado_asociado'] = ("Activo")
        else:
            cruce.loc[i, 'estado_asociado'] = ("Inactivo")
        
        print("Cargando archivo con la condicion de un solo movimiento")
        print(f"Cargando elemento # {i} de {len(cruce)}")

    writer = ExcelWriter(f'{fecha_fin}_Asociados_activos_condicion 1 movimiento.xlsx')
    cruce.to_excel(writer, 'Hoja de datos', index=False)
    writer.save()

    for i in cruce.index:
        if cruce.loc[i, 'Cuenta_movimientos'] > 3:
            cruce.loc[i, 'condicion_movimientos'] = 1
        else: 
            cruce.loc[i, 'condicion_movimientos'] = 0
        
        cruce.loc[i, 'suma_condiciones'] = cruce.loc[i, 'condicion_saldo'] + cruce.loc[i, 'condicion_movimientos'] + cruce.loc[i, 'condicion_prestamo']
        if cruce.loc[i, 'suma_condiciones'] > 0:
            cruce.loc[i, 'estado_asociado'] = ("Activo")
        else:
            cruce.loc[i, 'estado_asociado'] = ("Inactivo")
        
        print("Cargando archivo con la condicion de mas de tres movimiento")
        print(f"Cargando elemento # {i} de {len(cruce)}")         


    writer = ExcelWriter(f'{fecha_fin}_Asociados_activos_condicion 4 movimientos.xlsx')
    cruce.to_excel(writer, 'Hoja de datos', index=False)
    writer.save()

    print("El proceso ha sido exitoso y puede revisar los dos archivos en su carpeta")