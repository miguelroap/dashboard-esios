import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# --- CONFIGURACIÓN DE SEGURIDAD Y PÁGINA ---
try:
    TOKEN_ESIOS = st.secrets["ESIOS_TOKEN"]
except FileNotFoundError:
    TOKEN_ESIOS = "TU_TOKEN_AQUI" # Cambia esto para pruebas en local

st.set_page_config(page_title="Dashboard Analítico ESIOS", layout="wide")
st.title("⚡ Dashboard Analítico de Mercado (ESIOS)")

# --- BARRA LATERAL: NAVEGACIÓN Y FILTROS ---
st.sidebar.header("Navegación")
seccion = st.sidebar.radio(
    "Selecciona el módulo:",
    ("Mercados de Ajuste", "Precios de Captura Renovables", "Producción vs Estimación")
)

st.sidebar.markdown("---")
st.sidebar.header("Filtros Globales")

hoy = datetime.now()
hace_7_dias = hoy - timedelta(days=7)
mañana = hoy + timedelta(days=1)

fechas = st.sidebar.date_input("Selecciona el periodo:", value=(hace_7_dias, hoy), max_value=mañana)

# --- DICCIONARIOS DE INDICADORES ---
indicadores_precio = {
    "600": "Precio mercado diario España",
    "708": "Precio restricciones técnicas fase 2",
    "2197": "Precio mFRR (terciaria)"
}

indicadores_energia = {
    "704": "Energía casada RT fase 2 a bajar",
    "10394": "Energía asignada mFRR a bajar",
    "10395": "Energía asignada mFRR a subir"
}

indicadores_secundaria = {
    "2130": "Precio banda secundaria subir (€/MW)",
    "634": "Precio banda secundaria bajar (€/MW)",
    "682": "Precio energía secundaria subir (€/MWh)",
    "683": "Precio energía secundaria bajar (€/MWh)"
}

# --- FUNCIONES DE ACCESO A LA API ---
headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN_ESIOS
}

@st.cache_data(ttl=3600)
def obtener_datos_simples(indicator_id, nombre_indicador, start_date, end_date):
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    params = {
        'start_date': start_date.strftime('%Y-%m-%dT00:00:00Z'),
        'end_date': end_date.strftime('%Y-%m-%dT23:59:59Z')
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if 'indicator' in data and 'values' in data['indicator']:
            df = pd.DataFrame(data['indicator']['values'])
            if not df.empty and 'geo_id' in df.columns:
                geo_filtro = 3 if str(indicator_id) == "600" else 8741
                df = df[df['geo_id'] == geo_filtro]
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Europe/Madrid').dt.tz_localize(None)
                df['indicator_id'] = str(indicator_id)
                df['Indicador'] = nombre_indicador
                return df[['datetime', 'value', 'Indicador']]
    return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def obtener_datos_batched(indicator_id, start_dt, end_dt, specific_geo):
    all_chunks = []
    current_date = start_dt
    
    while current_date <= end_dt:
        next_month = current_date + relativedelta(months=1)
        chunk_end = min(next_month, end_dt)
        
        url = f"https://api.esios.ree.es/indicators/{indicator_id}"
        params = {
            "start_date": current_date.strftime('%Y-%m-%dT00:00:00Z'),
            "end_date": chunk_end.strftime('%Y-%m-%dT23:59:59Z'),
            "geo_ids[]": specific_geo
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'indicator' in data and 'values' in data['indicator']:
                    df = pd.DataFrame(data['indicator']['values'])
                    if not df.empty and 'geo_id' in df.columns:
                        df = df[df['geo_id'] == specific_geo]
                    if not df.empty:
                        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                        all_chunks.append(df[['datetime', 'value']])
        except:
            pass
        current_date = next_month

    if not all_chunks:
        return pd.DataFrame()
        
    df_total = pd.concat(all_chunks).drop_duplicates(subset='datetime')
    df_total['datetime'] = df_total['datetime'].dt.tz_convert('Europe/Madrid').dt.tz_localize(None)
    df_total = df_total.set_index('datetime').sort_index()
    return df_total[['value']]

# --- FUNCIONES DE PROCESAMIENTO ---
def agrupar_datos(df, frecuencia, tipo):
    if frecuencia is None or df.empty: return df 
    operacion = 'mean' if tipo == 'precio' else 'sum'
    return df.groupby([pd.Grouper(key='datetime', freq=frecuencia), 'Indicador'])['value'].agg(operacion).reset_index()

def generar_perfil(df):
    if df.empty: return df
    df_local = df.copy()
    df_local['Hora'] = df_local['datetime'].dt.hour
    return df_local.groupby(['Hora', 'Indicador'])['value'].mean().reset_index()

# ==============================================================================
# PÁGINA 1: MERCADOS DE AJUSTE
# ==============================================================================
def pagina_ajustes(start_date, end_date):
    st.subheader("📊 Mercados Diarios y de Ajuste")
    
    col1, col2 = st.columns(2)
    with col1:
        agrupacion = st.selectbox("Agrupación temporal:", ("Cuartohorario", "Horario", "Diario", "Mensual", "Anual"))
    with col2:
        st.write("")
        st.write("")
        perfil_24h = st.checkbox("Activar Perfil Medio 24h", value=False)

    frecuencias = {"Cuartohorario": None, "Horario": "h", "Diario": "D", "Mensual": "MS", "Anual": "YS"}
    freq = frecuencias[agrupacion]
    x_col = 'Hora' if perfil_24h else 'datetime'
    
    with st.spinner('Obteniendo y procesando datos de ajuste...'):
        dfs_precios = [obtener_datos_simples(i, n, start_date, end_date) for i, n in indicadores_precio.items()]
        dfs_precios = [df for df in dfs_precios if not df.empty]
        
        dfs_energia = [obtener_datos_simples(i, n, start_date, end_date) for i, n in indicadores_energia.items()]
        dfs_energia = [df for df in dfs_energia if not df.empty]
        
        dfs_secundaria = [obtener_datos_simples(i, n, start_date, end_date) for i, n in indicadores_secundaria.items()]
        dfs_secundaria = [df for df in dfs_secundaria if not df.empty]
        
        # PROCESAMIENTO Y AGRUPACIÓN
        lista_dfs_agrupados = []
        df_final_precios = None
        df_final_energia = None
        df_final_secundaria = None

        if dfs_precios:
            df_final_precios = pd.concat(dfs_precios, ignore_index=True)
            if perfil_24h:
                df_final_precios = agrupar_datos(df_final_precios, 'h', 'precio')
                df_final_precios = generar_perfil(df_final_precios)
            else:
                df_final_precios = agrupar_datos(df_final_precios, freq, 'precio')
            lista_dfs_agrupados.append(df_final_precios)

        if dfs_energia:
            df_final_energia = pd.concat(dfs_energia, ignore_index=True)
            if perfil_24h:
                df_final_energia = agrupar_datos(df_final_energia, 'h', 'energia')
                df_final_energia = generar_perfil(df_final_energia)
            else:
                df_final_energia = agrupar_datos(df_final_energia, freq, 'energia')
            lista_dfs_agrupados.append(df_final_energia)

        if dfs_secundaria:
            df_final_secundaria = pd.concat(dfs_secundaria, ignore_index=True)
            if perfil_24h:
                df_final_secundaria = agrupar_datos(df_final_secundaria, 'h', 'precio')
                df_final_secundaria = generar_perfil(df_final_secundaria)
            else:
                df_final_secundaria = agrupar_datos(df_final_secundaria, freq, 'precio')
            lista_dfs_agrupados.append(df_final_secundaria)

        # --- CÁLCULOS MATEMÁTICOS DE SPREAD Y BENEFICIO ---
        spread = None
        beneficio = None
        df_pivot_precios = pd.DataFrame()
        
        if df_final_precios is not None and df_final_energia is not None:
            # Pivotar los dataframes para tener los indicadores como columnas y alinear los periodos temporales
            df_pivot_precios = df_final_precios.pivot_table(index=x_col, columns='Indicador', values='value', aggfunc='first')
            df_pivot_energia = df_final_energia.pivot_table(index=x_col, columns='Indicador', values='value', aggfunc='first')
            
            ind_diario = "Precio mercado diario España"
            ind_rt2 = "Precio restricciones técnicas fase 2"
            ind_e_rt2_baj = "Energía casada RT fase 2 a bajar"
            
            # Calcular spread si existen los indicadores
            if ind_diario in df_pivot_precios.columns and ind_rt2 in df_pivot_precios.columns:
                spread = df_pivot_precios[ind_diario] - df_pivot_precios[ind_rt2]
                
                # Calcular beneficio si existe la energía
                if ind_e_rt2_baj in df_pivot_energia.columns:
                    beneficio = spread * df_pivot_energia[ind_e_rt2_baj]

        # --- NUEVO: TABLA DE POTENCIAL BENEFICIO AL PRINCIPIO ---
        if beneficio is not None:
            st.markdown("### 💰 Potencial Beneficio RT Fase 2 a Bajar")
            st.markdown("Cálculo: *(Precio Mercado Diario - Precio RT Fase 2)* x *Energía Casada RT Fase 2 a bajar*")
            
            # Crear dataframe para la tabla
            df_beneficio = pd.DataFrame({x_col: df_pivot_precios.index, 'Beneficio': beneficio.values})
            
            if not perfil_24h:
                df_beneficio[x_col] = df_beneficio[x_col].dt.strftime('%Y-%m-%d %H:%M')
                
            # Transponer para que sea una sola fila con los periodos en las columnas
            df_beneficio_row = df_beneficio.set_index(x_col).T
            df_beneficio_row.index = ["Potencial beneficio RT fase 2 a bajar (€)"]
            
            # Mostrar la tabla redondeada a 2 decimales para que se vea bonita
            st.dataframe(df_beneficio_row.fillna(0).round(2), use_container_width=True)
            st.markdown("---")

        # --- GRÁFICO 1: PRECIOS (Restaurado a la normalidad sin el spread) ---
        if df_final_precios is not None:
            fig_precios = px.line(
                df_final_precios, x=x_col, y='value', color='Indicador', 
                title='Evolución de Precios (€/MWh)', 
                template='plotly_white',
                markers=True if perfil_24h or (freq and freq != 'h') else False
            )
            fig_precios.update_xaxes(title_text='Hora del día' if perfil_24h else 'Fecha')
            fig_precios.update_yaxes(title_text='Precio (€/MWh)')
            if perfil_24h: fig_precios.update_xaxes(tickmode='linear', dtick=1)
            st.plotly_chart(fig_precios, use_container_width=True)
            
        st.markdown("---")

        # --- GRÁFICO 2: ENERGÍA (Y SPREAD EN EJE SECUNDARIO) ---
        if df_final_energia is not None:
            fig_energia = make_subplots(specs=[[{"secondary_y": True}]])
            colores = px.colors.qualitative.Plotly
            
            # 1. Añadimos las líneas de energía al eje primario (Izquierda)
            for i, indicador in enumerate(df_final_energia['Indicador'].unique()):
                df_filtro = df_final_energia[df_final_energia['Indicador'] == indicador]
                fig_energia.add_trace(
                    go.Scatter(
                        x=df_filtro[x_col], 
                        y=df_filtro['value'], 
                        name=indicador,
                        mode='lines+markers' if perfil_24h or (freq and freq != 'h') else 'lines',
                        line=dict(color=colores[i % len(colores)])
                    ),
                    secondary_y=False
                )
            
            # 2. Añadimos la línea del SPREAD al eje secundario (Derecha)
            if spread is not None:
                fig_energia.add_trace(
                    go.Scatter(
                        x=df_pivot_precios.index,
                        y=spread.values,
                        name='Diferencia de Precio (Diario - RT2)',
                        mode='lines+markers' if perfil_24h or (freq and freq != 'h') else 'lines',
                        line=dict(color='rgba(150, 150, 150, 0.6)', dash='dash')
                    ),
                    secondary_y=True
                )
                
            fig_energia.update_layout(title_text='Evolución de Energía y Diferencial de Precio', template='plotly_white', hovermode="x unified")
            fig_energia.update_xaxes(title_text='Hora del día' if perfil_24h else 'Fecha')
            fig_energia.update_yaxes(title_text="Energía (MWh)", secondary_y=False)
            fig_energia.update_yaxes(title_text="Diferencial de Precio (€/MWh)", secondary_y=True, showgrid=False)
            
            if perfil_24h: fig_energia.update_xaxes(tickmode='linear', dtick=1)
            st.plotly_chart(fig_energia, use_container_width=True)

        st.markdown("---")
        
        # --- GRÁFICO 3: SECUNDARIA ---
        if df_final_secundaria is not None:
            fig_secundaria = make_subplots(specs=[[{"secondary_y": True}]])
            
            for indicador in df_final_secundaria['Indicador'].unique():
                df_filtro = df_final_secundaria[df_final_secundaria['Indicador'] == indicador]
                es_banda = "banda" in indicador.lower() 
                fig_secundaria.add_trace(go.Scatter(x=df_filtro[x_col], y=df_filtro['value'], name=indicador, mode='lines'), secondary_y=es_banda)

            fig_secundaria.update_layout(title_text='Precios Banda y Energía Secundaria', template='plotly_white', hovermode="x unified")
            fig_secundaria.update_yaxes(title_text="Precio Energía (€/MWh)", secondary_y=False)
            fig_secundaria.update_yaxes(title_text="Precio Banda (€/MW)", secondary_y=True, showgrid=False)
            if perfil_24h: fig_secundaria.update_xaxes(tickmode='linear', dtick=1)
            st.plotly_chart(fig_secundaria, use_container_width=True)

        if lista_dfs_agrupados:
            st.markdown("---")
            st.subheader("📑 Tabla Consolidada")
            df_total = pd.concat(lista_dfs_agrupados, ignore_index=True)
            df_pivot = df_total.pivot_table(index=x_col, columns='Indicador', values='value', aggfunc='first').reset_index()
            df_pivot = df_pivot.sort_values(by=x_col).reset_index(drop=True)
            if not perfil_24h:
                df_pivot[x_col] = df_pivot[x_col].dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(df_pivot, use_container_width=True)

# ==============================================================================
# PÁGINA 2: PRECIOS DE CAPTURA RENOVABLES
# ==============================================================================
def pagina_renovables(start_date, end_date):
    st.subheader("☀️🌪️ Análisis de Precios de Captura y Apuntamiento")
    st.markdown("Cálculo de ingresos ponderados cruzando el perfil de generación horario real de REE con el mercado diario de OMIE.")
    
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    if st.button("🚀 Iniciar Cálculo Analítico"):
        with st.spinner("Descargando e integrando datos históricos..."):
            
            df_precio = obtener_datos_batched(600, start_dt, end_dt, 3)
            df_eolica = obtener_datos_batched(551, start_dt, end_dt, 8741)
            df_solar = obtener_datos_batched(1295, start_dt, end_dt, 8741)
            
            if df_precio.empty or df_eolica.empty or df_solar.empty:
                st.error("Error: Faltan datos para procesar el periodo.")
                return

            df_precio_h = df_precio.resample('1h').mean().rename(columns={'value': 'Precio_Spot'})
            df_eolica_h = df_eolica.resample('1h').mean().rename(columns={'value': 'Gen_Eolica'})
            df_solar_h = df_solar.resample('1h').mean().rename(columns={'value': 'Gen_Solar'})
            
            df_master = df_precio_h.join(df_eolica_h, how='inner').join(df_solar_h, how='inner').fillna(0)
            
            if df_master.empty:
                st.warning("No hay suficientes datos cruzados para calcular.")
                return
            
            # --- MATEMÁTICAS ---
            precio_medio_spot = df_master['Precio_Spot'].mean()
            
            vol_solar = df_master['Gen_Solar'].sum()
            ingresos_solar = (df_master['Gen_Solar'] * df_master['Precio_Spot']).sum()
            precio_cap_solar = ingresos_solar / vol_solar if vol_solar > 0 else 0
            apunt_solar = precio_cap_solar / precio_medio_spot if precio_medio_spot > 0 else 0
            
            vol_eolica = df_master['Gen_Eolica'].sum()
            ingresos_eolica = (df_master['Gen_Eolica'] * df_master['Precio_Spot']).sum()
            precio_cap_eolica = ingresos_eolica / vol_eolica if vol_eolica > 0 else 0
            apunt_eolica = precio_cap_eolica / precio_medio_spot if precio_medio_spot > 0 else 0
            
            # --- RESULTADOS MÉTRICAS ---
            st.markdown("### 🏆 Resultados Resumen del Periodo")
            col1, col2, col3 = st.columns(3)
            
            col1.metric("Precio Medio Spot (Aritmético)", f"{precio_medio_spot:.2f} €/MWh")
            
            col2.metric("Precio Captura Solar", f"{precio_cap_solar:.2f} €/MWh", f"Apuntamiento: {apunt_solar*100:.1f}%")
            st.markdown(f"<div style='text-align: center; color: gray; font-size: 0.9em;'>Energía procesada: {vol_solar/1000000:.2f} TWh</div>", unsafe_allow_html=True)
            
            col3.metric("Precio Captura Eólica", f"{precio_cap_eolica:.2f} €/MWh", f"Apuntamiento: {apunt_eolica*100:.1f}%")
            st.markdown(f"<div style='text-align: center; color: gray; font-size: 0.9em;'>Energía procesada: {vol_eolica/1000000:.2f} TWh</div>", unsafe_allow_html=True)

            # --- GRÁFICA RENOVABLES VS PRECIO ---
            st.markdown("---")
            st.subheader("Curva de Generación vs Precio (Resolución Horaria)")
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig.add_trace(go.Scatter(x=df_master.index, y=df_master['Precio_Spot'], name="Precio Spot", line=dict(color='black', width=1)), secondary_y=False)
            fig.add_trace(go.Scatter(x=df_master.index, y=df_master['Gen_Eolica'], name="Generación Eólica", fill='tozeroy', line=dict(color='green', width=0), opacity=0.3), secondary_y=True)
            fig.add_trace(go.Scatter(x=df_master.index, y=df_master['Gen_Solar'], name="Generación Solar", fill='tozeroy', line=dict(color='orange', width=0), opacity=0.3), secondary_y=True)
            
            fig.update_layout(template="plotly_white", hovermode="x unified", height=550)
            fig.update_xaxes(title_text='Fecha y Hora')
            fig.update_yaxes(title_text="Precio Spot (€/MWh)", secondary_y=False)
            fig.update_yaxes(title_text="Generación Peninsular (MWh)", secondary_y=True, showgrid=False)
            
            st.plotly_chart(fig, use_container_width=True)

# ==============================================================================
# PÁGINA 3: PRODUCCIÓN VS ESTIMACIÓN
# ==============================================================================
def pagina_estimaciones(start_date, end_date):
    st.subheader("🔮 Producción Renovable: Real vs Estimación")
    st.markdown("Combina los datos reales de generación con la estimación de REE para el futuro (líneas discontinuas).")
    
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    with st.spinner("Descargando series reales y previsiones..."):
        df_eol_real = obtener_datos_batched(551, start_dt, end_dt, 8741)
        df_sol_real = obtener_datos_batched(1295, start_dt, end_dt, 8741)
        
        df_eol_est = obtener_datos_batched(1777, start_dt, end_dt, 8741)
        df_sol_est = obtener_datos_batched(1779, start_dt, end_dt, 8741)
        
        max_dt_eol = df_eol_real.index.max() if not df_eol_real.empty else start_dt
        max_dt_sol = df_sol_real.index.max() if not df_sol_real.empty else start_dt
        
        df_eol_est_futuro = df_eol_est[df_eol_est.index > max_dt_eol] if not df_eol_est.empty else pd.DataFrame()
        df_sol_est_futuro = df_sol_est[df_sol_est.index > max_dt_sol] if not df_sol_est.empty else pd.DataFrame()
        
        if not df_eol_real.empty and not df_eol_est_futuro.empty:
            df_eol_est_futuro = pd.concat([df_eol_real.iloc[[-1]], df_eol_est_futuro])
        if not df_sol_real.empty and not df_sol_est_futuro.empty:
            df_sol_est_futuro = pd.concat([df_sol_real.iloc[[-1]], df_sol_est_futuro])

        def formatear(df, nombre_indicador):
            if df.empty: return pd.DataFrame()
            df_temp = df.reset_index()
            df_temp['Indicador'] = nombre_indicador
            return df_temp

        df_plot = pd.concat([
            formatear(df_eol_real, 'Eólica (Real)'),
            formatear(df_eol_est_futuro, 'Eólica (Estimación)'),
            formatear(df_sol_real, 'Solar Fotovoltaica (Real)'),
            formatear(df_sol_est_futuro, 'Solar Fotovoltaica (Estimación)')
        ])
        
        if not df_plot.empty:
            fig = px.line(
                df_plot, 
                x='datetime', 
                y='value', 
                color='Indicador',
                title="Evolución y Previsión de Energías Renovables (MWh)",
                color_discrete_map={
                    'Eólica (Real)': 'green', 
                    'Eólica (Estimación)': 'green',
                    'Solar Fotovoltaica (Real)': 'orange', 
                    'Solar Fotovoltaica (Estimación)': 'orange'
                },
                line_dash='Indicador',
                line_dash_map={
                    'Eólica (Real)': 'solid', 
                    'Eólica (Estimación)': 'dash',
                    'Solar Fotovoltaica (Real)': 'solid', 
                    'Solar Fotovoltaica (Estimación)': 'dash'
                },
                template='plotly_white'
            )
            fig.update_xaxes(title_text='Fecha y Hora')
            fig.update_yaxes(title_text='Generación (MWh)')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos disponibles para el rango seleccionado.")

# ==============================================================================
# EJECUCIÓN DEL CONTROLADOR PRINCIPAL
# ==============================================================================
if TOKEN_ESIOS and TOKEN_ESIOS != "TU_TOKEN_AQUI" and len(fechas) == 2:
    start_date, end_date = fechas
    
    if seccion == "Mercados de Ajuste":
        pagina_ajustes(start_date, end_date)
    elif seccion == "Precios de Captura Renovables":
        pagina_renovables(start_date, end_date)
    elif seccion == "Producción vs Estimación":
        pagina_estimaciones(start_date, end_date)
else:
    st.info("👈 Por favor, verifica la configuración del Token ESIOS y selecciona un rango de fechas válido.")