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
    # ⚠️ Si estás probando en local y no has creado la carpeta .streamlit, pon tu token aquí:
    TOKEN_ESIOS = "TU_TOKEN_AQUI" 

st.set_page_config(page_title="Dashboard Analítico ESIOS", layout="wide")
st.title("⚡ Dashboard Analítico de Mercado (ESIOS)")

# --- BARRA LATERAL: NAVEGACIÓN Y FILTROS ---
st.sidebar.header("Navegación")
seccion = st.sidebar.radio(
    "Selecciona el módulo:",
    ("Mercados de Ajuste", "Precios de Captura Renovables")
)

st.sidebar.markdown("---")
st.sidebar.header("Filtros Globales")

# Fechas por defecto: últimos 7 días
hoy = datetime.now()
hace_7_dias = hoy - timedelta(days=7)
fechas = st.sidebar.date_input("Selecciona el periodo:", value=(hace_7_dias, hoy), max_value=hoy)

# --- DICCIONARIOS DE INDICADORES (MERCADO DE AJUSTE) ---
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
    """Descarga datos estándar para la sección de Mercados de Ajuste."""
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
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df['indicator_id'] = str(indicator_id)
                df['Indicador'] = nombre_indicador
                return df[['datetime', 'value', 'Indicador']]
    else:
        st.sidebar.error(f"Error al obtener el indicador {indicator_id}: Código {response.status_code}")
    
    return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def obtener_datos_batched(indicator_id, start_dt, end_dt, specific_geo):
    """Descarga por lotes (mes a mes) para indicadores pesados (Sección Renovables)."""
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
        except Exception as e:
            pass # Si hay un timeout en un chunk, continúa con el siguiente
            
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
    df_local['datetime'] = df_local['datetime'].dt.tz_convert('Europe/Madrid')
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
        
        # Obtener datos de las tres categorías
        dfs_precios = [obtener_datos_simples(i, n, start_date, end_date) for i, n in indicadores_precio.items()]
        dfs_precios = [df for df in dfs_precios if not df.empty]
        
        dfs_energia = [obtener_datos_simples(i, n, start_date, end_date) for i, n in indicadores_energia.items()]
        dfs_energia = [df for df in dfs_energia if not df.empty]
        
        dfs_secundaria = [obtener_datos_simples(i, n, start_date, end_date) for i, n in indicadores_secundaria.items()]
        dfs_secundaria = [df for df in dfs_secundaria if not df.empty]
        
        st.markdown(f"***Mostrando datos {'(Perfil Medio 24h)' if perfil_24h else f'(Agrupación: {agrupacion})'}***")
        lista_dfs_agrupados = []

        # --- GRÁFICO 1: PRECIOS ---
        if dfs_precios:
            df_final_precios = pd.concat(dfs_precios, ignore_index=True)
            if perfil_24h:
                df_final_precios = agrupar_datos(df_final_precios, 'h', 'precio')
                df_final_precios = generar_perfil(df_final_precios)
            else:
                df_final_precios = agrupar_datos(df_final_precios, freq, 'precio')
                
            lista_dfs_agrupados.append(df_final_precios)
            
            fig_precios = px.line(
                df_final_precios, x=x_col, y='value', color='Indicador',
                title='Evolución de Precios (€/MWh)',
                labels={x_col: 'Hora del día' if perfil_24h else 'Fecha', 'value': 'Precio (€/MWh)'},
                template='plotly_white', markers=True if perfil_24h or (freq and freq != 'h') else False
            )
            if perfil_24h: fig_precios.update_xaxes(tickmode='linear', dtick=1)
            st.plotly_chart(fig_precios, use_container_width=True)

        st.markdown("---")

        # --- GRÁFICO 2: ENERGÍA ---
        if dfs_energia:
            df_final_energia = pd.concat(dfs_energia, ignore_index=True)
            if perfil_24h:
                df_final_energia = agrupar_datos(df_final_energia, 'h', 'energia')
                df_final_energia = generar_perfil(df_final_energia)
            else:
                df_final_energia = agrupar_datos(df_final_energia, freq, 'energia')
                
            lista_dfs_agrupados.append(df_final_energia)
            
            fig_energia = px.line(
                df_final_energia, x=x_col, y='value', color='Indicador',
                title='Evolución de Energía (MWh)',
                labels={x_col: 'Hora del día' if perfil_24h else 'Fecha', 'value': 'Energía (MWh)'},
                template='plotly_white', markers=True if perfil_24h or (freq and freq != 'h') else False
            )
            if perfil_24h: fig_energia.update_xaxes(tickmode='linear', dtick=1)
            st.plotly_chart(fig_energia, use_container_width=True)

        st.markdown("---")
        
        # --- GRÁFICO 3: SECUNDARIA (DOBLE EJE Y) ---
        if dfs_secundaria:
            df_final_secundaria = pd.concat(dfs_secundaria, ignore_index=True)
            if perfil_24h:
                df_final_secundaria = agrupar_datos(df_final_secundaria, 'h', 'precio')
                df_final_secundaria = generar_perfil(df_final_secundaria)
            else:
                df_final_secundaria = agrupar_datos(df_final_secundaria, freq, 'precio')
                
            lista_dfs_agrupados.append(df_final_secundaria)
            
            fig_secundaria = make_subplots(specs=[[{"secondary_y": True}]])
            
            for indicador in df_final_secundaria['Indicador'].unique():
                df_filtro = df_final_secundaria[df_final_secundaria['Indicador'] == indicador]
                es_banda = "banda" in indicador.lower() 
                
                fig_secundaria.add_trace(
                    go.Scatter(
                        x=df_filtro[x_col], y=df_filtro['value'], name=indicador,
                        mode='lines+markers' if perfil_24h or (freq and freq != 'h') else 'lines'
                    ),
                    secondary_y=es_banda
                )

            fig_secundaria.update_layout(title_text='Precios Banda y Energía Secundaria', template='plotly_white')
            fig_secundaria.update_xaxes(title_text='Hora del día' if perfil_24h else 'Fecha')
            if perfil_24h: fig_secundaria.update_xaxes(tickmode='linear', dtick=1)
            fig_secundaria.update_yaxes(title_text="Precio Energía (€/MWh)", secondary_y=False)
            fig_secundaria.update_yaxes(title_text="Precio Banda (€/MW)", secondary_y=True, showgrid=False)
            
            st.plotly_chart(fig_secundaria, use_container_width=True)

        # --- TABLA CONSOLIDADA PIVOTANTE ---
        if lista_dfs_agrupados:
            st.markdown("---")
            st.subheader("📑 Tabla Consolidada de Indicadores")
            
            df_total = pd.concat(lista_dfs_agrupados, ignore_index=True)
            df_pivot = df_total.pivot_table(index=x_col, columns='Indicador', values='value', aggfunc='first').reset_index()
            df_pivot = df_pivot.sort_values(by=x_col).reset_index(drop=True)
            
            if not perfil_24h:
                df_pivot[x_col] = df_pivot[x_col].dt.tz_convert('Europe/Madrid').dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(df_pivot, use_container_width=True)


# ==============================================================================
# PÁGINA 2: PRECIOS DE CAPTURA RENOVABLES
# ==============================================================================
def pagina_renovables(start_date, end_date):
    st.subheader("☀️🌪️ Análisis de Precios de Captura y Apuntamiento")
    st.markdown("Cálculo de ingresos ponderados cruzando el perfil de generación horario real de REE con el mercado diario de OMIE.")
    
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    if st.button("🚀 Iniciar Cálculo Analítico (Puede tardar unos segundos)"):
        with st.spinner("Descargando e integrando datos históricos..."):
            
            df_precio = obtener_datos_batched(600, start_dt, end_dt, 3)
            df_eolica = obtener_datos_batched(551, start_dt, end_dt, 8741)
            df_solar = obtener_datos_batched(1295, start_dt, end_dt, 8741)
            
            if df_precio.empty or df_eolica.empty or df_solar.empty:
                st.error("Error: Faltan datos para procesar el periodo en la API de ESIOS.")
                return

            df_precio_h = df_precio.resample('1h').mean().rename(columns={'value': 'Precio_Spot'})
            df_eolica_h = df_eolica.resample('1h').mean().rename(columns={'value': 'Gen_Eolica'})
            df_solar_h = df_solar.resample('1h').mean().rename(columns={'value': 'Gen_Solar'})
            
            df_master = df_precio_h.join(df_eolica_h, how='inner').join(df_solar_h, how='inner').fillna(0)
            
            if df_master.empty:
                st.warning("No hay suficientes datos cruzados (solapamiento temporal) para calcular.")
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
# EJECUCIÓN DEL CONTROLADOR PRINCIPAL
# ==============================================================================
if TOKEN_ESIOS and TOKEN_ESIOS != "TU_TOKEN_AQUI" and len(fechas) == 2:
    start_date, end_date = fechas
    
    if seccion == "Mercados de Ajuste":
        pagina_ajustes(start_date, end_date)
    elif seccion == "Precios de Captura Renovables":
        pagina_renovables(start_date, end_date)
else:
    st.info("👈 Por favor, verifica la configuración del Token ESIOS y selecciona un rango de fechas válido.")