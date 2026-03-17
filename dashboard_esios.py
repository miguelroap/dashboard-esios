import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
try:
    TOKEN_ESIOS = st.secrets["ESIOS_TOKEN"]
except FileNotFoundError:
    st.error("⚠️ No se ha encontrado el token en los secretos de Streamlit.")
    TOKEN_ESIOS = ""



st.set_page_config(page_title="Dashboard ESIOS", layout="wide")
st.title("⚡ Dashboard de Mercado Eléctrico (ESIOS)")

st.sidebar.header("Configuración")

# Fechas por defecto: últimos 7 días
hoy = datetime.now()
hace_7_dias = hoy - timedelta(days=7)

fechas = st.sidebar.date_input(
    "Selecciona el periodo:",
    value=(hace_7_dias, hoy),
    max_value=hoy
)

agrupacion = st.sidebar.selectbox(
    "Agrupación temporal:",
    ("Cuartohorario", "Horario", "Diario", "Mensual", "Anual")
)

# NUEVO: Toggle de Perfil 24h
perfil_24h = st.sidebar.checkbox("Perfil 24h", value=False)

frecuencias = {
    "Cuartohorario": None,
    "Horario": "h",
    "Diario": "D",
    "Mensual": "MS",
    "Anual": "YS"
}
freq = frecuencias[agrupacion]

# Variable para saber qué columna usar en el eje X
x_col = 'Hora' if perfil_24h else 'datetime'

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

# --- FUNCIONES ---
@st.cache_data(ttl=3600)
def obtener_datos_esios(indicator_id, nombre_indicador, start_date, end_date, api_token):
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    
    headers = {
        'x-api-key': api_token,
        'Content-Type': 'application/json'
    }
    
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
                if str(indicator_id) == "600":
                    df = df[df['geo_id'] == 3]     
                else:
                    df = df[df['geo_id'] == 8741]  
            
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df['indicator_id'] = str(indicator_id)
                df['Indicador'] = nombre_indicador
                return df[['datetime', 'value', 'Indicador']]
    else:
        st.sidebar.error(f"Error al obtener el indicador {indicator_id}: Código {response.status_code}")
    
    return pd.DataFrame()

def agrupar_datos(df, frecuencia, tipo):
    if frecuencia is None or df.empty:
        return df 
    operacion = 'mean' if tipo == 'precio' else 'sum'
    return df.groupby([pd.Grouper(key='datetime', freq=frecuencia), 'Indicador'])['value'].agg(operacion).reset_index()

def generar_perfil(df):
    """Calcula el perfil medio de 24h para el periodo seleccionado"""
    if df.empty: return df
    df_local = df.copy()
    # Convertimos a la zona horaria de España para que la Hora 0 sea correcta
    df_local['datetime'] = df_local['datetime'].dt.tz_convert('Europe/Madrid')
    df_local['Hora'] = df_local['datetime'].dt.hour
    # Promediamos el valor de cada hora a través de todos los días
    return df_local.groupby(['Hora', 'Indicador'])['value'].mean().reset_index()

# --- LÓGICA PRINCIPAL ---
if TOKEN_ESIOS != "TU_TOKEN_AQUI" and len(fechas) == 2:
    start_date, end_date = fechas
    
    with st.spinner('Obteniendo y procesando datos...'):
        
        dfs_precios = [obtener_datos_esios(ind_id, nombre, start_date, end_date, TOKEN_ESIOS) for ind_id, nombre in indicadores_precio.items()]
        dfs_precios = [df for df in dfs_precios if not df.empty]
        
        dfs_energia = [obtener_datos_esios(ind_id, nombre, start_date, end_date, TOKEN_ESIOS) for ind_id, nombre in indicadores_energia.items()]
        dfs_energia = [df for df in dfs_energia if not df.empty]
        
        dfs_secundaria = [obtener_datos_esios(ind_id, nombre, start_date, end_date, TOKEN_ESIOS) for ind_id, nombre in indicadores_secundaria.items()]
        dfs_secundaria = [df for df in dfs_secundaria if not df.empty]
        
        titulo_fecha = f"Datos desde {start_date} hasta {end_date}"
        st.subheader(f"{titulo_fecha} {'(Perfil Medio 24h)' if perfil_24h else f'(Agrupación: {agrupacion})'}")
        
        lista_dfs_agrupados = []

        # --- GRÁFICO 1: PRECIOS ---
        if dfs_precios:
            df_final_precios = pd.concat(dfs_precios, ignore_index=True)
            if perfil_24h:
                df_final_precios = agrupar_datos(df_final_precios, 'h', 'precio') # Unificamos a hora primero
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
            if perfil_24h: fig_precios.update_xaxes(tickmode='linear', dtick=1) # Fuerza a mostrar las 24 horas
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
            
            # Usamos Graph Objects para crear el doble eje
            fig_secundaria = make_subplots(specs=[[{"secondary_y": True}]])
            
            for indicador in df_final_secundaria['Indicador'].unique():
                df_filtro = df_final_secundaria[df_final_secundaria['Indicador'] == indicador]
                es_banda = "banda" in indicador.lower() # Identificamos si es banda o energía
                
                fig_secundaria.add_trace(
                    go.Scatter(
                        x=df_filtro[x_col], 
                        y=df_filtro['value'], 
                        name=indicador,
                        mode='lines+markers' if perfil_24h or (freq and freq != 'h') else 'lines'
                    ),
                    secondary_y=es_banda # Si es banda, se va al eje secundario
                )

            # Configuramos los títulos y los ejes
            fig_secundaria.update_layout(title_text='Precios Banda y Energía Secundaria', template='plotly_white')
            fig_secundaria.update_xaxes(title_text='Hora del día' if perfil_24h else 'Fecha')
            if perfil_24h: fig_secundaria.update_xaxes(tickmode='linear', dtick=1)
            
            # Ejes Y
            fig_secundaria.update_yaxes(title_text="Precio Energía (€/MWh)", secondary_y=False)
            fig_secundaria.update_yaxes(title_text="Precio Banda (€/MW)", secondary_y=True, showgrid=False)
            
            st.plotly_chart(fig_secundaria, use_container_width=True)

        st.markdown("---")

        # --- TABLA CONSOLIDADA ---
        if lista_dfs_agrupados:
            st.subheader("📊 Tabla Consolidada de Indicadores")
            
            df_total = pd.concat(lista_dfs_agrupados, ignore_index=True)
            
            df_pivot = df_total.pivot_table(
                index=x_col, 
                columns='Indicador', 
                values='value',
                aggfunc='first' 
            ).reset_index()
            
            df_pivot = df_pivot.sort_values(by=x_col).reset_index(drop=True)
            
            # Si NO estamos en Perfil 24h, formateamos la fecha para que salga limpia en huso horario español
            if not perfil_24h:
                df_pivot[x_col] = df_pivot[x_col].dt.tz_convert('Europe/Madrid').dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(df_pivot, use_container_width=True)

elif TOKEN_ESIOS == "TU_TOKEN_AQUI":
    st.error("⚠️ Por favor, edita el archivo Python y pon tu token real en la variable TOKEN_ESIOS.")
elif len(fechas) != 2:
    st.info("Por favor, selecciona una fecha de inicio y una de fin en la barra lateral.")