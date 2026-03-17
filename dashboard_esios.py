import streamlit as st
import requests
import pandas as pd
import plotly.express as px
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

# Diccionarios con los indicadores separados por tipo de gráfico
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

# --- FUNCIÓN PARA LLAMAR A LA API ---
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
            
            # Filtro de geolocalización dinámico
            if not df.empty and 'geo_id' in df.columns:
                if str(indicator_id) == "600":
                    df = df[df['geo_id'] == 3]     # Nacional / España
                else:
                    df = df[df['geo_id'] == 8741]  # Sistema Peninsular
            
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['indicator_id'] = str(indicator_id)
                df['Indicador'] = nombre_indicador
                return df[['datetime', 'value', 'Indicador']]
    else:
        st.sidebar.error(f"Error al obtener el indicador {indicator_id}: Código {response.status_code}")
    
    return pd.DataFrame()

# --- LÓGICA PRINCIPAL ---
if TOKEN_ESIOS != "TU_TOKEN_AQUI" and len(fechas) == 2:
    start_date, end_date = fechas
    
    with st.spinner('Obteniendo datos de ESIOS...'):
        
        # 1. Obtener datos de Precios
        dfs_precios = []
        for ind_id, nombre in indicadores_precio.items():
            df_temp = obtener_datos_esios(ind_id, nombre, start_date, end_date, TOKEN_ESIOS)
            if not df_temp.empty:
                dfs_precios.append(df_temp)
                
        # 2. Obtener datos de Energía
        dfs_energia = []
        for ind_id, nombre in indicadores_energia.items():
            df_temp = obtener_datos_esios(ind_id, nombre, start_date, end_date, TOKEN_ESIOS)
            if not df_temp.empty:
                dfs_energia.append(df_temp)
        
        st.subheader(f"Datos desde {start_date} hasta {end_date}")
        
        # --- GRÁFICO 1: PRECIOS ---
        if dfs_precios:
            df_final_precios = pd.concat(dfs_precios, ignore_index=True)
            fig_precios = px.line(
                df_final_precios, 
                x='datetime', 
                y='value', 
                color='Indicador',
                title='Evolución de Precios (€/MWh)',
                labels={'datetime': 'Fecha y Hora', 'value': 'Precio (€/MWh)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_precios, use_container_width=True)
        else:
            st.warning("No se encontraron datos de precios para este periodo.")

        st.markdown("---") # Línea separadora visual

        # --- GRÁFICO 2: ENERGÍA ---
        if dfs_energia:
            df_final_energia = pd.concat(dfs_energia, ignore_index=True)
            fig_energia = px.line(
                df_final_energia, 
                x='datetime', 
                y='value', 
                color='Indicador',
                title='Evolución de Energía (MWh)',
                labels={'datetime': 'Fecha y Hora', 'value': 'Energía (MWh)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_energia, use_container_width=True)
        else:
            st.warning("No se encontraron datos de energía para este periodo.")

elif TOKEN_ESIOS == "TU_TOKEN_AQUI":
    st.error("⚠️ Por favor, edita el archivo Python y pon tu token real en la variable TOKEN_ESIOS.")
elif len(fechas) != 2:
    st.info("Por favor, selecciona una fecha de inicio y una de fin en la barra lateral.")