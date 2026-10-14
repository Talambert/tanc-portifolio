import streamlit as st

from portal import ui

ui.init()
settings = st.session_state.settings

st.title("⚙️ Configurações")
st.caption("Parâmetros do ambiente e limites de custo.")
ui.banner()

st.toggle("Modo simulação", value=settings.simulation, disabled=True,
          help="O modo real (Dataplex, BigQuery e Vertex AI) ainda não foi implementado.")
st.caption("O modo real entra nas próximas fases do plano. Por enquanto o portal roda só em simulação.")

with st.form("config"):
    st.subheader("Ambiente")
    c1, c2 = st.columns(2)
    project_id = c1.text_input("Projeto GCP", value=settings.project_id)
    region = c2.text_input("Região", value=settings.region)
    model = st.text_input("Modelo (Vertex AI)", value=settings.model,
                          help="Configurável: os IDs dos modelos mudam com o tempo, confirme na documentação do Vertex AI.")

    st.subheader("Limites de custo e de amostragem")
    max_rows = st.slider("Linhas de amostra enviadas ao modelo", 1, 20, settings.max_sample_rows)
    max_gb = st.number_input("Máximo de GB processados por execução", min_value=0.1, max_value=100.0,
                             value=float(settings.max_gb_per_run), step=0.1)
    if st.form_submit_button("Salvar", type="primary"):
        settings.project_id, settings.region, settings.model = project_id, region, model
        settings.max_sample_rows, settings.max_gb_per_run = max_rows, max_gb
        st.success("Configurações salvas nesta sessão.")

st.subheader("Segurança")
st.markdown(
    "- Dados sensíveis são **mascarados antes** de qualquer envio ao modelo.\n"
    "- Nada é gravado ou criado sem aprovação.\n"
    "- Toda gravação fica na **trilha de auditoria** e pode ser desfeita."
)
