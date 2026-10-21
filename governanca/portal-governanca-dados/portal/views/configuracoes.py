import streamlit as st

from governanca.core import errors
from portal import ui

ui.init()
settings = st.session_state.settings

ui.page_header("Configurações", "Parâmetros do ambiente e limites de custo.")

st.toggle("Modo simulação", value=settings.simulation, disabled=True,
          help="O modo real (Dataplex, BigQuery e Vertex AI) ainda não foi implementado.")
st.caption("O modo real entra nas próximas fases do plano. Por enquanto o portal roda só em simulação.")

with st.form("config"):
    st.header("Ambiente", anchor=False)
    c1, c2 = st.columns(2)
    project_id = c1.text_input("Projeto GCP", value=settings.project_id)
    region = c2.text_input("Região", value=settings.region)
    model = st.text_input("Modelo (Vertex AI)", value=settings.model,
                          help="Configurável: os IDs dos modelos mudam com o tempo, confirme na documentação do Vertex AI.")

    st.header("Limites de custo e de amostragem", anchor=False)
    max_rows = st.slider("Linhas de amostra enviadas ao modelo", 1, 20, settings.max_sample_rows)
    max_gb = st.number_input("Máximo de GB processados por execução", min_value=0.1, max_value=100.0,
                             value=float(settings.max_gb_per_run), step=0.1)
    if st.form_submit_button("Salvar configurações", type="primary", icon=":material/save:"):
        settings.project_id, settings.region, settings.model = project_id, region, model
        settings.max_sample_rows, settings.max_gb_per_run = max_rows, max_gb
        st.success("Configurações salvas nesta sessão.", icon=":material/check_circle:")

st.header("Segurança", anchor=False)
st.markdown(
    "- Dados sensíveis são **mascarados antes** de qualquer envio ao modelo.\n"
    "- Nada é gravado ou criado sem aprovação.\n"
    "- Toda gravação fica na **trilha de auditoria** e pode ser desfeita."
)

st.header("Demonstração", anchor=False)
fault = st.selectbox("Simular falha do serviço", list(errors.SIMULATED_FAULTS), index=list(errors.SIMULATED_FAULTS).index(settings.simulated_fault),
                     help="Provoca uma falha na próxima operação (gerar sugestões, profiling, propor regras, gravar, publicar, "
                          "responder) para você ver como o portal avisa e orienta. Nada fica pela metade.")
settings.simulated_fault = fault
if fault != "Nenhuma":
    st.warning(f"Falha simulada ativa: **{fault}**. As próximas operações vão falhar até você voltar para \"Nenhuma\".",
               icon=":material/science:")
st.caption("As aprovações, a auditoria e os scans publicados sobrevivem ao recarregamento da página. "
           "Para recomeçar a demonstração do zero:")
if st.button("Restaurar demonstração", icon=":material/restart_alt:", width="content"):
    ui.reset_demo()
    st.rerun()
