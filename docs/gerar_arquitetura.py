"""Gera docs/arquitetura.svg: arquitetura do portfólio, do dado bruto à governança com IA.

Para atualizar o desenho (por exemplo, quando um projeto passar de "planejado" a "pronto",
troque o status da caixa correspondente: "pronto", "andamento", "planejado" ou "externo"):

    python docs/gerar_arquitetura.py > docs/arquitetura.svg

Para gerar o PNG usado no README (Edge, em Git Bash, a partir da raiz do repositório):

    "/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe" --headless --disable-gpu \
      --hide-scrollbars --force-device-scale-factor=2 --window-size=1800,1340 \
      --screenshot="$(pwd -W)/docs/arquitetura.png" "file:///$(pwd -W)/docs/arquitetura.svg"
"""
import sys
from xml.sax.saxutils import escape

W, H = 1800, 1340
FONT = "'Segoe UI', 'Helvetica Neue', Arial, sans-serif"
MONO = "'Cascadia Mono', Consolas, 'Courier New', monospace"
INK, MUTED = "#1F2A37", "#5B6775"

# status: (preenchimento, borda, tracejado, cor do nome do projeto)
STATUS = {
    "pronto": ("#E3F4E7", "#0A5C20", None, "#0A5C20"),
    "andamento": ("#FFF1D6", "#D98200", None, "#8A5200"),
    "planejado": ("#F1F2F4", "#7A828C", "7 5", "#4B5563"),
    "externo": ("#FFFFFF", "#9AA3AE", None, "#4B5563"),
}

out: list[str] = []


def add(s: str) -> None:
    out.append(s)


def text(x, y, s, size=15, weight="400", fill=INK, anchor="middle", style="normal", family=None):
    fam = f' font-family="{family}"' if family else ""
    add(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" '
        f'text-anchor="{anchor}" font-style="{style}"{fam}>{escape(s)}</text>')


def box(x, y, w, h, status, title, lines=(), tag=None, badge=None, project=None,
        title_size=17, line_size=14.5, rx=12):
    """Caixa com título, linhas de apoio e, no pé, o nome da pasta do projeto (fonte monoespaçada)."""
    fill, stroke, dash, project_color = STATUS[status]
    d = f' stroke-dasharray="{dash}"' if dash else ""
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="2"{d}/>')
    projects = [project] if isinstance(project, str) else list(project or [])
    rows = ([("tag", tag)] if tag else []) + [("title", title)] + [("line", l) for l in lines] + [("project", p) for p in projects]
    heights = {"tag": 22, "title": 26, "line": 21, "project": 19}
    total = sum(heights[k] for k, _ in rows)
    cy = y + (h - total) / 2 + 17
    for kind, content in rows:
        if kind == "tag":
            text(x + w / 2, cy - 2, content, size=12.5, weight="700", fill=stroke)
        elif kind == "title":
            text(x + w / 2, cy, content, size=title_size, weight="700")
        elif kind == "line":
            text(x + w / 2, cy, content, size=line_size, fill=MUTED)
        else:
            text(x + w / 2, cy + 1, content, size=11.5, weight="600", fill=project_color, family=MONO)
        cy += heights[kind]
    if badge:
        add(f'<circle cx="{x + 2}" cy="{y + 2}" r="15" fill="{INK}"/>')
        text(x + 2, y + 8, str(badge), size=16, weight="700", fill="#fff")


def arrow(points, dashed=False, color=INK, width=2.5):
    pts = " ".join(f"{px},{py}" for px, py in points)
    d = ' stroke-dasharray="7 6"' if dashed else ""
    marker = "arrowDash" if dashed else "arrow"
    add(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{width}"{d} '
        f'stroke-linejoin="round" marker-end="url(#{marker})"/>')


add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">')
add("<title>Arquitetura do portfólio de dados</title>")
add("<desc>Do arquivo cru da ANP ao painel e ao portal de governança com IA: ingestão para o Cloud Storage, "
    "processamento para a silver, transformação com dbt para a gold, consumo no Power BI e em um agente de dados, "
    "e um portal de governança que gera metadados, regras de data quality e serve um catálogo. "
    "Cada caixa indica a pasta do projeto no repositório.</desc>")
add('<defs>'
    f'<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{INK}"/></marker>'
    f'<marker id="arrowDash" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{MUTED}"/></marker>'
    '</defs>')
add(f'<rect width="{W}" height="{H}" fill="#FFFFFF"/>')

# ---------- título ----------
text(40, 52, "Arquitetura do portfólio: do dado bruto à governança com IA", size=30, weight="700", anchor="start")
text(40, 80, "Série de projetos de combustíveis (dados abertos da ANP) e portal de governança. As cores indicam o status de cada parte.",
     size=15.5, fill=MUTED, anchor="start")

PY = 245   # topo da linha do pipeline
TOP = 100  # topo da faixa superior
TOP_H = 96

# ---------- faixa superior: extração, Power BI, agente ----------
box(40, TOP, 430, TOP_H, "planejado", "Extração de documentos com IA",
    ["PDF e planilhas sem padrão → Gemini →", "validação em camadas → quarentena"],
    badge=1, project="extracao-documentos-ia")
box(1335, TOP, 200, TOP_H, "planejado", "Power BI", ["painel de preços", "e de bandeiras"],
    badge=4, project="combustiveis-bi-powerbi")
box(1550, TOP, 210, TOP_H, "planejado", "Agente de dados (Gemini)", ["perguntas em português"],
    badge=4, title_size=14, project=["combustiveis-agente-dados", "combustiveis-mcp-bigquery"])

# ---------- pipeline ----------
PIPE = "combustiveis-pipeline-gcp"
box(40, PY, 190, 72, "externo", "ANP (gov.br)", ["CSV e ZIP públicos"], badge=1)
box(40, PY + 88, 190, 82, "pronto", "Parceiros", ["upload pelo app"], badge=1, project="gcp-file-uploader")
box(275, PY, 190, 82, "andamento", "Download com retry", ["manifesto de URLs"], badge=1, title_size=15.5, project=PIPE)
box(520, PY, 200, 170, "planejado", "Cloud Storage", ["arquivos crus", "por semestre"], tag="BRONZE", badge=1, project=PIPE)
box(775, PY, 240, 82, "planejado", "Python", ["Cloud Run Jobs"], badge=2, project=PIPE)
box(775, PY + 88, 240, 82, "planejado", "PySpark", ["Dataproc Serverless"], badge=2, project=PIPE)
box(1070, PY, 220, 170, "planejado", "BigQuery", ["limpa e tipada", "quarentena das", "linhas malformadas"], tag="SILVER", badge=2, project=PIPE)
box(1335, PY + 35, 170, 100, "planejado", "dbt", ["staging → marts", "testes e docs"], badge=3, project="combustiveis-dbt")
box(1550, PY, 210, 170, "planejado", "BigQuery", ["fato + dimensões", "(esquema estrela)"], tag="GOLD", badge=3, project="combustiveis-dbt")
text(895, PY + 192, "mesmo contrato de dados, resultado comparado", size=13, fill=MUTED, style="italic")

arrow([(230, PY + 39), (275, PY + 39)])
arrow([(465, PY + 39), (520, PY + 39)])
arrow([(230, PY + 131), (520, PY + 131)])
arrow([(720, PY + 39), (775, PY + 39)])
arrow([(720, PY + 131), (775, PY + 131)])
arrow([(1015, PY + 39), (1070, PY + 39)])
arrow([(1015, PY + 131), (1070, PY + 131)])
arrow([(1290, PY + 85), (1335, PY + 85)])
arrow([(1505, PY + 85), (1550, PY + 85)])
arrow([(470, 150), (1180, 150), (1180, PY)])
text(820, 140, "carga validada (ou quarentena)", size=13, fill=MUTED, style="italic")
arrow([(1580, PY), (1580, 218), (1435, 218), (1435, TOP + TOP_H)], dashed=True, color=MUTED, width=2)
arrow([(1700, PY), (1700, TOP + TOP_H)], dashed=True, color=MUTED, width=2)

# ---------- orquestração e observabilidade ----------
BY, BH = 470, 120
add(f'<rect x="40" y="{BY}" width="1720" height="{BH}" rx="14" fill="#EEF3FB" stroke="#7C93BD" stroke-width="2"/>')
text(60, BY + 26, "ORQUESTRAÇÃO E OBSERVABILIDADE", size=14, weight="700", fill="#3D5A8A", anchor="start")
box(60, BY + 38, 210, 64, "planejado", "Airflow local (Docker)", title_size=15, project="combustiveis-orquestracao")
box(285, BY + 38, 210, 64, "planejado", "Cloud Composer (GCP)", title_size=15, project="combustiveis-orquestracao")
box(510, BY + 38, 610, 64, "planejado", "Agente de observabilidade (Gemini): diagnostica falhas e abre issue/PR",
    title_size=15, project="combustiveis-agente-observabilidade")
text(1425, BY + 62, "agenda e reexecuta (retry e backfill)", size=14, fill="#3D5A8A", style="italic")
text(1425, BY + 84, "e alerta todas as etapas acima", size=14, fill="#3D5A8A", style="italic")

# ---------- portal ----------
PT = 640  # topo do portal
add(f'<rect x="40" y="{PT}" width="1720" height="410" rx="16" fill="#FFF8EA" stroke="#D98200" stroke-width="2.5"/>')
add(f'<circle cx="42" cy="{PT + 2}" r="15" fill="#1F2A37"/>')
text(42, PT + 8, "5", size=16, weight="700", fill="#fff")
text(70, PT + 35, "PORTAL DE GOVERNANÇA DE DADOS", size=20, weight="700", anchor="start")
text(500, PT + 35, "portal-governanca-dados", size=14, weight="600", fill="#8A5200", anchor="start", family=MONO)
text(70, PT + 59, "Streamlit no Cloud Run · login Google (IAP) · biblioteca genérica, serve qualquer dataset do BigQuery · protótipo em simulação",
     size=14.5, fill=MUTED, anchor="start")

MX = (60, 630, 1200)
MY = PT + 80
box(MX[0], MY, 540, 150, "andamento", "Metadados",
    ["Amostra mascarada + metadados do Dataplex", "Gemini sugere descrições e classificação", "Revisão humana → grava no BigQuery e Dataplex"])
box(MX[1], MY, 540, 150, "andamento", "Data Quality",
    ["Profiling no Dataplex", "Gemini propõe regras, testadas em dado limpo", "Aprovação → YAML versionado e scans no Dataplex"])
box(MX[2], MY, 540, 150, "andamento", "Catálogo (RAG)",
    ["Pergunta em português → busca nas fontes", "Resposta com citação da fonte", "Sem fonte, responde \"não encontrei\""])

text(900, MY + 176, "Serviços Google Cloud usados pelos três módulos", size=13.5, fill=MUTED, style="italic")
box(MX[0], MY + 186, 540, 52, "externo", "Dataplex", ["catálogo, profiling e scans de qualidade"], title_size=15, line_size=13)
box(MX[1], MY + 186, 540, 52, "externo", "Vertex AI", ["Gemini e embeddings"], title_size=15, line_size=13)
box(MX[2], MY + 186, 540, 52, "externo", "BigQuery", ["dados, amostras e busca vetorial"], title_size=15, line_size=13)
add(f'<rect x="60" y="{MY + 262}" width="1680" height="46" rx="10" fill="#FFF1D6" stroke="#D98200" stroke-width="1.5"/>')
text(900, MY + 291, "Núcleo comum: mascaramento de dados sensíveis · auditoria com desfazer · aprovação humana antes de gravar · limites de custo",
     size=14.5, weight="600")

# leitura do portal sobre silver e gold
arrow([(1180, PY + 170), (1180, PT)], dashed=True, color=MUTED, width=2)
arrow([(1655, PY + 170), (1655, PT)], dashed=True, color=MUTED, width=2)
text(1195, PT - 20, "lê esquema, amostra mascarada e profiling", size=13.5, fill=MUTED, anchor="start", style="italic")
text(1668, PT - 20, "lê os marts", size=13.5, fill=MUTED, anchor="start", style="italic")

# ---------- plataforma de templates ----------
TY = 1075  # topo da faixa da plataforma
add(f'<rect x="40" y="{TY}" width="1720" height="140" rx="14" fill="#EEF6F0" stroke="#5F9A6E" stroke-width="2"/>')
text(60, TY + 26, "PLATAFORMA DE TEMPLATES: BASE REUTILIZÁVEL PARA NOVOS PIPELINES", size=14, weight="700", fill="#3F7450", anchor="start")
text(1740, TY + 26, "o template gera o repositório; a biblioteca valida, planeja e roda; a DAG vem do mesmo pipeline.yaml",
     size=13.5, fill="#3F7450", anchor="end", style="italic")
box(60, TY + 38, 420, 64, "andamento", "Template de pipeline (Cookiecutter)", title_size=15, project="template-pipeline")
box(690, TY + 38, 420, 64, "andamento", "Projeto gerado (tipo híbrido)", title_size=15, project=PIPE)
box(1320, TY + 38, 420, 64, "andamento", "Biblioteca dataeng-core", title_size=15, project="lib-dataeng-core")
arrow([(480, TY + 70), (690, TY + 70)])
text(585, TY + 62, "gera", size=13, fill=MUTED, style="italic")
arrow([(1320, TY + 70), (1110, TY + 70)])
text(1215, TY + 62, "usa", size=13, fill=MUTED, style="italic")
arrow([(270, TY + 102), (270, TY + 126), (1530, TY + 126), (1530, TY + 102)], dashed=True, color=MUTED, width=2)
text(900, TY + 120, "o template depende da biblioteca (referência por versão)", size=13, fill=MUTED, style="italic")

# ---------- legenda ----------
LY = 1265
x = 40
for status, label in (("pronto", "Pronto"), ("andamento", "Em andamento ou protótipo"), ("planejado", "Planejado"), ("externo", "Fonte ou serviço externo")):
    fill, stroke, dash, _ = STATUS[status]
    d = f' stroke-dasharray="{dash}"' if dash else ""
    add(f'<rect x="{x}" y="{LY - 14}" width="26" height="18" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="2"{d}/>')
    text(x + 36, LY, label, size=14.5, anchor="start")
    x += 36 + len(label) * 8.2 + 34
arrow([(x, LY - 5), (x + 46, LY - 5)])
text(x + 56, LY, "fluxo de dados", size=14.5, anchor="start")
x += 56 + 14 * 8.2 + 30
arrow([(x, LY - 5), (x + 46, LY - 5)], dashed=True, color=MUTED, width=2)
text(x + 56, LY, "leitura ou consulta", size=14.5, anchor="start")
text(40, LY + 30, "Números nas caixas: 1 entrada dos dados · 2 processamento · 3 transformação · 4 consumo · 5 governança com IA",
     size=14, fill=MUTED, anchor="start")
text(40, LY + 52, "Texto em fonte monoespaçada: nome da pasta do projeto no repositório",
     size=14, fill=MUTED, anchor="start")

add("</svg>")
sys.stdout.reconfigure(encoding="utf-8")
print("\n".join(out))
