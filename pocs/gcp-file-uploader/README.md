# ☁️ GCP Storage File Uploader

Uma aplicação desktop moderna, leve e intuitiva desenvolvida em Python utilizando **CustomTkinter** para facilitar o upload e a ingestão de arquivos no **Google Cloud Storage (GCS)** via **Service Accounts**.

O objetivo principal desta ferramenta é permitir que parceiros externos, clientes ou usuários não técnicos enviem arquivos diretamente para buckets no GCP de forma segura, visual e amigável, com suporte a **detecção automática de credenciais e pasta local**, sem precisar acessar o console web da Google Cloud ou manipular linhas de comando.

---

## 🏗️ Estrutura do Projeto

```text
gcp-file-uploader/
├── config/                      # Modelos de configuração
│   └── service_account.json.example
├── src/                         # Código-fonte modularizado
│   ├── ui/                      # Camada de Interface Gráfica (Tkinter)
│   │   ├── __init__.py
│   │   └── app_window.py        # Janela, botões, eventos e detecção automática
│   ├── services/                # Lógica de negócio e integração
│   │   ├── __init__.py
│   │   └── gcp_storage.py       # Cliente oficial do GCS e busca de credenciais
│   ├── __init__.py
│   └── main.py                  # Ponto de entrada da aplicação
├── assets/                      # Ícones e recursos visuais (.ico)
├── .gitignore                   # Proteção contra commit de senhas e builds
├── requirements.txt             # Bibliotecas Python necessárias
└── README.md                    # Documentação do projeto
```

---

## 🔑 Pré-requisitos e Permissões no GCP

Para garantir o funcionamento correto e seguir as boas práticas do **Princípio do Menor Privilégio**, a **Service Account** utilizada deve ter a seguinte papel/permissão mínima atribuída no bucket de destino no Google Cloud Console:

* **Storage Object Creator** (`roles/storage.objectCreator`): Permite que a chave crie e grave novos arquivos dentro do bucket, **sem conceder permissão** para ler, alterar ou deletar arquivos que já estejam armazenados lá.

> **Dica:** Não é necessário atribuir permissões de administrador global (`Storage Admin`) para a Service Account do usuário final.

---

## 🛠️ Como Executar Localmente (Desenvolvimento)

Siga os passos abaixo para rodar a aplicação em seu ambiente local de desenvolvimento:

### 1. Clonar o repositório
```bash
git clone [https://github.com/seu-usuario/gcp-file-uploader.git](https://github.com/seu-usuario/gcp-file-uploader.git)
cd gcp-file-uploader
```

### 2. Criar e ativar o ambiente virtual (venv)
```bash
# No Windows (Git Bash)
python -m venv venv
source venv/Scripts/activate

# No Windows (PowerShell)
python -m venv venv
.\venv\Scripts\Activate.ps1

# No Linux / MacOS
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar as dependências do projeto
```bash
pip install -r requirements.txt
```

### 4. Executar a aplicação
```bash
python src/main.py
```

---

## 📦 Compilando para Executável (.exe)

Para distribuir o aplicativo para usuários finais sem a necessidade de instalar o Python ou configurar dependências em suas máquinas, compile o projeto usando o **PyInstaller**.

### Opção A: Pasta com dependências (Inicialização mais rápida — Recomendado)
Cria uma pasta contendo o arquivo `.exe` e todas as bibliotecas necessárias isoladas.
```bash
pyinstaller --noconfirm --onedir --windowed --icon "assets/icon.ico" --add-data "assets;assets" --collect-all customtkinter --name "GCP_Uploader" src/main.py
```

### Opção B: Arquivo Executável Único (`--onefile`)
Empacota toda a aplicação e dependências em um único arquivo portátil `.exe`.
```bash
pyinstaller --noconfirm --onefile --windowed --icon "assets/icon.ico" --add-data "assets;assets" --collect-all customtkinter --name "GCP_Uploader" src/main.py
```

> Após a compilação, o executável gerado estará disponível dentro da pasta **`dist/`**.

---

## 🚀 Como Entregar a Pasta para o Parceiro (Modo Automático)

Você pode enviar um arquivo `.zip` já pré-configurado para que o parceiro não precise selecionar arquivos manualmente:

```text
Envio_GCP/
├── GCP_Uploader.exe
├── assets/                    # (Se usar --onedir) Ícone e recursos
├── service_account.json       # Sua chave fornecida (carregada automaticamente)
└── arquivos_para_envio/       # (Opcional) Pasta onde o parceiro joga os arquivos
```

## 🧪 Modo Simulação (Para Testes e Demonstrações)

O aplicativo conta com uma chave seletora **"Modo Simulação"** no topo da janela:
* Permite testar todo o fluxo visual (adicionar arquivos, marcar/desmarcar na lista, barra de progresso individual, pop-up de sucesso).
* **Não exige chave de Service Account real nem acesso a buckets do Google Cloud**.
* Preenche automaticamente campos de demonstração e simula a latência de envio em tempo real.

---

## 👥 Guia Rápido de Uso para o Usuário Final

Se você recebeu esta ferramenta para enviar arquivos para a nossa equipe, siga o passo a passo:

1. **Abra o aplicativo:** Dê um duplo clique no arquivo **`GCP_Uploader.exe`**.
2. **Verifique a Credencial:**
   * O aplicativo tentará carregar automaticamente o arquivo `.json` presente na mesma pasta.
   * Caso não seja carregado automaticamente (ou você queira trocar), clique em **Alterar JSON** e selecione a credencial fornecida.
3. **Selecione os Arquivos:**
   * Se houver arquivos na pasta `arquivos_para_envio`, eles serão listados automaticamente.
   * Clique em **+ Adicionar Arquivos** para adicionar novos arquivos à lista (você pode clicar várias vezes e selecionar novos arquivos à vontade).
   * **Gerenciar lista:**
     * Cada arquivo aparece em uma linha com seu tamanho.
     * Para remover um arquivo específico, basta clicar no botão **✕** ao lado dele.
     * Para remover vários arquivos de uma vez, marque a caixa de seleção dos arquivos desejados (ou utilize a opção **Marcar Todos**) e clique em **Excluir Marcados**.
4. **Informe o Destino:**
   * **Nome do Bucket:** Digite o nome exato do bucket de destino (ex: `meu-bucket-dados`).
   * **Pasta (Opcional):** Se desejar salvar dentro de uma pasta específica no bucket, digite o caminho (ex: `inbound/2026/`).
5. **Enviar:** Clique em **Realizar Upload**. A barra de progresso acompanhará o progresso de cada arquivo e uma mensagem com o resumo dos arquivos enviados e URIs `gs://...` será exibida ao final.

---

## 🔒 Segurança e Restrição de Arquivos

* 🛡️ **Bloqueio de Arquivos Executáveis/Nocivos:** Para mitigar riscos e impedir envios acidentais ou maliciosos de arquivos executáveis (`.exe`, `.bat`, `.cmd`, `.sh`, `.vbs`, etc.), a ferramenta possui uma lista estrita (*allowlist*) de extensões seguras aceitas:
  * **Texto e Dados:** `.csv`, `.tsv`, `.txt`, `.json`, `.jsonl`, `.xml`, `.yaml`, `.yml`
  * **Analíticos e Colunares:** `.parquet`, `.orc`, `.avro`
  * **Planilhas e Documentos:** `.xlsx`, `.xls`, `.ods`, `.pdf`
  * **Compactados de Dados:** `.zip`, `.gz`, `.tar`, `.tgz`
* 🚨 **Chaves Privadas:** O arquivo de chave da Service Account (`.json`) concede acesso de gravação ao seu projeto. **NUNCA compartilhe esse arquivo publicamente nem faça commit dele em repositórios abertos.**
* O arquivo `.gitignore` deste projeto já está pré-configurado para ignorar qualquer arquivo `.json` presente na raiz ou na pasta `config/`.
* É altamente recomendado definir políticas de expiração de chave ou rotacionar as credenciais periodicamente no painel **IAM & Admin** da GCP.

---
