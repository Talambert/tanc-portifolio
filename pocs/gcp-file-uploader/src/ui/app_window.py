import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox
import customtkinter as ctk

from PIL import Image

from src.services.gcp_storage import (
    upload_files_to_gcp,
    find_default_json_key,
    get_base_path,
    get_icon_path,
    get_asset_path,
    is_allowed_file,
    ALLOWED_EXTENSIONS,
)

ctk.set_appearance_mode("System")
ctk.set_default_color_theme("green")


class GCPUploaderApp(ctk.CTk):
    # Altura fixa da caixa de arquivos: 3 linhas visíveis; acima disso rola com scroll
    FILES_BOX_HEIGHT = 96

    def __init__(self):
        super().__init__()

        self.title("GCP Storage File Uploader")
        self.geometry("660x570")
        self.resizable(False, False)

        self._apply_icon()

        self.json_path = tk.StringVar()
        self.selected_files: list[str] = []
        self.bucket_name = tk.StringVar()
        self.folder_path = tk.StringVar()
        self.simulation_mode = tk.BooleanVar(value=False)

        self._create_widgets()
        self._load_auto_defaults()

    def _apply_icon(self):
        icon_path = get_icon_path()
        if icon_path and os.path.exists(icon_path):
            try:
                self.iconbitmap(icon_path)
            except Exception:
                pass

        # Garante que o Tkinter use a imagem em alta resolução diretamente na barra de título e no Windows Taskbar
        logo_img_path = get_asset_path("logo.png")
        if logo_img_path and os.path.exists(logo_img_path):
            try:
                from PIL import ImageTk
                img = Image.open(logo_img_path)
                bbox = img.getbbox()
                if bbox:
                    img = img.crop(bbox)
                
                # Gera versões 16x16, 32x32 e 64x64 nítidas
                p16 = ImageTk.PhotoImage(img.resize((16, 16), Image.Resampling.LANCZOS))
                p32 = ImageTk.PhotoImage(img.resize((32, 32), Image.Resampling.LANCZOS))
                p64 = ImageTk.PhotoImage(img.resize((64, 64), Image.Resampling.LANCZOS))
                
                # Manter referência para evitar garbage collector do Tkinter
                self._icon_photos = [p16, p32, p64]
                self.iconphoto(True, p64, p32, p16)
            except Exception:
                pass

    def _create_widgets(self):
        # Container principal com rolagem suave caso a resolução/DPI da tela seja reduzida
        self.main_container = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.main_container.pack(fill="both", expand=True, padx=4, pady=4)

        # Container superior com Logo e Título
        header_container = ctk.CTkFrame(self.main_container, fg_color="transparent")
        header_container.pack(fill="x", padx=16, pady=(10, 2))

        header_left = ctk.CTkFrame(header_container, fg_color="transparent")
        header_left.pack(side="left", fill="y")

        # Exibe o logo no cabeçalho se disponível
        logo_path = get_asset_path("logo_header.png")
        if not logo_path:
            logo_path = get_asset_path("logo.png")

        if logo_path and os.path.exists(logo_path):
            try:
                pil_logo = Image.open(logo_path)
                self.ctk_logo = ctk.CTkImage(light_image=pil_logo, dark_image=pil_logo, size=(38, 38))
                logo_label = ctk.CTkLabel(header_left, image=self.ctk_logo, text="")
                logo_label.pack(side="left", padx=(0, 10))
            except Exception:
                pass

        titles_box = ctk.CTkFrame(header_left, fg_color="transparent")
        titles_box.pack(side="left")

        title_label = ctk.CTkLabel(
            titles_box,
            text="GCP Storage File Uploader",
            font=ctk.CTkFont(size=20, weight="bold")
        )
        title_label.pack(anchor="w")

        subtitle_label = ctk.CTkLabel(
            titles_box,
            text="Upload seguro para o Google Cloud Storage",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=("#054416", "#26A69A")
        )
        subtitle_label.pack(anchor="w")

        # Switch de Modo Simulação / Teste
        self.switch_sim = ctk.CTkSwitch(
            header_container,
            text="Modo Simulação",
            variable=self.simulation_mode,
            command=self._on_simulation_toggle,
            font=ctk.CTkFont(size=12, weight="bold"),
            progress_color="#FFB300"
        )
        self.switch_sim.pack(side="right")

        self.auto_info_label = ctk.CTkLabel(
            self.main_container, text="", font=ctk.CTkFont(size=12, slant="italic"), text_color="gray"
        )
        self.auto_info_label.pack(pady=(2, 4))

        # 1. Credencial JSON
        json_frame = ctk.CTkFrame(self.main_container)
        json_frame.pack(fill="x", padx=16, pady=3)

        ctk.CTkLabel(json_frame, text="1. Chave da Service Account (.json):", font=ctk.CTkFont(weight="bold")).pack(
            anchor="w", padx=10, pady=(4, 2)
        )
        json_entry = ctk.CTkEntry(json_frame, textvariable=self.json_path, placeholder_text="Caminho do arquivo .json...")
        json_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 6))

        btn_json = ctk.CTkButton(
            json_frame,
            text="Alterar JSON",
            width=110,
            fg_color=("#054416", "#0a5c20"),
            hover_color=("#032e0e", "#0e7329"),
            command=self._select_json
        )
        btn_json.pack(side="right", padx=(5, 10), pady=(0, 6))

        # 2. Arquivos Locais (Lista Dinâmica)
        file_frame = ctk.CTkFrame(self.main_container)
        file_frame.pack(fill="x", padx=16, pady=3)

        file_header_frame = ctk.CTkFrame(file_frame, fg_color="transparent")
        file_header_frame.pack(fill="x", padx=10, pady=(4, 2))

        ctk.CTkLabel(
            file_header_frame,
            text="2. Arquivos para Envio:",
            font=ctk.CTkFont(weight="bold")
        ).pack(side="left")

        self.file_count_label = ctk.CTkLabel(
            file_header_frame,
            text="(0 arquivo(s))",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        self.file_count_label.pack(side="left", padx=8)

        # Botões de ação dos arquivos
        btn_file = ctk.CTkButton(
            file_header_frame,
            text="+ Adicionar Arquivos",
            width=140,
            fg_color=("#054416", "#0a5c20"),
            hover_color=("#032e0e", "#0e7329"),
            command=self._select_files
        )
        btn_file.pack(side="right", padx=(4, 0))

        btn_clear_all = ctk.CTkButton(
            file_header_frame,
            text="Limpar Lista",
            width=90,
            fg_color="#5c636a",
            hover_color="#474d53",
            command=self._clear_files
        )
        btn_clear_all.pack(side="right", padx=(4, 4))

        # Área de rolagem para lista de arquivos (começa compacta e cresce até 3 itens)
        self.scrollable_files_frame = ctk.CTkScrollableFrame(
            file_frame,
            height=self.FILES_BOX_HEIGHT,
            fg_color=("gray92", "gray17")
        )
        # A scrollbar interna do CTkScrollableFrame pede 200px de altura por padrão,
        # o que impede a caixa de encolher abaixo disso.
        self.scrollable_files_frame._scrollbar.configure(height=0)
        self.scrollable_files_frame.pack(fill="x", padx=10, pady=(2, 6))

        # 3. Destino no GCP
        dest_frame = ctk.CTkFrame(self.main_container)
        dest_frame.pack(fill="x", padx=16, pady=3)

        ctk.CTkLabel(dest_frame, text="3. Destino no Cloud Storage:", font=ctk.CTkFont(weight="bold")).pack(
            anchor="w", padx=10, pady=(4, 2)
        )

        grid_frame = ctk.CTkFrame(dest_frame, fg_color="transparent")
        grid_frame.pack(fill="x", padx=10, pady=(0, 6))

        ctk.CTkLabel(grid_frame, text="Nome do Bucket:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        bucket_entry = ctk.CTkEntry(grid_frame, textvariable=self.bucket_name, placeholder_text="ex: meu-bucket-dados")
        bucket_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=2)

        ctk.CTkLabel(grid_frame, text="Pasta (Opcional):").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        folder_entry = ctk.CTkEntry(grid_frame, textvariable=self.folder_path, placeholder_text="ex: inbound/2026/")
        folder_entry.grid(row=1, column=1, sticky="ew", padx=5, pady=2)

        grid_frame.columnconfigure(1, weight=1)

        # Botão e Status
        self.btn_upload = ctk.CTkButton(
            self.main_container,
            text="Realizar Upload",
            font=ctk.CTkFont(size=15, weight="bold"),
            height=42,
            fg_color=("#054416", "#0a5c20"),
            hover_color=("#032e0e", "#0e7329"),
            command=self._start_upload_thread
        )
        self.btn_upload.pack(fill="x", padx=16, pady=(8, 4))

        self.progress_bar = ctk.CTkProgressBar(
            self.main_container,
            progress_color=("#00C853", "#00E676")
        )
        self.progress_bar.set(0)
        self.progress_bar.pack(fill="x", padx=16, pady=4)

        self.status_label = ctk.CTkLabel(self.main_container, text="Aguardando ação...", text_color="gray")
        self.status_label.pack(pady=(2, 8))

    def _render_file_list(self):
        # Limpa widgets existentes dentro do frame com rolagem
        for widget in self.scrollable_files_frame.winfo_children():
            widget.destroy()

        count = len(self.selected_files)
        self.file_count_label.configure(text=f"({count} arquivo(s))")

        if count == 0:
            self.empty_label = ctk.CTkLabel(
                self.scrollable_files_frame,
                text="Nenhum arquivo na fila. Clique em '+ Adicionar Arquivos'.",
                text_color="gray",
                font=ctk.CTkFont(slant="italic", size=12)
            )
            self.empty_label.pack(pady=8)
            return

        # Renderiza cada arquivo em uma linha limpa
        for fpath in self.selected_files:
            row_frame = ctk.CTkFrame(self.scrollable_files_frame, fg_color="transparent")
            row_frame.pack(fill="x", padx=4, pady=2)

            # Ícone de documento
            icon_bullet = ctk.CTkLabel(
                row_frame,
                text="📄",
                font=ctk.CTkFont(size=14)
            )
            icon_bullet.pack(side="left", padx=(6, 8))

            # Nome e tamanho do arquivo
            fname = os.path.basename(fpath)
            try:
                size_kb = os.path.getsize(fpath) / 1024
                size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{(size_kb / 1024):.1f} MB"
            except Exception:
                size_str = ""

            info_text = f"{fname}  ({size_str})" if size_str else fname
            lbl = ctk.CTkLabel(
                row_frame,
                text=info_text,
                anchor="w",
                font=ctk.CTkFont(size=12)
            )
            lbl.pack(side="left", fill="x", expand=True)

            # Botão individual de exclusão (✕)
            btn_del = ctk.CTkButton(
                row_frame,
                text="✕",
                width=28,
                height=24,
                fg_color="transparent",
                hover_color=("#ffcdd2", "#b71c1c"),
                text_color=("#c62828", "#ef9a9a"),
                command=lambda p=fpath: self._delete_single_file(p)
            )
            btn_del.pack(side="right", padx=(4, 4))


    def _set_selected_files(self, files: list[str]):
        self.selected_files = [f for f in files if os.path.isfile(f)]
        self._render_file_list()

    def _delete_single_file(self, file_path: str):
        if file_path in self.selected_files:
            self.selected_files.remove(file_path)
            self._render_file_list()

    def _clear_files(self):
        self._set_selected_files([])

    def _load_auto_defaults(self):
        detected_json = find_default_json_key()
        if detected_json:
            self.json_path.set(detected_json)
            self.auto_info_label.configure(
                text="✔ Chave JSON carregada automaticamente da pasta local.",
                text_color="green"
            )
        else:
            self.auto_info_label.configure(
                text="ℹ Nenhuma chave JSON encontrada na pasta. Selecione manualmente.",
                text_color="gray"
            )

        base_dir = get_base_path()
        auto_folder = os.path.join(base_dir, "arquivos_para_envio")
        if os.path.exists(auto_folder) and os.path.isdir(auto_folder):
            files = [
                os.path.join(auto_folder, f)
                for f in os.listdir(auto_folder)
                if os.path.isfile(os.path.join(auto_folder, f)) and is_allowed_file(f)
            ]
            if files:
                self._set_selected_files(files)

    def _select_json(self):
        filename = filedialog.askopenfilename(filetypes=[("JSON Files", "*.json")])
        if filename:
            self.json_path.set(filename)

    def _select_files(self):
        filetypes = [
            ("Arquivos permitidos (dados, texto e planilhas)", "*.csv;*.tsv;*.txt;*.json;*.jsonl;*.xml;*.yaml;*.yml;*.parquet;*.orc;*.avro;*.xlsx;*.xls;*.ods;*.pdf;*.gz;*.zip;*.tar;*.tgz"),
            ("Arquivos de Texto e Dados (*.csv, *.txt, *.json, *.parquet...)", "*.csv;*.tsv;*.txt;*.json;*.jsonl;*.parquet;*.xml;*.yaml;*.yml"),
            ("Planilhas e Documentos (*.xlsx, *.xls, *.pdf...)", "*.xlsx;*.xls;*.ods;*.pdf"),
            ("Arquivos Compactados (*.zip, *.gz, *.tar...)", "*.zip;*.gz;*.tar;*.tgz"),
            ("Todos os Arquivos", "*.*")
        ]
        filenames = filedialog.askopenfilenames(
            title="Selecione um ou mais arquivos para envio",
            filetypes=filetypes
        )
        if filenames:
            valid_files = [f for f in filenames if is_allowed_file(f)]
            blocked_files = [os.path.basename(f) for f in filenames if not is_allowed_file(f)]

            if blocked_files:
                messagebox.showerror(
                    "Arquivos Bloqueados por Segurança",
                    f"Os seguintes arquivos não são permitidos e foram descartados:\n{', '.join(blocked_files)}\n\n"
                    "Por motivos de segurança, executáveis (.exe, .bat, .cmd, etc.) ou scripts do sistema não podem ser enviados.\n"
                    "São permitidos apenas arquivos de dados, textos e planilhas (ex: .csv, .txt, .json, .parquet, .xlsx, .pdf, .zip)."
                )

            if valid_files:
                # Acrescenta novos arquivos à lista já existente, evitando duplicatas
                current_files = list(self.selected_files)
                added_count = 0
                for vf in valid_files:
                    if vf not in current_files:
                        current_files.append(vf)
                        added_count += 1

                self._set_selected_files(current_files)
            elif not blocked_files:
                # usuário cancelou ou não selecionou nada válido
                pass

    def _on_simulation_toggle(self):
        if self.simulation_mode.get():
            self.auto_info_label.configure(
                text="⚡ MODO SIMULAÇÃO ATIVADO: Não requer chave JSON nem bucket real para testar.",
                text_color="#E65100"
            )
            if not self.bucket_name.get().strip():
                self.bucket_name.set("bucket-demonstracao-teste")
            if not self.folder_path.get().strip():
                self.folder_path.set("inbound/testes/")
            if not self.json_path.get().strip():
                self.json_path.set("[Simulação - Sem Credencial Necessária]")
        else:
            self.auto_info_label.configure(
                text="Modo real ativado.",
                text_color="gray"
            )
            if self.json_path.get() == "[Simulação - Sem Credencial Necessária]":
                self.json_path.set("")
            self._load_auto_defaults()

    def _start_upload_thread(self):
        sim = self.simulation_mode.get()

        if not sim:
            if not self.json_path.get() or not os.path.exists(self.json_path.get()):
                messagebox.showwarning("Atenção", "Por favor, selecione um arquivo JSON de credencial válido.")
                return

        if not self.selected_files:
            messagebox.showwarning("Atenção", "Por favor, selecione pelo menos um arquivo para upload.")
            return

        if not self.bucket_name.get().strip():
            messagebox.showwarning("Atenção", "Por favor, informe o nome do Bucket de destino.")
            return

        self.btn_upload.configure(state="disabled")
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(0)
        prefix = "[SIMULAÇÃO] " if sim else ""
        self.status_label.configure(
            text=f"{prefix}Iniciando envio de {len(self.selected_files)} arquivo(s)...",
            text_color="#1F6AA5"
        )

        threading.Thread(target=self._upload_process, daemon=True).start()

    def _upload_callback(self, idx: int, total: int, file_name: str, uri: str):
        fraction = idx / total
        self.after(
            0,
            lambda: self._update_progress_ui(idx, total, file_name, fraction)
        )

    def _update_progress_ui(self, idx: int, total: int, file_name: str, fraction: float):
        self.progress_bar.set(fraction)
        prefix = "[SIMULAÇÃO] " if self.simulation_mode.get() else ""
        self.status_label.configure(
            text=f"{prefix}Enviando ({idx}/{total}): {file_name}",
            text_color="#1F6AA5"
        )

    def _upload_process(self):
        try:
            uploaded_uris = upload_files_to_gcp(
                json_path=self.json_path.get(),
                file_paths=self.selected_files,
                bucket_name=self.bucket_name.get(),
                folder_path=self.folder_path.get(),
                progress_callback=self._upload_callback,
                is_simulation=self.simulation_mode.get()
            )
            self.after(0, self._upload_success, uploaded_uris)
        except Exception as e:
            self.after(0, self._upload_error, str(e))

    def _upload_success(self, uploaded_uris: list[str]):
        self.progress_bar.set(1.0)
        self.btn_upload.configure(state="normal")
        self.status_label.configure(text="✔ Upload concluído com sucesso!", text_color="green")

        count = len(uploaded_uris)
        if count == 1:
            dest_msg = f"Destino:\n{uploaded_uris[0]}"
        else:
            first_few = "\n".join(f"• {uri}" for uri in uploaded_uris[:4])
            more = f"\n... e mais {count - 4} arquivo(s)." if count > 4 else ""
            dest_msg = f"Total: {count} arquivo(s) enviados:\n{first_few}{more}"

        messagebox.showinfo(
            "Sucesso!",
            f"Arquivo(s) enviado(s) com sucesso para o Google Cloud Storage!\n\n{dest_msg}"
        )

    def _upload_error(self, error_msg: str):
        self.progress_bar.set(0)
        self.btn_upload.configure(state="normal")
        self.status_label.configure(text="✖ Falha no envio.", text_color="#D32F2F")

        messagebox.showerror("Atenção - Erro no Upload", error_msg)
