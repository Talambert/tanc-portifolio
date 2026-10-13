import sys
import os

# Adiciona o diretório raiz ao PYTHONPATH para garantir que as importações do pacote 'src' funcionem
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Garante que o Windows use o ícone customizado na Barra de Tarefas em vez do ícone padrão do Python
if sys.platform == "win32":
    try:
        import ctypes
        app_id = "gcp.fileuploader.v1"
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_id)
    except Exception:
        pass

from src.ui.app_window import GCPUploaderApp

if __name__ == "__main__":
    app = GCPUploaderApp()
    app.mainloop()