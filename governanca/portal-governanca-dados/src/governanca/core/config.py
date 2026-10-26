from dataclasses import dataclass


@dataclass
class Settings:
    """Configuração do portal. Na fase de simulação, nada chama o GCP."""

    project_id: str = "projeto-demo"
    region: str = "southamerica-east1"
    model: str = "gemini (configurável)"
    simulation: bool = True
    simulated_fault: str = "Nenhuma"  # só no modo simulação: provoca falhas para demonstrar o tratamento de erro
    max_sample_rows: int = 5
    max_gb_per_run: float = 1.0
